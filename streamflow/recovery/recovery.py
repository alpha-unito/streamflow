from __future__ import annotations


import asyncio
from typing import MutableMapping, MutableSequence, Tuple

import pkg_resources

from streamflow.core.utils import (
    get_class_fullname,
    get_class_from_name,
)
from streamflow.core.deployment import Connector, Location
from streamflow.core.exception import (
    FailureHandlingException,
)

from streamflow.core.workflow import Job, Step, Port, Token
from streamflow.cwl.token import CWLFileToken
from streamflow.cwl.transformer import ForwardTransformer
from streamflow.data import remotepath
from streamflow.workflow.port import ConnectorPort, JobPort
from streamflow.workflow.step import CombinatorStep
from streamflow.workflow.step import ExecuteStep
from streamflow.workflow.token import TerminationToken, JobToken

from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Workflow


INIT_DAG_FLAG = "init"
TOKEN_WAITER = "twaiter"


def str_tok(token):
    if isinstance(token, JobToken):
        return token.value.name
    elif isinstance(token, CWLFileToken):
        return token.value["path"]
    else:
        return token.value


def check_double_reference(dag_ports):
    for tmpp in dag_ports[INIT_DAG_FLAG]:
        for tmpport_name, tmpnext_port_names in dag_ports.items():
            if tmpport_name != INIT_DAG_FLAG and tmpp in tmpnext_port_names:
                msg = f"Port {tmpp} appartiene sia a INIT che a {tmpport_name}"
                print("WARN", msg)
                # print("OOOOOOOOOOOOOOOOOOOOOOOOOOOO" * 100, "\n", msg)
                # raise FailureHandlingException(msg)


async def get_execute_step_out_token_ids(next_token_ids, context):
    execute_step_out_token_ids = set()
    for t_id in next_token_ids:
        port_row = await context.database.get_port_from_token(t_id)
        for step_id_row in await context.database.get_steps_from_output_port(
            port_row["id"]
        ):
            step_row = await context.database.get_step(step_id_row["step"])
            if step_row["type"] == get_class_fullname(ExecuteStep):
                execute_step_out_token_ids.add(t_id)
    return execute_step_out_token_ids


async def _cleanup_dir(
    connector: Connector, location: Location, directory: str
) -> None:
    await remotepath.rm(
        connector, location, await remotepath.listdir(connector, location, directory)
    )


def get_prev_vertices(searched_vertex, dag):
    prev_vertices = set()
    for vertex, next_vertices in dag.items():
        if searched_vertex in next_vertices and vertex != INIT_DAG_FLAG:
            prev_vertices.add(vertex)
    return prev_vertices


async def _put_tokens(
    new_workflow: Workflow,
    init_ports: MutableSequence[str],
    port_tokens: MutableMapping[str, MutableSequence[int]],
    token_visited: MutableMapping[int, Tuple[Token, bool]],
    dag_ports,
    interesting_out_ports,
):
    for port_name in init_ports:
        token_list = [
            token_visited[t_id][0]
            for t_id in port_tokens[port_name]
            if isinstance(t_id, int) and token_visited[t_id][1]
        ]
        token_list.sort(key=lambda x: x.tag, reverse=False)
        for i, t in enumerate(token_list):
            for t1 in token_list[i:]:
                if t.persistent_id != t1.persistent_id and t.tag == t1.tag:
                    raise FailureHandlingException(
                        f"Tag ripetuto id1: {t.persistent_id} id2: {t1.persistent_id}"
                    )

        if port_name not in new_workflow.ports.keys():
            tlist = None
            if port_name in port_tokens.keys():
                tlist = port_tokens[port_name]
            pass
        port = new_workflow.ports[port_name]
        for t in token_list:
            if isinstance(t, TerminationToken):
                raise FailureHandlingException(
                    f"Aggiungo un termination token nell port {port.name} ma non dovrei"
                )
            port.put(t)
        if len(port.token_list) > 0 and len(port.token_list) == len(
            port_tokens[port_name]
        ):
            port.put(TerminationToken())
        else:
            print(
                f"Port {port.name} with {len(port.token_list)} tokens. NO TerminationToken"
            )


def tmp_prova(
    tag_level, port_name, dag_ports, port_tokens, token_visited, execute_out_ports
):
    if port_name not in port_tokens.keys():
        return
    for t_id in port_tokens[port_name]:
        if get_tag_level(token_visited[t_id][0].tag) == tag_level:
            if token_visited[t_id][1]:
                token_visited[t_id] = (token_visited[t_id][0], False)
        else:
            return
    for next_port_name in dag_ports[port_name]:
        if next_port_name not in execute_out_ports:
            tmp_prova(
                tag_level,
                next_port_name,
                dag_ports,
                port_tokens,
                token_visited,
                execute_out_ports,
            )


async def _put_tokens1(
    new_workflow: Workflow,
    init_ports: MutableSequence[str],
    port_tokens: MutableMapping[str, MutableSequence[int]],
    token_visited: MutableMapping[int, Tuple[Token, bool]],
    dag_ports,
    interesting_out_ports,
):
    for step in new_workflow.steps.values():
        if isinstance(step, CombinatorStep):
            a = step.output_ports.values()
            pass
            for port_name in step.output_ports.values():
                if port_name in port_tokens.keys():
                    tag_level = None
                    for t_id in port_tokens[port_name]:
                        if tag_level is None:
                            tag_level = get_tag_level(token_visited[t_id][0].tag)
                        if token_visited[t_id][1]:
                            token_visited[t_id] = (token_visited[t_id][0], False)
                    for next_port_name in dag_ports[port_name]:
                        tmp_prova(
                            tag_level,
                            next_port_name,
                            dag_ports,
                            port_tokens,
                            token_visited,
                            interesting_out_ports["execute"],
                        )

    # for port_name in interesting_out_ports["combinator"]:
    #     if port_name in port_tokens.keys():
    #         tag_level = None
    #         for t_id in port_tokens[port_name]:
    #             if tag_level is None:
    #                 tag_level = get_tag_level(token_visited[t_id][0].tag)
    #             if token_visited[t_id][1]:
    #                 token_visited[t_id] = (token_visited[t_id][0], False)
    #         for next_port_name in dag_ports[port_name]:
    #             tmp_prova(
    #                 tag_level,
    #                 next_port_name,
    #                 dag_ports,
    #                 port_tokens,
    #                 token_visited,
    #                 interesting_out_ports["execute"],
    #             )

    for port_name in interesting_out_ports["combinator"]:
        if port_name in port_tokens.keys():
            for t_id in port_tokens[port_name]:
                if token_visited[t_id][1]:
                    token_visited[t_id] = (token_visited[t_id][0], False)
            for next_port_name in dag_ports[port_name]:
                for t_id in port_tokens[next_port_name]:
                    if token_visited[t_id][1]:
                        # todo: scendere i next ports finché il livello del tag non cambia e mettere tutti i token a false
                        token_visited[t_id] = (token_visited[t_id][0], False)

    for port in new_workflow.ports.values():
        available_tokens = set()
        for t_id in port_tokens.get(port.name, []):
            if not isinstance(t_id, str) and token_visited[t_id][1]:
                available_tokens.add(t_id)
        if port.name in interesting_out_ports["combinator"]:
            # todo: controllare che il combinator step ci sia ancora. Altra opzione modificare i metodi di add in modo tale da controllare che se la port è un combinator allora il token non èavai
            for t_id in available_tokens:
                token_visited[t_id] = (token_visited[t_id][0], False)
        else:
            for prev_port_name in get_prev_vertices(port.name, dag_ports):
                for t_id in available_tokens:
                    if token_visited[t_id][0].tag in (
                        token_visited[t_id2][0].tag
                        for t_id2 in port_tokens[prev_port_name]
                        # if token_visited[t_id2][1]
                    ):
                        token_visited[t_id] = (token_visited[t_id][0], False)

    for port_name, token_list1 in port_tokens.items():
        if port_name not in new_workflow.ports.keys():
            continue
        port = new_workflow.ports[port_name]
        new_token_list = []
        for t_id in token_list1:
            if not isinstance(t_id, str) and token_visited[t_id][1]:
                new_token_list.append(token_visited[t_id][0])
        new_token_list.sort(key=lambda x: x.tag, reverse=False)
        for i, t in enumerate(new_token_list):
            for t1 in new_token_list[i:]:
                if t.persistent_id != t1.persistent_id and t.tag == t1.tag:
                    raise FailureHandlingException(
                        f"Tag ripetuto id1: {t.persistent_id} id2: {t1.persistent_id}"
                    )
        for t in new_token_list:
            if isinstance(t, TerminationToken):
                raise FailureHandlingException(
                    f"Aggiungo un termination token nell port {port.name} ma non dovrei"
                )
            port.put(t)
        if len(port.token_list) > 0 and len(port.token_list) == len(
            port_tokens[port_name]
        ):
            port.put(TerminationToken())
        else:
            print(
                f"Port {port.name} with {len(port.token_list)} tokens. NO TerminationToken"
            )


async def _populate_workflow(
    failed_step,
    token_visited,
    new_workflow,
    loading_context,
    port_tokens,
    dag_ports,
    workflow,
):
    # { id : all_tokens_are_available }
    ports = {}

    id_name = {}
    for token_id, (_, is_available) in token_visited.items():
        row = await failed_step.workflow.context.database.get_port_from_token(token_id)
        id_name[row["id"]] = row["name"]
        if TOKEN_WAITER not in port_tokens[row["name"]]:
            if row["id"] in ports.keys():
                ports[row["id"]] = ports[row["id"]] and is_available
            else:
                ports[row["id"]] = is_available
        # else nothing because the ports are already loading in the new_workflow

    # add port into new_workflow
    print(
        f"new workflow {new_workflow.name} ports init situation {new_workflow.ports.keys()}"
    )
    for port in await asyncio.gather(
        *(
            asyncio.create_task(
                Port.load(
                    new_workflow.context,
                    port_id,
                    loading_context,
                    new_workflow,
                )
            )
            for port_id in ports.keys()
        )
    ):
        if port.name not in new_workflow.ports.keys():
            new_workflow.add_port(port)
        else:
            print(
                f"La port {port.name} è già presente nel workflow {new_workflow.name}"
            )
    print("Port caricate")

    for tmpp in dag_ports[INIT_DAG_FLAG]:
        if tmpp not in dag_ports.keys():
            msg = f"Port {tmpp} non porta da nessuna parte"
            print("OOOOOOOOOOOOOOOOOOOOOOOOOOOO" * 100, "\n", msg)
            raise FailureHandlingException(msg)
    print("check iniziato")
    check_double_reference(dag_ports)
    print("Check finito")

    steps = set()
    for row_dependencies in await asyncio.gather(
        *(
            asyncio.create_task(
                new_workflow.context.database.get_steps_from_output_port(port_id)
            )
            for port_id in ports.keys()
            if id_name[port_id] not in dag_ports[INIT_DAG_FLAG]
        )
    ):
        for row_dependency in row_dependencies:
            steps.add(row_dependency["step"])
    print("Step id recuperati", len(steps))
    for step in await asyncio.gather(
        *(
            asyncio.create_task(
                Step.load(
                    new_workflow.context,
                    step_id,
                    loading_context,
                    new_workflow,
                )
            )
            for step_id in steps
        )
    ):
        new_workflow.add_step(step)
    print("Step caricati")

    rm_steps = set()
    missing_ports = set()
    for step in new_workflow.steps.values():
        for p_name in step.input_ports.values():
            if p_name not in new_workflow.ports.keys():
                rm_steps.add(step.name)
                # problema nato dai loop. Vengono caricati nel new_workflow tutti gli step che hanno come output le port
                # nel grafo. Però nei loop, più step hanno stessa porta di output (forwad, backprop, loop-term).
                # per capire se lo step sia necessario controlliamo che anche le sue port di input siano state caricate
                print(
                    f"Rimuovo step {step.name} dal wf {new_workflow.name} perché manca la input port {p_name}"
                )
                pass
        for p_name in step.output_ports.values():
            if p_name not in new_workflow.ports.keys():
                missing_ports.add(workflow.ports[p_name].persistent_id)
                # problema nato dai loop. when-loop ha output tutti i param. Però il grafo è costruito sulla presenza o
                # meno dei file, invece i param str, int, ..., no. Quindi le port di questi param non vengono esplorate
                # le aggiungo ma vengono usati come "port pozzo" ovvero il loro output non viene utilizzato
                print(
                    f"Aggiungo port {p_name} al wf {new_workflow.name} perché è un output port dello step {step.name}"
                )
                pass
    for s_name in rm_steps:
        new_workflow.steps.pop(s_name)

    for port in await asyncio.gather(
        *(
            asyncio.create_task(
                Port.load(new_workflow.context, p_id, loading_context, new_workflow)
            )
            for p_id in missing_ports
        )
    ):
        new_workflow.add_port(port)

    # add output port of failed step into new_workflow
    for port in await asyncio.gather(
        *(
            asyncio.create_task(
                Port.load(
                    new_workflow.context, p.persistent_id, loading_context, new_workflow
                )
            )
            for p in failed_step.get_output_ports().values()
        )
    ):
        new_workflow.add_port(port)
    print("Ultime Port caricate")


# todo: move it in utils
def get_token_by_tag(token_tag, token_list):
    for token in token_list:
        if token_tag == token.tag:
            return token
    return None


async def _is_token_available(token, context):
    return not isinstance(token, JobToken) and await token.is_available(context)


def get_necessary_tokens(
    port_tokens, all_token_visited
) -> MutableMapping[int, Tuple[Token, bool]]:
    d = {
        t_id: all_token_visited[t_id]
        for token_list in port_tokens.values()
        for t_id in token_list
        if t_id != TOKEN_WAITER
    }
    return dict(sorted(d.items()))


async def reduce_graph(
    dag_ports,
    port_tokens,
    available_new_job_tokens,
    token_visited,
    context,
    loading_context,
):
    for v in available_new_job_tokens.values():
        if v["out-token-port-name"] not in port_tokens.keys():
            raise FailureHandlingException("Non c'è la porta. NON VA BENE")

    # todo: aggiustare nel caso in cui uno step abbia più port di output.
    #   Nella replace_token, se i token della port sono tutti disponibili
    #   allora rimuove tutte le port precedenti.
    #   ma se i token della seconda port di output dello step non sono disponibili
    #   è necessario eseguire tutte le port precedenti.
    for v in available_new_job_tokens.values():
        if v["out-token-port-name"] in port_tokens.keys():
            replace_token(
                v["out-token-port-name"],
                v["old-out-token"],
                token_visited[v["new-out-token"]][0],
                dag_ports,
                port_tokens,
                token_visited,
            )
            await remove_prev(
                v["old-out-token"],
                dag_ports,
                port_tokens,
                token_visited,
                context,
                loading_context,
            )
            print(
                f"Port_tokens {v['out-token-port-name']} - token {v['old-out-token']} sostituito con token {v['new-out-token']}. Quindi la port ha {port_tokens[v['out-token-port-name']]} tokens"
            )
        else:
            print(
                f"Port_tokens {v['out-token-port-name']} - port non più presente. Non serve più eseguire lo step annesso"
            )
            pass


def is_next_of_someone(p_name, dag_ports):
    for port_name, next_port_names in dag_ports.items():
        if port_name != INIT_DAG_FLAG and p_name in next_port_names:
            return True
    return False


def add_into_vertex(
    port_name_to_add: str,
    token_to_add: Token,
    port_tokens: MutableMapping[str, MutableSequence[int]],
    token_visited: MutableMapping[int, Tuple[Token, bool]],
):
    if isinstance(token_to_add, JobToken):
        print(
            f"job token {token_to_add.value.name} (id {token_to_add.persistent_id}) port {port_name_to_add}"
        )
    if port_name_to_add in port_tokens.keys():
        for sibling_token_id in port_tokens[port_name_to_add]:
            sibling_token = token_visited[sibling_token_id][0]
            # If there are two of the same token in the same port, keep the newer token or that available
            # In the case of JobTokens, they are the same when they have the same job.name
            # In other token types, they are equal when they have the same tag
            if isinstance(token_to_add, JobToken):
                if not isinstance(sibling_token, JobToken):
                    raise FailureHandlingException(f"Non è un jobtoken {sibling_token}")
                if token_to_add.value.name == sibling_token.value.name:
                    if (
                        token_visited[token_to_add.persistent_id][1]
                        or token_to_add.persistent_id > sibling_token_id
                    ):
                        port_tokens[port_name_to_add].remove(sibling_token_id)
                        break
                    else:
                        return
            else:
                if token_to_add.tag == sibling_token.tag:
                    if (
                        token_visited[token_to_add.persistent_id][1]
                        or token_to_add.persistent_id > sibling_token_id
                    ):
                        port_tokens[port_name_to_add].remove(sibling_token_id)
                        break
                    else:
                        return
    print(
        f"Aggiungo token {str_tok(token_to_add)} id {token_to_add.persistent_id} tag {token_to_add.tag} nella port_tokens[{port_name_to_add}]"
    )
    port_tokens.setdefault(port_name_to_add, set()).add(token_to_add.persistent_id)


def add_into_graph(
    port_name_to_add: str,
    token_to_add: Token,
    port_name_key: str,
    dag_ports: MutableMapping[str, MutableSequence[str]],
    port_tokens: MutableMapping[str, MutableSequence[int]],
    token_visited: MutableMapping[int, Tuple[Token, bool]],
    port_type: str,
):
    if token_to_add:
        add_into_vertex(port_name_to_add, token_to_add, port_tokens, token_visited)

    # todo: fare i controlli prima di aggiungere.
    #   in particolare se la port è presente in init e si sta aggiungendo in un'altra port
    #       - o non si aggiunge
    #       - o si toglie da init e si aggiunge nella nuova port
    #   stessa cosa viceversa, se è in un'altra port e si sta aggiungendo in init
    # edit. attuale implementazione: si toglie da init e si aggiunge alla port nuova
    if (
        INIT_DAG_FLAG in dag_ports.keys()
        and port_name_to_add in dag_ports[INIT_DAG_FLAG]
        and port_name_key != INIT_DAG_FLAG
    ):
        print(
            f"Inserisco la port {port_name_to_add} dopo la port {port_name_key}. però è già in INIT. Aggiorno grafo"
        )
    if port_name_key == INIT_DAG_FLAG and port_name_to_add in [
        pp for p, plist in dag_ports.items() for pp in plist if p != INIT_DAG_FLAG
    ]:
        print(
            f"Inserisco la port {port_name_to_add} in INIT. però è già in {port_name_key}. Aggiorno grafo"
        )
    dag_ports.setdefault(port_name_key, set()).add(port_name_to_add)

    # It is possible find some token available and other unavailable in a scatter
    # In this case the port is added in init and in another port.
    # If the port is next in another port, it is necessary to detach the port from the init next list
    if (
        INIT_DAG_FLAG in dag_ports.keys()
        # and (
        #     port_type is None
        #     or not issubclass(get_class_from_name(port_type), ForwardTransformer)
        # )
        and port_name_to_add in dag_ports[INIT_DAG_FLAG]
        and is_next_of_someone(port_name_to_add, dag_ports)
    ):
        dag_ports[INIT_DAG_FLAG].remove(port_name_to_add)


def remove_from_graph(
    port_name_to_remove: str,
    dag_ports: MutableMapping[str, MutableSequence[str]],
    port_tokens: MutableMapping[str, MutableSequence[int]],
    token_visited: MutableMapping[int, Tuple[Token, bool]],
):
    other_ports_to_remove = set()
    # Removed all occurrences as next port
    for port_name, next_port_names in dag_ports.items():
        if port_name_to_remove in next_port_names:
            next_port_names.remove(port_name_to_remove)
        if len(next_port_names) == 0:
            other_ports_to_remove.add(port_name)

    # removed all tokens generated by port
    for t_id in port_tokens.pop(port_name_to_remove, ()):
        # token_visited.pop(t_id)
        print(f"New-method - pop {t_id} token_visited")

    # removed in the graph and moved its next port in INIT
    for port_name in dag_ports.pop(port_name_to_remove, ()):
        if not is_next_of_someone(port_name, dag_ports):
            add_into_graph(
                port_name,
                None,
                INIT_DAG_FLAG,
                dag_ports,
                port_tokens,
                token_visited,
                None,
            )
    print(f"New-method - pop {port_name_to_remove} from dag_ports and port_tokens")
    # remove vertex detached from the graph
    for port_name in other_ports_to_remove:
        remove_from_graph(
            port_name,
            dag_ports,
            port_tokens,
            token_visited,
        )


def remove_token_by_id(
    token_id_to_remove: int,
    dag_ports: MutableMapping[str, MutableSequence[str]],
    port_tokens: MutableMapping[str, MutableSequence[int]],
    token_visited: MutableMapping[int, Tuple[Token, bool]],
):
    # token_visited.pop(token_id_to_remove, None)
    print(f"new-method - remove token {token_id_to_remove} from visited")
    ports_to_remove = set()
    for port_name, token_ids in port_tokens.items():
        if token_id_to_remove in token_ids:
            token_ids.remove(token_id_to_remove)
            print(f"new-method - remove token {token_id_to_remove} from {port_name}")
        if len(token_ids) == 0:
            ports_to_remove.add(port_name)
    for port_name in ports_to_remove:
        remove_from_graph(port_name, dag_ports, port_tokens, token_visited)


def get_port_from_token(token, port_tokens, token_visited):
    for port_name, token_ids in port_tokens.items():
        if token.tag in (token_visited[t_id][0].tag for t_id in token_ids):
            return port_name
    raise FailureHandlingException("Token assente")


def remove_token(token, port_name, dag_ports, port_tokens, token_visited):
    msg = (
        "job {token.value.name}" if isinstance(token, JobToken) else f"tag {token.tag}"
    )
    if port_name in port_tokens.keys():
        for t_id in port_tokens[port_name]:
            if (
                not isinstance(token, JobToken)
                and token_visited[t_id][0].tag == token.tag
            ) or (
                isinstance(token, JobToken)
                and token_visited[t_id][0].value.name == token.value.name
            ):
                print(f"Rimuovo token {t_id} con {msg} dal port_tokens[{port_name}]")
                remove_token_by_id(t_id, dag_ports, port_tokens, token_visited)
                return
        print(
            f"Volevo rimuovere token con {msg} dal port_tokens[{port_name}] ma non ce n'è"
        )
    else:
        print(
            f"Volevo rimuovere token con {msg} ma non c'è port {port_name} in port_tokens"
        )


def get_tag_level(tag: str):
    return len(tag.split("."))


# richiamare ricorsivamente finche i token.tag hanno sempre lo stesso livello o se si trova un JobToken
async def remove_prev(
    token_id_to_remove: int,
    dag_ports: MutableMapping[str, MutableSequence[str]],
    port_tokens: MutableMapping[str, MutableSequence[int]],
    token_visited: MutableMapping[int, Tuple[Token, bool]],
    context: StreamFlowContext,
    loading_context,
):
    curr_tag_level = get_tag_level(token_visited[token_id_to_remove][0].tag)
    for token_row in await context.database.get_dependees(token_id_to_remove):
        port_row = await context.database.get_port_from_token(token_row["dependee"])
        if token_row["dependee"] not in token_visited.keys():
            print(
                f"rm prev - token id {token_row['dependee']} non presente nei token_visited"
            )
            continue
        elif port_row["type"] != get_class_fullname(ConnectorPort):
            if token_row["dependee"] not in token_visited.keys():
                token_visited[token_row["dependee"]] = (
                    await loading_context.load_token(context, token_row["dependee"]),
                    False,
                )
                print(
                    f"rm prev - prev token id {token_row['dependee']} tag {token_visited[token_row['dependee']][0].tag} non è presente tra i token visitati, lo aggiungo"
                )
            print(
                f"rm prev - token {token_id_to_remove} rm prev token id: {token_row['dependee']} val: {str_tok(token_visited[token_row['dependee']][0])} tag: {token_visited[token_row['dependee']][0].tag}"
            )

            port_dependee_row = await context.database.get_port_from_token(
                token_row["dependee"]
            )
            dependee_token = token_visited[token_row["dependee"]][0]
            a = port_dependee_row["type"] == get_class_fullname(JobPort)
            b = isinstance(dependee_token, JobToken)
            c = a != b
            if isinstance(dependee_token, JobToken) or (
                not isinstance(token_visited[token_id_to_remove][0], JobToken)
                and curr_tag_level == get_tag_level(dependee_token.tag)
            ):
                remove_token(
                    dependee_token,
                    port_dependee_row["name"],
                    dag_ports,
                    port_tokens,
                    token_visited,
                )
                msg = (
                    f"job {dependee_token.value.name}"
                    if isinstance(dependee_token, JobToken)
                    else ""
                )
                print(
                    f"rm prev - token {token_id_to_remove} tag {token_visited[token_id_to_remove][0].tag} (lvl {curr_tag_level}) ha prev {msg}id {token_row['dependee']} tag {dependee_token.tag} (lvl {get_tag_level(dependee_token.tag)}) -> {curr_tag_level == get_tag_level(dependee_token.tag)}"
                )
                await remove_prev(
                    token_row["dependee"],
                    dag_ports,
                    port_tokens,
                    token_visited,
                    context,
                    loading_context,
                )
            else:
                print(
                    f"rm prev - pulizia fermata al token {token_id_to_remove} con tag {token_visited[token_id_to_remove][0].tag}"
                )
        else:
            print(
                f"rm prev - token {token_id_to_remove} volevo rm prev token id {token_row['dependee']} tag {token_visited[token_row['dependee']][0].tag} ma è un token del ConnectorPort"
            )


def replace_token(
    port_name,
    token_id_to_replace: int,
    token_to_add: Token,
    dag_ports: MutableMapping[str, MutableSequence[str]],
    port_tokens: MutableMapping[str, MutableSequence[int]],
    token_visited: MutableMapping[int, Tuple[Token, bool]],
):
    if token_id_to_replace in port_tokens[port_name]:
        port_tokens[port_name].remove(token_id_to_replace)
    # token_visited.pop(token_id_to_replace, None)
    port_tokens[port_name].add(token_to_add.persistent_id)
    token_visited[token_to_add.persistent_id] = (token_to_add, True)
    print(
        f"new-method - replace token {token_id_to_replace} con {token_to_add.persistent_id} - I token della port {port_name} sono tutti disp? {all((token_visited[t_id][1] for t_id in port_tokens[port_name]))}"
    )
    if all((token_visited[t_id][1] for t_id in port_tokens[port_name])):
        for prev_port_name in get_prev_vertices(port_name, dag_ports):
            remove_from_graph(prev_port_name, dag_ports, port_tokens, token_visited)
        add_into_graph(
            port_name, None, INIT_DAG_FLAG, dag_ports, port_tokens, token_visited, None
        )


class JobVersion:
    __slots__ = ("job", "outputs", "step", "version")

    def __init__(
        self,
        job: Job = None,
        outputs: MutableMapping[str, Token] | None = None,
        step: Step = None,
        version: int = 1,
    ):
        self.job: Job = job
        self.outputs: MutableMapping[str, Token] | None = outputs
        self.step: Step = step
        self.version: int = version
