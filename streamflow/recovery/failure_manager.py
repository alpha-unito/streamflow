from __future__ import annotations

import os
import json
import asyncio
import logging
import datetime
from typing import MutableMapping, MutableSequence, cast, Tuple, Iterable

import pkg_resources

from streamflow.core.utils import (
    random_name,
    contains_id,
    get_class_fullname,
    get_class_from_name,
)
from streamflow.core.deployment import Connector, Location
from streamflow.core.exception import (
    FailureHandlingException,
    WorkflowTransferException,
    WorkflowExecutionException,
)
from streamflow.core.recovery import FailureManager
from streamflow.core.workflow import CommandOutput, Job, Status, Step, Port, Token
from streamflow.cwl.token import CWLFileToken
from streamflow.data import remotepath
from streamflow.log_handler import logger
from streamflow.recovery.recovery import JobVersion
from streamflow.token_printer import (
    temp_print_retag,
    print_debug_divergenza,
    print_grafici_parte_uno,
    print_grafici_post_remove,
    print_graph_figure,
    print_step_from_ports,
    dag_workflow,
)
from streamflow.workflow.step import ScatterStep, CombinatorStep
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.step import ExecuteStep
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.workflow.token import TerminationToken, JobToken

from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Workflow

from streamflow.workflow.utils import get_job_token


def check_double_reference(dag_ports):
    for tmpp in dag_ports[INIT_DAG_FLAG]:
        for tmpport_name, tmpnext_port_names in dag_ports.items():
            if tmpport_name != INIT_DAG_FLAG and tmpp in tmpnext_port_names:
                msg = f"Port {tmpp} appartiene sia a INIT che a {tmpport_name}"
                print("OOOOOOOOOOOOOOOOOOOOOOOOOOOO" * 100, "\n", msg)
                raise FailureHandlingException(msg)


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


async def _populate_workflow(
    failed_step, token_visited, new_workflow, loading_context, port_tokens, dag_ports
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
        new_workflow.add_port(port)
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
    # for row_dependency in await asyncio.gather(
    #     *(
    #         asyncio.create_task(
    #             new_workflow.context.database.get_step_from_output_port(port_id)
    #         )
    #         for port_id in ports.keys()
    #         if id_name[port_id] not in dag_ports[INIT_DAG_FLAG]
    #     )
    # ):
    #     steps.add(row_dependency["step"])
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
    return {
        t_id: all_token_visited[t_id]
        for token_list in port_tokens.values()
        for t_id in token_list
        if t_id != TOKEN_WAITER
    }


def reduce_graph(
    dag_ports,
    port_tokens,
    available_new_job_tokens,
    token_visited,
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
        replace_token(
            v["out-token-port-name"],
            v["old-out-token"],
            token_visited[v["new-out-token"]][0],
            dag_ports,
            port_tokens,
            token_visited,
        )
        print(
            f"Port_tokens {v['out-token-port-name']} - token {v['old-out-token']} sostituito con token {v['new-out-token']}. Quindi la port ha {port_tokens[v['out-token-port-name']]} tokens"
        )


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
    if port_name_to_add in port_tokens.keys():
        to_remove = set()
        to_add = set()
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
                        to_add.add(token_to_add.persistent_id)
                        to_remove.add(sibling_token_id)
            else:
                if token_to_add.tag == sibling_token.tag:
                    if (
                        token_visited[token_to_add.persistent_id][1]
                        or token_to_add.persistent_id > sibling_token_id
                    ):
                        to_add.add(token_to_add.persistent_id)
                        to_remove.add(sibling_token_id)
        for remove_t_id, add_t_id in zip(to_remove, to_add):
            port_tokens[port_name_to_add].remove(remove_t_id)
            # token_visited.pop(remove_t_id)
            port_tokens[port_name_to_add].add(add_t_id)
    else:
        port_tokens.setdefault(port_name_to_add, set()).add(token_to_add.persistent_id)


def add_into_graph(
    port_name_to_add: str,
    token_to_add: Token,
    port_name_key: str,
    dag_ports: MutableMapping[str, MutableSequence[str]],
    port_tokens: MutableMapping[str, MutableSequence[int]],
    token_visited: MutableMapping[int, Tuple[Token, bool]],
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
                port_name, None, INIT_DAG_FLAG, dag_ports, port_tokens, token_visited
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


def remove_token(
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
            port_name, None, INIT_DAG_FLAG, dag_ports, port_tokens, token_visited
        )


INIT_DAG_FLAG = "init"
TOKEN_WAITER = "twaiter"


class PortRecovery:
    def __init__(self, port):
        self.port = port
        self.waiting_token = 1


class JobRequest:
    def __init__(self):
        self.job_token: JobToken = None
        self.token_output: MutableMapping[str, Token] = {}
        self.lock = asyncio.Condition()
        self.is_running = True
        self.queue: MutableSequence[PortRecovery] = []
        self.workflow = None
        # todo: togliere direttamente le istanze delle port e usare una lista di port.id.
        #  Quando necessario le port si recuperano dal DB. Invece usare direttamente le port (ovvero l'attuale
        #  implementazione) grava in memoria perché ci sono tanti oggetti nella classe DefaultFailureManager
        #  Stessa cosa con i token_output. togliere l'istanza del token e mettere l'id


class DefaultFailureManager(FailureManager):
    def __init__(
        self,
        context: StreamFlowContext,
        max_retries: int | None = None,
        retry_delay: int | None = None,
    ):
        super().__init__(context)
        self.jobs: MutableMapping[str, JobVersion] = {}
        self.max_retries: int = max_retries
        self.retry_delay: int | None = retry_delay

        # { workflow.name : { port.id: [ token ] } }
        self.retags: MutableMapping[
            str, MutableMapping[str, MutableSequence[Token]]
        ] = {}

        # { job.name : RequestJob }
        self.job_requests: MutableMapping[str, JobRequest] = {}

    async def _has_token_already_been_recovered(
        self,
        token,
        token_visited,
        available_new_job_tokens,
        running_new_job_tokens,
        context,
    ):
        if (
            not isinstance(token, JobToken)
            or token.value.name not in self.job_requests.keys()
        ):
            return False

        async with self.job_requests[token.value.name].lock:
            job_request = self.job_requests[token.value.name]

            # todo: togliere parametro is_running, usare lo status dello step OPPURE status dentro lo scheduler
            # todo: questo controllo forse meglio toglierlo da qui. Dal momento tra cui li individuo is_running al
            #  momento in cui li attacco al wofklow (sync_rollbacks) i job potrebbero terminare e non avrei i token
            #  in input. Una soluzione facile è costruire tutto il grafo ignorando quelli running in questo
            #  momento. Poi sync_rollbacks aggiusta tutto. La pecca è di costruire tutto il grafo quando in realtà
            #  non serve perché poi verrà prunato fino al job running.
            if job_request.is_running:
                print(
                    f"Il job {token.value.name} {token.persistent_id} - è running, dovrei aspettare il suo output"
                )

                running_new_job_tokens[token.persistent_id] = {
                    "job_request": job_request
                }
                # raise FailureHandlingException(f"Job {token.value.name} is already running. This case is still a working progress")
            elif (
                job_request.job_token
                and job_request.job_token.persistent_id
                and token.persistent_id != job_request.job_token.persistent_id
                and job_request.token_output
            ):
                # todo: Fare lo load del token direttamente quando recupero
                #   così se più avanti serve, non si deve riaccedere al db ....
                #   update: FORSE NON SERVE FARLO
                out_tokens_json = await context.database.get_out_tokens_from_job_token(
                    token.persistent_id
                )
                # out_tokens_json dovrebbe essere una lista...caso in cui un job produce più token
                if not out_tokens_json:
                    print(
                        f"il job {token.value.name} ({token.persistent_id}) ha il out_token_json null",
                    )
                    return False
                print(
                    f"il job {token.value.name} ({token.persistent_id}) ha il out_token_json['id']: {out_tokens_json['id']} vs job_request.token_output: {job_request.token_output}",
                    f"\n\t out_tokens_json: {dict(out_tokens_json)}",
                )
                port_json = await context.database.get_port_by_token(
                    out_tokens_json["id"]
                )
                is_available = await _is_token_available(
                    job_request.token_output[port_json["name"]], context
                )
                print(
                    f"il job {token.value.name} ({token.persistent_id}) - out_token_json['id'] {out_tokens_json['id']} ha il port_json {dict(port_json)}",
                    f"\n\til token di output {job_request.token_output[port_json['name']]} è già disponibile? {is_available}",
                )
                if is_available:
                    available_new_job_tokens[token.persistent_id] = {
                        "out-token-port-name": port_json["name"],
                        # "new-job-token": job_request.job_token.persistent_id,
                        "new-out-token": job_request.token_output[
                            port_json["name"]
                        ].persistent_id,
                        # "old-job-token": token.persistent_id,
                        "old-out-token": out_tokens_json["id"],
                        # "value": out_tokens_json["value"],
                    }
                    token_visited[token.persistent_id] = (
                        token,
                        True,
                    )
                    # token_visited[job_request.job_token.persistent_id] = (
                    #     job_request.job_token,
                    #     True,
                    # )
                    token_visited[
                        job_request.token_output[port_json["name"]].persistent_id
                    ] = (job_request.token_output[port_json["name"]], True)
                    return True
                else:
                    self.job_requests[token.value.name].job_token = None
                    # todo: come gestire i vari kport? Se un token non è valido mettere a None solo la kport
                    #  di quel token o tutta? la cosa migliore sarebbe solo di quella persa
                    self.job_requests[token.value.name].token_output = None
                    self.job_requests[token.value.name].workflow = None
        return False

    async def _build_dag(
        self,
        tokens,
        failed_job,
        failed_step,
        workflow,
        loading_context,
        new_workflow_name,
        dir_path,
    ):
        # port -> next ports
        # { port_name : set(port_names) }
        dag_ports = {}

        # DEBUG
        port_name_id = {}  # {port_name: port_id}

        # { port_name : set(token_ids) }
        port_tokens = {}

        # todo: cambiarlo in {token_id: is_available} e quando è necessaria l'istanza del token recuperarla dal loading_context.load_token(id)
        # { token_id: (token, is_available)}
        all_token_visited = {}

        # {old_job_token_id : job_request_running}
        running_new_job_tokens = {}

        # {old_job_token_id : (new_job_token_id, new_output_token_id)}
        available_new_job_tokens = {}

        for k, t in failed_job.inputs.items():
            if t.persistent_id is None:
                raise FailureHandlingException("Token has not a persistent_id")
            # se lo step è Transfer, allora non tutti gli input del job saranno nello step
            if k in failed_step.input_ports.keys():
                dag_ports[failed_step.get_input_port(k).name] = set((failed_step.name,))
            else:
                print(f"Step {failed_step.name} has not the input port {k}")
        dag_ports[failed_step.get_input_port("__job__").name] = set((failed_step.name,))

        while tokens:
            token = tokens.pop()

            if not await self._has_token_already_been_recovered(
                token,
                all_token_visited,
                available_new_job_tokens,
                running_new_job_tokens,
                workflow.context,
            ):
                is_available = await _is_token_available(token, workflow.context)
                if isinstance(token, CWLFileToken):
                    print(
                        f"CWLFileToken ({token.persistent_id}) {token} is available {is_available}"
                    )

                # impossible case because when added in tokens, the elem is checked
                if token.persistent_id in all_token_visited.keys():
                    raise FailureHandlingException(
                        f"Token {token.persistent_id} already visited"
                    )

                port_row = await workflow.context.database.get_port_from_token(
                    token.persistent_id
                )
                port_name_id[port_row["name"]] = port_row["id"]
                step_row = await workflow.context.database.get_step_from_outport(
                    port_row["id"]
                )
                if issubclass(get_class_from_name(step_row["type"]), CombinatorStep):
                    is_available = False
                all_token_visited[token.persistent_id] = (token, is_available)

                if not is_available:
                    if prev_tokens := await loading_context.load_prev_tokens(
                        workflow.context, token.persistent_id
                    ):
                        for pt in prev_tokens:
                            prev_port_row = (
                                await workflow.context.database.get_port_from_token(
                                    pt.persistent_id
                                )
                            )
                            port_name_id[prev_port_row["name"]] = prev_port_row["id"]
                            add_into_graph(
                                port_row["name"],
                                token,
                                prev_port_row["name"],
                                dag_ports,
                                port_tokens,
                                all_token_visited,
                            )
                            if (
                                pt.persistent_id not in all_token_visited.keys()
                                and not contains_id(pt.persistent_id, tokens)
                            ):
                                tokens.append(pt)
                    else:
                        add_into_graph(
                            port_row["name"],
                            token,
                            INIT_DAG_FLAG,
                            dag_ports,
                            port_tokens,
                            all_token_visited,
                        )
                else:
                    add_into_graph(
                        port_row["name"],
                        token,
                        INIT_DAG_FLAG,
                        dag_ports,
                        port_tokens,
                        all_token_visited,
                    )
                # alternativa al doppio else -> if available or not prev_tokens: add_into_graph # però ricordarsi di definire "prev_tokens=None" prima del if not available. ALtrimenti protrebbe prendere il prev_tokens dell'iterazione precedente e non essere corretto
            else:
                port_row = await workflow.context.database.get_port_from_token(
                    token.persistent_id
                )
                add_into_vertex(port_row["name"], token, port_tokens, all_token_visited)

        print("While tokens terminato")
        check_double_reference(dag_ports)

        print(
            f"after.build - JobTokens: {set([ t.value.name for t, _ in all_token_visited.values() if isinstance(t, JobToken)])}"
        )

        all_token_visited = dict(sorted(all_token_visited.items()))
        print(
            "available_new_job_tokens:",
            len(available_new_job_tokens) > 0,
            json.dumps(available_new_job_tokens, indent=2),
        )

        # DEBUG: create port-step-token graphs
        await print_grafici_parte_uno(
            all_token_visited,
            None,
            dag_ports,
            dir_path,
            new_workflow_name,
            port_tokens,
            port_name_id,
            workflow,
            failed_step,
        )
        # DEBUG: controllo se ci sono branch che divergono (token di diverse esecuzioni che per via dei tag vengono ripetuti)
        print_debug_divergenza(all_token_visited, port_tokens)

        reduce_graph(
            dag_ports,
            port_tokens,
            available_new_job_tokens,
            all_token_visited,
        )
        print("grafo ridotto")
        print(
            f"after.riduzione - JobTokens: {set([ t.value.name for t, _ in all_token_visited.values() if isinstance(t, JobToken)])}"
        )

        # DEBUG: create port-step-token graphs post remove
        await print_grafici_post_remove(
            dag_ports,
            dir_path,
            new_workflow_name,
            port_tokens,
            port_name_id,
            workflow,
            failed_step,
        )

        return (
            dag_ports,
            port_tokens,
            get_necessary_tokens(port_tokens, all_token_visited),
            port_name_id,
        )

    async def _do_handle_failure(self, job: Job, step: Step) -> CommandOutput:
        # Delay rescheduling to manage temporary failures (e.g. connection lost)
        if self.retry_delay is not None:
            await asyncio.sleep(self.retry_delay)
        try:
            new_workflow, loading_context = await self._recover_jobs(job, step)

            # get new job created by ScheduleStep
            command_output = await self._execute_failed_job(
                job, step, new_workflow, loading_context
            )
            # When receiving a FailureHandlingException, simply fail
        except FailureHandlingException as e:
            logger.exception(e)
            raise
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        except WorkflowTransferException as e:
            logger.exception(e)
            print("WorkflowTransferException ma stavo gestendo execute job")
            raise
        except Exception as e:
            logger.exception(e)
            return await self.handle_exception(job, step, e)
        return command_output

    async def sync_rollbacks(
        self,
        dag_ports,
        port_tokens,
        token_visited,
        new_workflow,
        loading_context,
        dir_path,
        port_name_id,
        workflow,
        failed_step,
    ):
        job_executed_in_new_workflow = set()  # debug variable
        job_token_list = []
        for token, _ in token_visited.values():
            if isinstance(token, JobToken):
                job_token_list.append(token)

        job_token_list_updated = []
        for token in job_token_list:
            # update job request
            if token.value.name not in self.job_requests.keys():
                self.job_requests[token.value.name] = JobRequest()
                self.job_requests[token.value.name].workflow = new_workflow.name
                job_token_list_updated.append(token)
            else:
                async with self.job_requests[token.value.name].lock:
                    if self.job_requests[token.value.name].is_running:
                        print(
                            f"Job {token.value.name} già in esecuzione nel workflow {self.job_requests[token.value.name].workflow} -> Ricostruire dag"
                        )

                        # alternativa: dal dag_ports prendo tutte le port successive al job_token,
                        # con il database ricavo quali di queste porte sono output di uno ExecuteStep
                        # get outport ScheduleStep
                        job_token_port_row = (
                            await new_workflow.context.database.get_port_from_token(
                                token.persistent_id
                            )
                        )
                        job_token_port_name = job_token_port_row["name"]

                        # these can be outport of ExecuteStep and TransferStep
                        next_port_names = dag_ports[job_token_port_name]

                        rows = await new_workflow.context.database.get_executestep_outports_from_jobtoken(
                            token.persistent_id
                        )
                        execute_step_outports = await asyncio.gather(
                            *(
                                asyncio.create_task(
                                    Port.load(
                                        new_workflow.context,
                                        row["id"],
                                        loading_context,
                                        new_workflow,
                                    )
                                )
                                for row in rows
                                if row["name"] not in new_workflow.ports.keys()
                                and row["name"] not in failed_step.output_ports.values()
                            )
                        )
                        # controllo per debug
                        for p in execute_step_outports:
                            if (
                                p.name not in next_port_names
                                and p.name not in failed_step.output_ports.values()
                            ):
                                raise FailureHandlingException(
                                    f"wf {new_workflow.name} Port {p.name} non presente tra quelle presente nel dag_ports {next_port_names}"
                                )
                            print(
                                f"wf {new_workflow.name} Porta {p.name} trovata tra le next con il secondo metodo {next_port_names}"
                            )

                        # if the port can have more tokens (todo: caso non testato)
                        for row in rows:
                            if row["name"] not in new_workflow.ports.keys():
                                continue
                            for pr in self.job_requests[token.value.name].queue:
                                if pr.port.name == row["name"]:
                                    pr.waiting_token += 1
                                    print(
                                        f"new_workflow {new_workflow.name} already has port {pr.port.name}. Increased waiting_tokens: {pr.waiting_token}"
                                    )
                                    break

                        # add port in the workflow and create port recovery (request)
                        for port in execute_step_outports:
                            new_workflow.add_port(port)
                            print(
                                f"new_workflow {new_workflow.name} added port {port.name} -> {new_workflow.ports.keys()}"
                            )
                            self.job_requests[token.value.name].queue.append(
                                PortRecovery(port)
                            )

                        # rimuovo i token generati da questo job token, pero'
                        # non posso togliere tutti i token della port perché non so quali job li genera
                        execute_step_out_token_ids = await get_execute_step_out_token_ids(
                            [
                                row["depender"]
                                for row in await workflow.context.database.get_dependers(
                                    token.persistent_id
                                )
                            ],
                            new_workflow.context,
                        )
                        for t_id in execute_step_out_token_ids:
                            for prev_t_id in [
                                row["dependee"]
                                for row in await workflow.context.database.get_dependees(
                                    t_id
                                )
                            ]:
                                remove_token(
                                    prev_t_id, dag_ports, port_tokens, token_visited
                                )

                        check_double_reference(dag_ports)
                        for p in execute_step_outports:
                            if not is_next_of_someone(p.name, dag_ports):
                                dag_ports[INIT_DAG_FLAG].add(p.name)
                            port_tokens.setdefault(p.name, set()).add(TOKEN_WAITER)
                        check_double_reference(dag_ports)

                        print(
                            f"SYNCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC job {token.value.name}",
                            f"in esecuzione in wf {self.job_requests[token.value.name].workflow}",
                            f"servirà al wf {new_workflow.name}",
                        )
                        print_graph_figure(
                            {k: v for k, v in dag_ports.items() if k != INIT_DAG_FLAG},
                            dir_path + "/ports-sync-" + new_workflow.name,
                        )
                        await print_step_from_ports(
                            dag_ports,
                            port_name_id,
                            list(port_tokens.keys()),
                            workflow.context,
                            failed_step.name,
                            dir_path + "/steps-sync-" + new_workflow.name,
                        )
                    else:
                        print(
                            f"Job {token.value.name} posto running, mentre job_token e token_output posti a None. (Valore corrente jt: {self.job_requests[token.value.name].job_token} - t: {self.job_requests[token.value.name].token_output})"
                        )
                        job_token_list_updated.append(token)
                        self.job_requests[token.value.name].is_running = True
                        self.job_requests[token.value.name].job_token = None
                        self.job_requests[token.value.name].token_output = None
                        self.job_requests[token.value.name].workflow = new_workflow.name

        for token in job_token_list_updated:
            async with self.job_requests[token.value.name].lock:
                # save jobs recovered
                if token.value.name not in self.jobs.keys():
                    self.jobs[token.value.name] = JobVersion(
                        version=1,
                    )

                if (
                    self.max_retries is None
                    or self.jobs[token.value.name].version < self.max_retries
                ):
                    job_executed_in_new_workflow.add(token.value.name)
                    self.jobs[token.value.name].version += 1
                    logger.debug(
                        f"Updated Job {token.value.name} at {self.jobs[token.value.name].version} times"
                    )
                else:
                    logger.error(
                        f"FAILED Job {token.value.name} {self.jobs[token.value.name].version} times. Execution aborted"
                    )
                    raise FailureHandlingException()
        return job_executed_in_new_workflow

    # todo: situazione problematica
    #  A -> B
    #  A -> C
    #  B -> C
    # A ha successo, B fallisce (cade ambiente), viene rieseguito A, in C che input di A arriva?
    # quello vecchio? quello vecchio e quello nuovo? In teoria solo quello vecchio, da gestire comunque?
    # oppure lasciamo che fallisce e poi il failure manager prende l'output nuovo di A?
    async def _recover_jobs(
        self, failed_job: Job, failed_step: Step, add_failed_step: bool = False
    ) -> CommandOutput:
        loading_context = DefaultDatabaseLoadingContext()

        workflow = failed_step.workflow
        new_workflow = Workflow(
            context=workflow.context,
            type=workflow.type,
            name=random_name(),
            config=workflow.config,
        )

        # should be an impossible case
        if failed_step.persistent_id is None:
            raise FailureHandlingException(
                f"Workflow {workflow.name} has the step {failed_step.name} not saved in the database."
            )

        job_token = get_job_token(
            failed_job.name,
            failed_step.get_input_port("__job__").token_list,
        )

        dt = str(datetime.datetime.now()).replace(" ", "_").replace(":", ".")
        dir_path = f"graphs/{dt}"
        os.makedirs(dir_path)

        tokens = list(failed_job.inputs.values())  # tokens to check
        tokens.append(job_token)
        (
            dag_ports,
            port_tokens,
            token_visited,
            port_name_id,
        ) = await self._build_dag(
            tokens,
            failed_job,
            failed_step,
            workflow,
            loading_context,
            new_workflow.name,
            dir_path,
        )
        print("end build dag")

        # update class state (attributes) and jobs synchronization
        job_executed_in_new_workflow = await self.sync_rollbacks(
            dag_ports,
            port_tokens,
            token_visited,
            new_workflow,
            loading_context,
            dir_path,
            port_name_id,
            workflow,
            failed_step,
        )
        token_visited = get_necessary_tokens(port_tokens, token_visited)
        print("End sync-rollbacks")

        await _populate_workflow(
            failed_step,
            token_visited,
            new_workflow,
            loading_context,
            port_tokens,
            dag_ports,
        )
        print("end populate")

        if add_failed_step:
            if failed_step.name not in new_workflow.steps.keys():
                print(
                    f"Lo step fallito {failed_step.name} non è presente nel new_workflow {new_workflow.name} ... Lo aggiungo"
                )
                new_workflow.add_step(
                    await Step.load(
                        new_workflow.context,
                        failed_step.persistent_id,
                        loading_context,
                        new_workflow,
                    )
                )
            for port in failed_step.get_input_ports().values():
                if port.name not in new_workflow.ports.keys():
                    print(
                        f"La input port {port.name} dello step fallito {failed_step.name} non è presente nel new_workflow {new_workflow.name} ... La aggiungo"
                    )
                    new_workflow.add_port(
                        await Port.load(
                            new_workflow.context,
                            port.persistent_id,
                            loading_context,
                            new_workflow,
                        )
                    )

        self._save_for_retag(new_workflow, dag_ports, port_tokens, token_visited)
        print("end save_for_retag")

        await _put_tokens(
            new_workflow,
            dag_ports[INIT_DAG_FLAG],
            port_tokens,
            token_visited,
        )
        print("end _put_tokens")

        print("New workflow", new_workflow.name, "popolato così:")
        print("\tJobs da rieseguire:", job_executed_in_new_workflow)

        dag_workflow(new_workflow, dir_path + "/new-wf")

        for step in new_workflow.steps.values():
            print(f"step {step.name} wf {step.workflow.name}")
            try:
                print(
                    f"Step {step.name}\n\tinput ports",
                    {
                        k_p: [(t.persistent_id, t.tag) for t in port.token_list]
                        for k_p, port in step.get_input_ports().items()
                    },
                    "\n\tkey-port_name",
                    {k: v.name for k, v in step.get_input_ports().items()},
                )
            except Exception as e:
                print(f"exception {step.name} -> {e}")
                raise

        for token, _ in token_visited.values():
            if isinstance(token, JobToken):
                # free resources scheduler
                await workflow.context.scheduler.notify_status(
                    token.value.name, Status.ROLLBACK
                )
                workflow.context.scheduler.deallocate_from_job_name(
                    token.value.name, keep_job_allocation=True
                )

        print("VIAAAAAAAAAAAAAA " + new_workflow.name)

        await new_workflow.save(workflow.context)
        executor = StreamFlowExecutor(new_workflow)
        await executor.run()
        return new_workflow, loading_context

    def is_valid_tag(self, workflow_name, tag, output_port):
        if workflow_name not in self.retags.keys():
            return True
        if output_port.name not in self.retags[workflow_name].keys():
            return True

        token_list = self.retags[workflow_name][output_port.name]
        for t in token_list:
            if t.tag == tag:
                temp_print_retag(
                    workflow_name, output_port, tag, self.retags, "return true"
                )
                return True
        temp_print_retag(workflow_name, output_port, tag, self.retags, "return false")
        return False

    def _save_for_retag(self, new_workflow, dag_ports, port_tokens, token_visited):
        # todo: aggiungere la possibilità di eserguire comunque tutti i job delle scatter aggiungendo un parametro nel StreamFlow file
        if new_workflow.name not in self.retags.keys():
            self.retags[new_workflow.name] = {}

        for step in new_workflow.steps.values():
            if isinstance(step, ScatterStep):
                port = step.get_output_port()

                print(
                    f"_save_for_retag wf {new_workflow.name} -> port_tokens[{port.name}]:",
                    [
                        (t_id, token_visited[t_id][0].tag, token_visited[t_id][1])
                        for t_id in port_tokens[port.name]
                    ],
                )
                for t_id in port_tokens[port.name]:
                    if (
                        t_id not in dag_ports[INIT_DAG_FLAG]
                        and not token_visited[t_id][1]
                    ):
                        # todo: salvare solo il tag. Cambiamento che dovrà essere applicanto anche in is_valid_tag
                        self.retags[new_workflow.name].setdefault(port.name, set()).add(
                            token_visited[t_id][0]
                        )

    async def _execute_failed_job(
        self, failed_job, failed_step, new_workflow, loading_context
    ):
        try:
            new_job_token = get_job_token(
                failed_job.name,
                new_workflow.ports[
                    failed_step.get_input_port("__job__").name
                ].token_list,
            )
        except WorkflowExecutionException as err:
            raise FailureHandlingException(err)

        # get new job inputs
        new_inputs = {}
        for step_port_name, token in failed_job.inputs.items():
            original_port = failed_step.get_input_port(step_port_name)
            if original_port is None:
                print(
                    f"Original_port is None. It is the input port {step_port_name} in the job {failed_job.name} of the step {failed_step.name}"
                )
            if original_port.name in new_workflow.ports.keys():
                new_inputs[step_port_name] = get_token_by_tag(
                    token.tag, new_workflow.ports[original_port.name].token_list
                )
            else:
                new_inputs[step_port_name] = get_token_by_tag(
                    token.tag, original_port.token_list
                )
        new_job_token.value.inputs = new_inputs

        async with self.job_requests[failed_job.name].lock:
            if self.job_requests[failed_job.name].job_token is not None:
                print(
                    f"WARN WARN WARN job {failed_job.name} ha già un job_token {self.job_requests[failed_job.name].job_token.persistent_id}. Però qui non dovrebbe averne. Io volevo aggiungere job_token {new_job_token.persistent_id}."
                )
                pass
            self.job_requests[failed_job.name].job_token = new_job_token
        new_job = new_job_token.value
        new_step = await Step.load(
            new_workflow.context,
            failed_step.persistent_id,
            loading_context,
            new_workflow,
        )
        new_workflow.add_step(new_step)
        await new_step.save(new_workflow.context)

        await new_workflow.context.scheduler.notify_status(new_job.name, Status.RUNNING)
        cmd_out = await cast(ExecuteStep, new_step).command.execute(new_job)
        if cmd_out.status == Status.FAILED:
            jt = new_step.get_input_port("__job__").token_list
            logger.error(
                f"FAILED Job {new_job.name} with jobtoken.id {get_job_token(new_job.name, jt if isinstance(jt, Iterable) else [jt]).persistent_id} with error:\n\t{cmd_out.value}"
            )
            cmd_out = await self.handle_failure(new_job, new_step, cmd_out)
        print("Finito " + new_workflow.name)
        return cmd_out

    async def get_valid_job_token(self, job_token):
        if job_token.value.name in self.job_requests.keys():
            async with self.job_requests[job_token.value.name].lock:
                request = self.job_requests[job_token.value.name]
                # if it is not present, save it
                if not request.job_token:
                    request.job_token = job_token
                # return the valid job_token passed as parameter or that used in a rollback
                return request.job_token
        return job_token

    async def close(self):
        pass

    async def notify_jobs(self, job_name, out_port_name, token):
        print("Notify end job", job_name)
        if job_name in self.job_requests.keys():
            async with self.job_requests[job_name].lock:
                if self.job_requests[job_name].token_output is None:
                    self.job_requests[job_name].token_output = {}
                self.job_requests[job_name].token_output.setdefault(
                    out_port_name, token
                )
                self.job_requests[job_name].is_running = False
                # todo: fare a tutte le port nella queue la put del token
                elems = []
                print(
                    f"job {job_name} sta notificando sulla port_name {out_port_name}. Ci sono in coda",
                    len(self.job_requests[job_name].queue),
                    "ports:",
                    "".join(
                        [
                            f"\n\tHa trovato port_name {elem.port.name} port_id {elem.port.persistent_id} workflow {elem.port.workflow.name} token_list {elem.port.token_list} queues {elem.port.queues}. Waiting per {elem.waiting_token} prima del terminationtoken"
                            if elem
                            else "\n\t\tElem-None"
                            for elem in self.job_requests[job_name].queue
                        ]
                    ),
                )

                for elem in self.job_requests[job_name].queue:
                    if elem.port.name == out_port_name:
                        elem.port.put(token)
                        # todo: non è giusto, potrebbe dover aspettare altri token
                        elem.waiting_token -= 1
                        if elem.waiting_token == 0:
                            elems.append(elem)
                            elem.port.put(TerminationToken())
                        print(
                            "Token added into Port of another wf",
                            json.dumps(
                                {
                                    "p.name": elem.port.name,
                                    "p.id": elem.port.persistent_id,
                                    "wf": elem.port.workflow.name,
                                    "p.token_list_len": len(elem.port.token_list),
                                    "p.queue": list(elem.port.queues.keys()),
                                    "Ha ricevuto token": token.persistent_id,
                                },
                                indent=2,
                            ),
                            f"Aspetta {elem.waiting_token} tokens prima di mettere il terminationtoken"
                            if elem.waiting_token
                            else "Mandato anche termination token",
                        )

                for elem in elems:
                    self.job_requests[job_name].queue.remove(elem)
                print("Notify end job", job_name, "- done")

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "default_failure_manager.json")
        )

    async def handle_exception(
        self, job: Job, step: Step, exception: BaseException
    ) -> CommandOutput:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"Handling {type(exception).__name__} failure for job {job.name}"
            )
        if job.name in self.job_requests.keys():
            self.job_requests[job.name].is_running = False
        return await self._do_handle_failure(job, step)

    async def handle_failure(
        self, job: Job, step: Step, command_output: CommandOutput
    ) -> CommandOutput:
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"Handling command failure for job {job.name}")

        if job.name in self.job_requests.keys():
            self.job_requests[job.name].is_running = False
        return await self._do_handle_failure(job, step)

    async def handle_failure_transfer(self, job: Job, step: Step, port_name: str):
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"Handling {WorkflowTransferException.__name__} failure for job {job.name}"
            )

        if job.name in self.job_requests.keys():
            self.job_requests[job.name].is_running = False

        if self.retry_delay is not None:
            await asyncio.sleep(self.retry_delay)
        try:
            new_workflow, loading_context = await self._recover_jobs(
                job, step, add_failed_step=True
            )
            status = await self._execute_transfer_step(step, new_workflow, port_name)
        # When receiving a FailureHandlingException, simply fail
        except FailureHandlingException as e:
            logger.exception(e)
            raise
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        except WorkflowTransferException as e:
            logger.exception(e)
            return await self._handle_failure_transfer(job, step, port_name)
        except Exception as e:
            logger.exception(e)
            return await self.handle_exception(job, step, e)
        return status

    async def _execute_transfer_step(self, failed_step, new_workflow, port_name):
        token_list = (
            new_workflow.steps[failed_step.name].get_output_port(port_name).token_list
        )
        if len(token_list) != 2:
            raise FailureHandlingException(
                f"Step recovery {failed_step.name} did not generate the right number of tokens: {len(token_list)}"
            )
        if not isinstance(token_list[1], TerminationToken):
            raise FailureHandlingException(
                f"Step recovery {failed_step.name} did not work well. It moved two tokens instead of one: {[t.persistent_id for t in token_list]}"
            )
        return token_list[0]


class DummyFailureManager(FailureManager):
    async def close(self):
        ...

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "dummy_failure_manager.json")
        )

    async def handle_exception(
        self, job: Job, step: Step, exception: BaseException
    ) -> CommandOutput:
        raise exception

    async def handle_failure(
        self, job: Job, step: Step, command_output: CommandOutput
    ) -> CommandOutput:
        return command_output

    async def get_valid_job_token(self, job_token):
        return job_token

    def is_valid_tag(self, workflow_name, tag, output_port):
        return True

    async def notify_jobs(self, job_name, out_port_name, token):
        pass

    async def handle_failure_transfer(self, job: Job, step: Step, port_name: str):
        return None
