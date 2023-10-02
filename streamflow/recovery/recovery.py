from __future__ import annotations


import asyncio
import json
from typing import MutableMapping, MutableSequence, Tuple

from streamflow.core.utils import (
    get_class_fullname,
    get_class_from_name,
    contains_id,
)
from streamflow.core.exception import (
    FailureHandlingException,
)

from streamflow.core.workflow import Job, Step, Port, Token
from streamflow.recovery.utils import (
    str_tok,
    compare_tags_relaxed,
    is_output_port_forward,
    is_next_of_someone,
    get_steps_from_output_port,
    check_double_reference,
    _is_token_available,
    get_necessary_tokens,
    get_tag_level,
    get_prev_vertices,
    get_input_ports,
    INIT_DAG_FLAG,
    TOKEN_WAITER,
)
from streamflow.workflow.combinator import LoopCombinator, LoopTerminationCombinator
from streamflow.workflow.port import ConnectorPort, JobPort
from streamflow.workflow.step import (
    CombinatorStep,
    LoopTerminatorStep,
    ConditionalStep,
    TransferStep,
    Transformer,
)
from streamflow.workflow.token import (
    TerminationToken,
    JobToken,
)
from streamflow.core.workflow import Workflow


class RecoveryContext:
    def __init__(self, output_ports, port_name_ids, context):
        self.input_ports = set()
        self.output_ports = output_ports
        self.context = context
        self.port_name_ids = port_name_ids


class WorkflowRecovery(RecoveryContext):
    def __init__(self, dag_ports, output_ports, port_name_ids, context):
        super().__init__(output_ports, port_name_ids, context)
        self.dag_ports = dag_ports
        self.port_tokens = {}
        self.token_visited = {}

    def add_into_vertex(
        self,
        port_name_to_add: str,
        token_to_add: Token,
        output_port_forward: bool,
    ):
        if isinstance(token_to_add, JobToken):
            print(
                f"job token {token_to_add.value.name} (id {token_to_add.persistent_id}) port {port_name_to_add}"
            )
        if port_name_to_add in self.port_tokens.keys():
            for sibling_token_id in self.port_tokens[port_name_to_add]:
                sibling_token = self.token_visited[sibling_token_id][0]
                # If there are two of the same token in the same port, keep the newer token or that available
                # In the case of JobTokens, they are the same when they have the same job.name
                # In other token types, they are equal when they have the same tag
                if isinstance(token_to_add, JobToken):
                    if not isinstance(sibling_token, JobToken):
                        raise FailureHandlingException(
                            f"Non è un jobtoken {sibling_token}"
                        )
                    if token_to_add.value.name == sibling_token.value.name:
                        if (
                            self.token_visited[token_to_add.persistent_id][1]
                            or token_to_add.persistent_id > sibling_token_id
                        ):
                            self.port_tokens[port_name_to_add].remove(sibling_token_id)
                            break
                        else:
                            return
                else:
                    if token_to_add.tag == sibling_token.tag:
                        if (
                            self.token_visited[token_to_add.persistent_id][1]
                            or token_to_add.persistent_id > sibling_token_id
                        ):
                            self.port_tokens[port_name_to_add].remove(sibling_token_id)
                            break
                        else:
                            return
        print(
            f"Aggiungo token {str_tok(token_to_add)} id {token_to_add.persistent_id} tag {token_to_add.tag} nella port_tokens[{port_name_to_add}]"
        )
        if output_port_forward:
            for t_id in self.port_tokens.get(port_name_to_add, []):
                comparison = compare_tags_relaxed(
                    token_to_add.tag, self.token_visited[t_id][0].tag
                )
                if comparison == 0:
                    raise FailureHandlingException(
                        f"Nella port {port_name_to_add} ci sono due token con stesso tag -> id_a {t_id} tag_a {self.token_visited[t_id][0].tag} e id_b {token_to_add.persistent_id} tag_b {token_to_add.tag}"
                    )
                elif (
                    self.token_visited[token_to_add.persistent_id][1]
                    and comparison == -1
                ):
                    self.token_visited[token_to_add.persistent_id] = (
                        token_to_add,
                        False,
                    )
                elif self.token_visited[t_id][1] and comparison == 1:
                    self.token_visited[t_id] = (self.token_visited[t_id][0], False)
        self.port_tokens.setdefault(port_name_to_add, set()).add(
            token_to_add.persistent_id
        )

    async def add_into_graph(
        self,
        port_name_key: str,
        port_name_to_add: str,
        token_to_add: Token,
    ):
        output_port_forward = await is_output_port_forward(
            self.port_name_ids[port_name_to_add], self.context
        )
        if token_to_add:
            self.add_into_vertex(
                port_name_to_add,
                token_to_add,
                output_port_forward,
            )

        # todo: fare i controlli prima di aggiungere.
        #   in particolare se la port è presente in init e si sta aggiungendo in un'altra port
        #       - o non si aggiunge
        #       - o si toglie da init e si aggiunge nella nuova port
        #   stessa cosa viceversa, se è in un'altra port e si sta aggiungendo in init
        # edit. attuale implementazione: si toglie da init e si aggiunge alla port nuova
        if (
            INIT_DAG_FLAG in self.dag_ports.keys()
            and port_name_to_add in self.dag_ports[INIT_DAG_FLAG]
            and port_name_key != INIT_DAG_FLAG
        ):
            print(
                f"Inserisco la port {port_name_to_add} dopo la port {port_name_key}. però è già in INIT"
            )
        if port_name_key == INIT_DAG_FLAG and port_name_to_add in [
            pp
            for p, plist in self.dag_ports.items()
            for pp in plist
            if p != INIT_DAG_FLAG
        ]:
            print(
                f"Inserisco la port {port_name_to_add} in INIT. però è già in {port_name_key}"
            )
        self.dag_ports.setdefault(port_name_key, set()).add(port_name_to_add)

        # It is possible find some token available and other unavailable in a scatter
        # In this case the port is added in init and in another port.
        # If the port is next in another port, it is necessary to detach the port from the init next list
        if (
            INIT_DAG_FLAG in self.dag_ports.keys()
            and port_name_to_add in self.dag_ports[INIT_DAG_FLAG]
            and is_next_of_someone(port_name_to_add, self.dag_ports)
        ):
            if not output_port_forward:
                print(f"add_into_graph remove from init port {port_name_to_add}")
                self.dag_ports[INIT_DAG_FLAG].remove(port_name_to_add)
            else:
                print(f"add_into_graph in init keeps port {port_name_to_add}")

    async def build_dag(self, token_frontier, workflow, loading_context):
        # todo: cambiarlo in {token_id: is_available} e quando è necessaria l'istanza del token recuperarla dal loading_context.load_token(id)
        # { token_id: (token, is_available)}
        all_token_visited = self.token_visited

        # {old_job_token_id : job_request_running}
        running_new_job_tokens = {}

        # {old_job_token_id : (new_job_token_id, new_output_token_id)}
        available_new_job_tokens = {}

        while token_frontier:
            token = token_frontier.pop()

            if not await self.context.failure_manager._has_token_already_been_recovered(
                token,
                all_token_visited,
                available_new_job_tokens,
                running_new_job_tokens,
                workflow.context,
            ):
                # impossible case because when added in tokens, the elem is checked
                if token.persistent_id in all_token_visited.keys():
                    raise FailureHandlingException(
                        f"Token {token.persistent_id} already visited"
                    )

                port_row = await workflow.context.database.get_port_from_token(
                    token.persistent_id
                )
                self.port_name_ids[port_row["name"]] = port_row["id"]
                step_rows = await get_steps_from_output_port(
                    port_row["id"], workflow.context
                )
                if len(s := [dict(sr)["name"] for sr in step_rows]) > 1:
                    pass  # for debug
                is_available = await _is_token_available(token, workflow.context)

                for step_row in step_rows:
                    if issubclass(
                        get_class_from_name(step_row["type"]),
                        (
                            TransferStep,
                            Transformer,
                            ConditionalStep,
                            # ScatterStep,
                        ),
                    ):
                        is_available = False
                    elif issubclass(
                        get_class_from_name(step_row["type"]), CombinatorStep
                    ):
                        # todo: vedere i next_tokens rispetto a token e aggiungere le next_port a port (così attacchiamo combinatorstep a tutti i token-transformer e non solo a param-file)
                        is_available = False

                all_token_visited[token.persistent_id] = (token, is_available)
                if not is_available:
                    if prev_tokens := await loading_context.load_prev_tokens(
                        workflow.context, token.persistent_id
                    ):
                        prev_port_rows = await asyncio.gather(
                            *(
                                asyncio.create_task(
                                    workflow.context.database.get_port_from_token(
                                        pt.persistent_id
                                    )
                                )
                                for pt in prev_tokens
                            )
                        )
                        print(
                            f"Dal token id {token.persistent_id} ho recuperato prev-tokens {[pt.persistent_id for pt in prev_tokens]}"
                        )
                        for pt, prev_port_row in zip(prev_tokens, prev_port_rows):
                            self.port_name_ids[prev_port_row["name"]] = prev_port_row[
                                "id"
                            ]
                            await self.add_into_graph(
                                prev_port_row["name"],
                                port_row["name"],
                                token,
                            )
                            if (
                                pt.persistent_id not in all_token_visited.keys()
                                and not contains_id(pt.persistent_id, token_frontier)
                            ):
                                token_frontier.append(pt)
                    else:
                        await self.add_into_graph(
                            INIT_DAG_FLAG,
                            port_row["name"],
                            token,
                        )
                else:
                    print(
                        f"Il token {token.persistent_id} è disponibile, quindi aggiungo la sua port {port_row['name']} tra gli init"
                    )
                    await self.add_into_graph(
                        INIT_DAG_FLAG,
                        port_row["name"],
                        token,
                    )
                # alternativa al doppio else -> if available or not prev_tokens: add_into_graph # però ricordarsi di definire "prev_tokens=None" prima del if not available. ALtrimenti protrebbe prendere il prev_tokens dell'iterazione precedente e non essere corretto
            else:
                port_row = await workflow.context.database.get_port_from_token(
                    token.persistent_id
                )
                await self.add_into_graph(
                    INIT_DAG_FLAG,
                    port_row["name"],
                    token,
                )
                # self.add_into_vertex(
                #     port_row["name"],
                #     token,
                #     await is_output_port_forward(port_row["id"], workflow.context),
                # )

        print("While tokens terminato")
        check_double_reference(self.dag_ports)

        print(
            f"after.build - JobTokens: {set([ t.value.name for t, _ in get_necessary_tokens(self.port_tokens, all_token_visited).values() if isinstance(t, JobToken)])}"
        )

        all_token_visited = dict(sorted(all_token_visited.items()))
        for t, a in all_token_visited.values():
            print(
                f"Token id: {t.persistent_id} tag: {t.tag} val: {str_tok(t)} is available? {a}"
            )
        for p, t in dict(sorted(self.port_tokens.items())).items():
            print(f"Port_tokens[{p}]: {t}")
        for p, nps in self.dag_ports.items():
            print(f"dag_ports[{p}]: {nps}")
        print(
            "available_new_job_tokens:",
            len(available_new_job_tokens) > 0,
            json.dumps(available_new_job_tokens, indent=2),
        )

        print("Pre riduzione")
        await self.reduce_graph(
            available_new_job_tokens,
            loading_context,
        )
        print("grafo ridotto")

        print(
            # f"after.riduzione - JobTokens: {set([ t.value.name for t, _ in all_token_visited.values() if isinstance(t, JobToken)])}"
            f"after.riduzione - JobTokens: {set([t.value.name for t, _ in get_necessary_tokens(self.port_tokens, all_token_visited).values() if isinstance(t, JobToken)])}"
        )

    async def reduce_graph(
        self,
        available_new_job_tokens,
        loading_context,
    ):
        for v in available_new_job_tokens.values():
            if v["out-token-port-name"] not in self.port_tokens.keys():
                raise FailureHandlingException("Non c'è la porta. NON VA BENE")

        # todo: aggiustare nel caso in cui uno step abbia più port di output.
        #   Nella replace_token, se i token della port sono tutti disponibili
        #   allora rimuove tutte le port precedenti.
        #   ma se i token della seconda port di output dello step non sono disponibili
        #   è necessario eseguire tutte le port precedenti.
        for v in available_new_job_tokens.values():
            if v["out-token-port-name"] in self.port_tokens.keys():
                await self.replace_token(
                    v["out-token-port-name"],
                    v["old-out-token"],
                    self.token_visited[v["new-out-token"]][0],
                )
                await self.remove_prev(
                    v["old-out-token"],
                    loading_context,
                )
                print(
                    f"Port_tokens {v['out-token-port-name']} - token {v['old-out-token']} sostituito con token {v['new-out-token']}. Quindi la port ha {self.port_tokens[v['out-token-port-name']]} tokens"
                )
            else:
                print(
                    f"Port_tokens {v['out-token-port-name']} - port non più presente. Non serve più eseguire lo step annesso"
                )
                pass

    async def replace_token(
        self,
        port_name,
        token_id_to_replace: int,
        token_to_add: Token,
    ):
        if token_id_to_replace in self.port_tokens[port_name]:
            self.port_tokens[port_name].remove(token_id_to_replace)
        # token_visited.pop(token_id_to_replace, None)
        self.port_tokens[port_name].add(token_to_add.persistent_id)
        self.token_visited[token_to_add.persistent_id] = (token_to_add, True)
        print(
            f"new-method - replace token {token_id_to_replace} con {token_to_add.persistent_id} - I token della port {port_name} sono tutti disp? {all((self.token_visited[t_id][1] for t_id in self.port_tokens[port_name]))}"
        )
        if all((self.token_visited[t_id][1] for t_id in self.port_tokens[port_name])):
            for prev_port_name in get_prev_vertices(port_name, self.dag_ports):
                await self.remove_from_graph(prev_port_name)
            await self.add_into_graph(INIT_DAG_FLAG, port_name, None)

    async def remove_from_graph(self, port_name_to_remove: str):
        other_ports_to_remove = set()
        # Removed all occurrences as next port
        for port_name, next_port_names in self.dag_ports.items():
            if port_name_to_remove in next_port_names:
                next_port_names.remove(port_name_to_remove)
            if len(next_port_names) == 0:
                other_ports_to_remove.add(port_name)

        # removed all tokens generated by port
        for t_id in self.port_tokens.pop(port_name_to_remove, ()):
            # token_visited.pop(t_id)
            print(
                f"New-method - pop token id {t_id} from port_tokens[{port_name_to_remove}]"
            )

        # removed in the graph and moved its next port in INIT
        for port_name in self.dag_ports.pop(port_name_to_remove, ()):
            if not is_next_of_someone(port_name, self.dag_ports):
                await self.add_into_graph(INIT_DAG_FLAG, port_name, None)
        print(f"New-method - pop {port_name_to_remove} from dag_ports and port_tokens")
        # remove vertex detached from the graph
        for port_name in other_ports_to_remove:
            await self.remove_from_graph(port_name)

    async def remove_token_by_id(self, token_id_to_remove: int):
        # token_visited.pop(token_id_to_remove, None)
        print(f"new-method - remove token {token_id_to_remove} from visited")
        ports_to_remove = set()
        for port_name, token_ids in self.port_tokens.items():
            if token_id_to_remove in token_ids:
                token_ids.remove(token_id_to_remove)
                print(
                    f"new-method - remove token {token_id_to_remove} from {port_name}"
                )
            if len(token_ids) == 0:
                ports_to_remove.add(port_name)
        for port_name in ports_to_remove:
            await self.remove_from_graph(port_name)

    async def remove_token(self, token, port_name):
        msg = (
            "job {token.value.name}"
            if isinstance(token, JobToken)
            else f"tag {token.tag}"
        )
        if port_name in self.port_tokens.keys():
            for t_id in self.port_tokens[port_name]:
                if (
                    not isinstance(token, JobToken)
                    and self.token_visited[t_id][0].tag == token.tag
                ) or (
                    isinstance(token, JobToken)
                    and self.token_visited[t_id][0].value.name == token.value.name
                ):
                    print(
                        f"Rimuovo token {t_id} con {msg} dal port_tokens[{port_name}]"
                    )
                    await self.remove_token_by_id(t_id)
                    return
            print(
                f"Volevo rimuovere token con {msg} dal port_tokens[{port_name}] ma non ce n'è"
            )
        else:
            print(
                f"Volevo rimuovere token con {msg} ma non c'è port {port_name} in port_tokens"
            )

    # richiamare ricorsivamente finche i token.tag hanno sempre lo stesso livello o se si trova un JobToken
    async def remove_prev(
        self,
        token_id_to_remove: int,
        loading_context,
    ):
        curr_tag_level = get_tag_level(self.token_visited[token_id_to_remove][0].tag)
        for token_row in await self.context.database.get_dependees(token_id_to_remove):
            port_row = await self.context.database.get_port_from_token(
                token_row["dependee"]
            )
            if token_row["dependee"] not in self.token_visited.keys():
                print(
                    f"rm prev - token id {token_row['dependee']} non presente nei token_visited"
                )
                continue
            elif port_row["type"] != get_class_fullname(ConnectorPort):
                if token_row["dependee"] not in self.token_visited.keys():
                    self.token_visited[token_row["dependee"]] = (
                        await loading_context.load_token(
                            self.context, token_row["dependee"]
                        ),
                        False,
                    )
                    print(
                        f"rm prev - prev token id {token_row['dependee']} tag {self.token_visited[token_row['dependee']][0].tag} non è presente tra i token visitati, lo aggiungo"
                    )
                print(
                    f"rm prev - token {token_id_to_remove} rm prev token id: {token_row['dependee']} val: {str_tok(self.token_visited[token_row['dependee']][0])} tag: {self.token_visited[token_row['dependee']][0].tag}"
                )

                port_dependee_row = await self.context.database.get_port_from_token(
                    token_row["dependee"]
                )
                dependee_token = self.token_visited[token_row["dependee"]][0]
                a = port_dependee_row["type"] == get_class_fullname(JobPort)
                b = isinstance(dependee_token, JobToken)
                if a != b:
                    raise FailureHandlingException("Diverse port")
                if isinstance(dependee_token, JobToken) or (
                    not isinstance(self.token_visited[token_id_to_remove][0], JobToken)
                    and curr_tag_level == get_tag_level(dependee_token.tag)
                ):
                    await self.remove_token(dependee_token, port_dependee_row["name"])
                    msg = (
                        f"job {dependee_token.value.name} "
                        if isinstance(dependee_token, JobToken)
                        else ""
                    )
                    print(
                        f"rm prev - token {token_id_to_remove} tag {self.token_visited[token_id_to_remove][0].tag} (lvl {curr_tag_level}) ha prev {msg}id {token_row['dependee']} tag {dependee_token.tag} (lvl {get_tag_level(dependee_token.tag)}) -> {curr_tag_level == get_tag_level(dependee_token.tag)}"
                    )
                    await self.remove_prev(token_row["dependee"], loading_context)
                else:
                    print(
                        f"rm prev - pulizia fermata al token {token_id_to_remove} con tag {self.token_visited[token_id_to_remove][0].tag}"
                    )
            else:
                print(
                    f"rm prev - token {token_id_to_remove} volevo rm prev token id {token_row['dependee']} tag {self.token_visited[token_row['dependee']][0].tag} ma è un token del ConnectorPort"
                )


class LoopRecovery(RecoveryContext):
    async def explore_loop(self):
        """
        Explore the loop with a bottom-up visit
        """
        ports_visited = set()

        ports_frontier = set()
        for port_name in self.output_ports:
            ports_frontier.add(port_name)
        while ports_frontier:
            port_name = ports_frontier.pop()
            ports_visited.add(port_name)
            port_id = self.port_name_ids[port_name]
            dep_steps_port_rows = (
                await self.context.database.get_steps_from_output_port(port_id)
            )
            step_rows = await asyncio.gather(
                *(
                    asyncio.create_task(
                        self.context.database.get_step(dependency_row["step"])
                    )
                    for dependency_row in dep_steps_port_rows
                )
            )
            for step_row in step_rows:
                if issubclass(get_class_from_name(step_row["type"]), CombinatorStep):
                    combinator_row = json.loads(step_row["params"])["combinator"]
                    if issubclass(
                        get_class_from_name(combinator_row["type"]), LoopCombinator
                    ):  # oppure  è meglio get_class_from_name() is LoopCombinator?
                        for port_row in await get_input_ports(
                            step_row["id"], self.context
                        ):
                            if port_row["name"] not in self.port_name_ids.keys():
                                self.port_name_ids[port_row["name"]] = port_row["id"]
                            self.input_ports.add(port_row["name"])
                        continue
                    elif issubclass(
                        get_class_from_name(combinator_row["type"]),
                        LoopTerminationCombinator,
                    ):
                        new_loop = False
                        port_rows = await get_input_ports(step_row["id"], self.context)
                        for port_row in port_rows:
                            if port_row["name"] not in self.port_name_ids.keys():
                                self.port_name_ids[port_row["name"]] = port_row["id"]
                            new_loop = port_row["name"] not in self.output_ports
                        if new_loop:
                            sub_rl = LoopRecovery(
                                output_ports=[
                                    port_row["name"] for port_row in port_rows
                                ],
                                port_name_ids={
                                    port_row["name"]: port_row["id"]
                                    for port_row in port_rows
                                },
                                context=self.context,
                            )
                            await sub_rl.explore_loop()
                            for port_name in sub_rl.input_ports:
                                if port_name not in self.port_name_ids.keys():
                                    self.port_name_ids[
                                        port_name
                                    ] = sub_rl.port_name_ids[port_name]
                                if port_name not in ports_visited:
                                    ports_frontier.add(port_name)
                        pass
                    pass
                for port_row in await get_input_ports(step_row["id"], self.context):
                    if port_row["name"] not in self.port_name_ids.keys():
                        self.port_name_ids[port_row["name"]] = port_row["id"]
                    if port_row["name"] not in ports_visited:
                        ports_frontier.add(port_row["name"])
        pass


async def _put_tokens(
    new_workflow: Workflow,
    init_ports: MutableSequence[str],
    port_tokens: MutableMapping[str, MutableSequence[int]],
    token_visited: MutableMapping[int, Tuple[Token, bool]],
    dag_ports,
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
        if (
            # port.name not in forward_output_ports and
            len(port.token_list) > 0
            and len(port.token_list) == len(port_tokens[port_name])
        ):
            port.put(TerminationToken())
        else:
            print(
                f"Port {port.name} with {len(port.token_list)} tokens. NO TerminationToken"
            )


async def _populate_workflow_lean(
    wr,
    port_ids: MutableSequence[int],
    step_ids: MutableSequence[int],
    failed_step,
    new_workflow,
    loading_context,
):
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
            for port_id in port_ids
        )
    ):
        if port.name not in new_workflow.ports.keys():
            new_workflow.add_port(port)
            print(f"wf {new_workflow.name} add port {port.name}")
        else:
            print(
                f"La port {port.name} è già presente nel workflow {new_workflow.name}"
            )
    print("Port caricate")

    step_name_id = {}
    for sid, step in zip(
        await asyncio.gather(
            *(
                asyncio.create_task(
                    Step.load(
                        new_workflow.context,
                        step_id,
                        loading_context,
                        new_workflow,
                    )
                )
                for step_id in step_ids
            )
        ),
    ):
        step_name_id[step.name] = sid
        if isinstance(step, CWLLoopConditionalStep):
            pass
        if not (set(step.input_ports.values()) - set(new_workflow.ports.keys())):
            if isinstance(step, CWLLoopConditionalStep):
                pass
            if isinstance(step, CWLLoopConditionalStep):
                if step.name + "-recovery" in new_workflow.steps.keys():
                    continue
                port_name = list(step.input_ports.values()).pop()
                loop_terminator_step = CustomLoopConditionalStep(
                    step.name + "-recovery",
                    new_workflow,
                    len(wr.port_tokens[port_name]),
                )
                for k_port, port in step.get_input_ports().items():
                    loop_terminator_step.add_input_port(k_port, port)
                try:
                    for k_port, port in step.get_output_ports().items():
                        loop_terminator_step.add_output_port(k_port, port)
                except Exception:
                    pass
                for k_port, port in step.get_skip_ports().items():
                    if port.name in wr.port_name_ids.values():
                        loop_terminator_step.add_skip_port(k_port, port)
                    else:
                        print(f"wf {new_workflow.name} pop skip-port {port.name}")
                        new_workflow.ports.pop(port.name)
                new_workflow.add_step(loop_terminator_step)
            elif isinstance(step, CustomLoopConditionalStep):
                if step.name in new_workflow.steps.keys():
                    continue
                port_name = list(step.input_ports.values()).pop()
                loop_terminator_step = CustomLoopConditionalStep(
                    step.name, new_workflow, len(wr.port_tokens[port_name])
                )
                for k_port, port in step.get_input_ports().items():
                    loop_terminator_step.add_input_port(k_port, port)
                for k_port, port in step.get_output_ports().items():
                    loop_terminator_step.add_output_port(k_port, port)
                for k_port, port in step.get_skip_ports().items():
                    if port.name in wr.port_name_ids.values():
                        loop_terminator_step.add_skip_port(k_port, port)
                    else:
                        print(f"wf {new_workflow.name} pop skip-port {port.name}")
                        new_workflow.ports.pop(port.name)
                new_workflow.add_step(loop_terminator_step)
            else:
                new_workflow.add_step(step)
        else:
            print(
                f"Step {step.name} non viene essere caricato perché nel wf {new_workflow.name} mancano le ports {set(step.input_ports.values()) - set(new_workflow.ports.keys())}"
            )
            pass
    print("Step caricati")

    rm_steps = set()
    missing_ports = set()
    for step in new_workflow.steps.values():
        if isinstance(step, InputInjectorStep):
            try:
                tmp_ins = step.input_ports.values()
            except Exception as e:
                print(e)
            continue
        for dep_name, p_name in step.output_ports.items():
            if p_name not in new_workflow.ports.keys():
                # problema nato dai loop. when-loop ha output tutti i param. Però il grafo è costruito sulla presenza o
                # meno dei file, invece i param str, int, ..., no. Quindi le port di questi param non vengono esplorate
                # le aggiungo ma vengono usati come "port pozzo" ovvero il loro output non viene utilizzato
                print(
                    f"Aggiungo port {p_name} al wf {new_workflow.name} perché è un output port dello step {step.name}"
                )
                depe_row = await new_workflow.context.database.get_output_port(
                    step_name_id[step.name], dep_name
                )
                missing_ports.add(depe_row["port"])
                pass
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
        if port.name not in new_workflow.ports.keys():
            new_workflow.add_port(port)

    for step in new_workflow.steps.values():
        if isinstance(step, InputInjectorStep):
            try:
                tmp_ins = step.input_ports.values()
            except Exception as e:
                print(e)
            continue
        for p_name in step.input_ports.values():
            if p_name not in new_workflow.ports.keys():
                # problema nato dai loop. Vengono caricati nel new_workflow tutti gli step che hanno come output le port
                # nel grafo. Però nei loop, più step hanno stessa porta di output (forwad, backprop, loop-term).
                # per capire se lo step sia necessario controlliamo che anche le sue port di input siano state caricate
                print(
                    f"Rimuovo step {step.name} dal wf {new_workflow.name} perché manca la input port {p_name}"
                )
                rm_steps.add(step.name)
                pass
    for step_name in rm_steps:
        new_workflow.steps.pop(step_name)
    print("Ultime Port caricate")


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
