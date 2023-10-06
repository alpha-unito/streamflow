from __future__ import annotations


import asyncio
import json
from typing import MutableMapping, MutableSequence, Tuple

from streamflow.core.context import StreamFlowContext
from streamflow.core.utils import (
    get_class_fullname,
    get_class_from_name,
    contains_id,
)
from streamflow.core.exception import (
    FailureHandlingException,
)

from streamflow.core.workflow import Job, Step, Port, Token
from streamflow.cwl.step import CWLLoopConditionalStep, CWLRecoveryLoopConditionalStep
from streamflow.cwl.transformer import (
    BackPropagationTransformer,
    ForwardTransformer,
    OutputForwardTransformer,
)
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
    INIT_DAG_FLAG,
    TOKEN_WAITER,
    get_key_by_value,
    get_recovery_loop_expression,
)
from streamflow.workflow.combinator import LoopTerminationCombinator
from streamflow.workflow.port import ConnectorPort, JobPort
from streamflow.workflow.step import (
    CombinatorStep,
    ConditionalStep,
    TransferStep,
    Transformer,
    LoopOutputStep,
    InputInjectorStep,
    LoopCombinatorStep,
)
from streamflow.workflow.token import (
    TerminationToken,
    JobToken,
    IterationTerminationToken,
)
from streamflow.core.workflow import Workflow


class RecoveryContext:
    def __init__(self, output_ports, port_name_ids, context):
        self.input_ports = set()
        self.output_ports = output_ports
        self.context = context
        self.port_name_ids = port_name_ids


class WorkflowRecovery(RecoveryContext):
    def __init__(
        self,
        dag_ports: MutableMapping[str, MutableSequence[str]],
        output_ports: MutableSequence[str],
        port_name_ids: MutableMapping[str, int],
        context: StreamFlowContext,
    ):
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
        if False and output_port_forward:
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

        loop_combinator_ports = {}
        loop_output_ports = {}
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

                invalidate = False
                for step_row in step_rows:
                    if issubclass(
                        get_class_from_name(step_row["type"]),
                        BackPropagationTransformer,
                    ):
                        invalidate = False
                        break
                    elif issubclass(
                        get_class_from_name(step_row["type"]),
                        (
                            TransferStep,
                            # Transformer is necessary to have the same tag in all input parameters in the scatter case
                            Transformer,
                            ConditionalStep,
                            ForwardTransformer,
                            CombinatorStep,
                            # CustomLoopConditionalStep,  # deprecated
                            # ScatterStep,
                        ),
                    ):
                        invalidate = True
                        if issubclass(
                            get_class_from_name(step_row["type"]),
                            LoopCombinatorStep,
                        ):
                            loop_combinator_ports.setdefault(
                                port_row["name"], port_row["id"]
                            )
                    elif issubclass(
                        get_class_from_name(step_row["type"]), LoopOutputStep
                    ):
                        # questo step si può usare come punto di checkpoint. Se l'output qui è available
                        # si può evitare di fare il loop
                        # edit. affermazione da ripensare dato che questo step è necessario nel recovery di iterazioni passate
                        # possibile soluzione mettere dopo di questo step un altro step di forward e usare lui come punto di checkpoint
                        loop_output_ports[port_row["id"]] = port_row["name"]
                # if invalidate is True, then is_available is set to False, otherwise leave the original value
                if invalidate:
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

    async def get_port_and_step_ids(self):
        ports = {}
        steps = set()
        # todo: cambiare port_name_ids da {str : int} a { str : {int} } e togliere questa var tmp
        tmp = {k: {v} for k, v in self.port_name_ids.items()}
        for token_id, (_, is_available) in self.token_visited.items():
            row = await self.context.database.get_port_from_token(token_id)
            if TOKEN_WAITER not in self.port_tokens[row["name"]]:
                print(
                    f"get_port_and_step_ids: Token {token_id} ha caricato port id {row['id']} name {row['name']}"
                )
                if row["id"] in ports.keys():
                    ports[row["id"]] = ports[row["id"]] and is_available
                else:
                    ports[row["id"]] = is_available
                tmp.setdefault(row["name"], set()).add(row["id"])
            # else nothing because the ports are already loading in the new_workflow

        back_prop_ports = {
            port_id
            for port_id in ports.keys()
            if await is_output_port_forward(port_id, self.context)
        }
        for row_dependencies in await asyncio.gather(
            *(
                asyncio.create_task(
                    self.context.database.get_steps_from_output_port(port_id)
                )
                for port_id in ports.keys()
                if (
                    get_key_by_value(port_id, tmp) not in self.dag_ports[INIT_DAG_FLAG]
                    or port_id in back_prop_ports
                )
            )
        ):
            for row_dependency in row_dependencies:
                print(
                    f"get_port_and_step_ids: Step {(await self.context.database.get_step(row_dependency['step']))['name']} recuperato dalla port {get_key_by_value(row_dependency['port'], tmp)}"
                )
                # todo prendere tutti i suoi input ports da dependency e se i loro nomi non sono presenti nel new_workflow,
                #  significa che è uno step da non caricare.
                #  Questo evita il controllo che viene effettuato sotto if not(set() - set())
                steps.add(row_dependency["step"])

        # Soluzione carina e poco costosa rispetto a quella attuale. Solo che con i sync e replace si scassa.
        # Ad esempio nella param-scatter con i sync posso trovare tutti i token available... ma nella normale esploratione
        # i token di questo step vengono tutti segnati non disponibili per risalire all'execute dello step precedente
        # for row_dependencies in await asyncio.gather(
        #     *(
        #         asyncio.create_task(
        #             self.context.database.get_steps_from_output_port(port_id)
        #         )
        #         for port_id, port_availability in ports.items()
        #         if not port_availability
        #     )
        # ):
        #     for row_dependency in row_dependencies:
        #         steps.add(row_dependency["step"])
        return set(ports.keys()), steps


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
        loop_combinator_input = False
        reduced = False
        # Valori validi 0 oppure 1. Se però contiamo tutti i tipi di forward allora può essere maggiore di 1
        back_prop_list = [
            s
            for s in port.get_input_steps()
            if isinstance(s, BackPropagationTransformer)
        ]
        output_port_forward = len(back_prop_list) == 1
        # output_port_forward = await is_output_port_forward(
        #     wr.port_name_ids[port_name], new_workflow.context
        # )
        for ss in port.get_output_steps():
            # se la port è output di un back-prop allora metti meno token
            if isinstance(ss, LoopCombinatorStep):
                loop_combinator_input = True

                if len(token_list) == 1:  # tmp soluzione
                    break
                found = False
                for ss1 in port.get_input_steps():
                    found = found or isinstance(ss1, BackPropagationTransformer)
                if not found:
                    break
                break
        for t in token_list:
            if isinstance(t, TerminationToken):
                raise FailureHandlingException(
                    f"Aggiungo un termination token nell port {port.name} ma non dovrei"
                )
            port.put(t)
            if output_port_forward:
                break

        len_port_token_list = len(port.token_list)
        len_port_tokens = len(port_tokens[port_name])
        if loop_combinator_input:
            print(
                f"put_tokens: Port {port.name} inserts TerminationToken as second element in token_list (it simulates the TerminationToken of InputForwardStep)"
            )
            port.token_list.insert(1, TerminationToken())
        if reduced:
            len_port_tokens -= 1

        if len_port_token_list > 0 and len_port_token_list == len_port_tokens:
            print(f"put_tokens: Port {port.name} with {len(port.token_list)} tokens")
            if loop_combinator_input and not output_port_forward:
                if len_port_token_list > 1:
                    if isinstance(port.token_list[-1], TerminationToken):
                        token = port.token_list[-1]
                    else:
                        tag = port.token_list[-1].tag.split(".")
                        tag[-1] = str(int(tag[-1]) + 1)
                        token = Token(tag=".".join(tag), value=None)
                        print(
                            f"put_tokens: Port {port.name}, with {len(port.token_list)} tokens, inserts EMPTY token with tag {token.tag}"
                        )
                        port.put(token)
                    print(
                        f"put_tokens: Port {port.name}, with {len(port.token_list)} tokens, inserts IterationTerminationToken with tag {token.tag}"
                    )
                    port.put(IterationTerminationToken(token.tag))
                print(
                    f"put_tokens: Port {port.name}, with {len(port.token_list)} tokens, inserts IterationTerminationToken with tag {port.token_list[0].tag}"
                )
                port.put(IterationTerminationToken(port.token_list[0].tag))
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

    add_step_ids = set()
    replace_step = None
    step_name_id = {}
    step_instances = {}
    for sid, step in zip(
        step_ids,
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
        step_instances[sid] = step
        if not (set(step.input_ports.values()) - set(new_workflow.ports.keys())):
            if isinstance(step, CWLLoopConditionalStep):
                if not replace_step:
                    replace_step = step
                else:
                    continue
            elif isinstance(step, OutputForwardTransformer):
                for (
                    step_dep_row
                ) in await new_workflow.context.database.get_steps_from_input_port(
                    wr.port_name_ids[step.get_output_port().name]
                ):
                    step_row = await new_workflow.context.database.get_step(
                        step_dep_row["step"]
                    )
                    if step_row["name"] not in new_workflow.steps.keys():
                        if issubclass(
                            get_class_from_name(step_row["type"]), LoopOutputStep
                        ):
                            print(
                                f"Step {step_row['name']} from id {step_row['id']} will be added soon (2)"
                            )
                            add_step_ids.add(step_row["id"])
            print(
                f"populate_workflow: (1) Step {step.name} caricato nel wf {new_workflow.name}"
            )
            new_workflow.add_step(step)
        else:
            print(
                f"Step {step.name} non viene essere caricato perché nel wf {new_workflow.name} mancano le ports {set(step.input_ports.values()) - set(new_workflow.ports.keys())}"
            )
            pass
    print("Step caricati")

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

    if replace_step:  # se c'è un LoopOutputStep non serve fare sto casino
        port_name = list(replace_step.input_ports.values()).pop()
        ll_cond_step = CWLRecoveryLoopConditionalStep(
            replace_step.name
            if isinstance(replace_step, CWLRecoveryLoopConditionalStep)
            else replace_step.name + "-recovery",
            new_workflow,
            get_recovery_loop_expression(
                len(wr.port_tokens[port_name])
                # 1
                # if len(wr.port_tokens[port_name]) == 1
                # else len(wr.port_tokens[port_name]) - 1
            ),
            full_js=True,
        )
        print(
            f"Step {ll_cond_step.name} (wf {new_workflow.name}) setted with expression: {ll_cond_step.expression}"
        )
        for k_port, port in replace_step.get_input_ports().items():
            ll_cond_step.add_input_port(k_port, port)
        for k_port, port in replace_step.get_output_ports().items():
            ll_cond_step.add_output_port(k_port, port)
        for k_port, port in replace_step.get_skip_ports().items():
            if port.name in wr.port_name_ids.keys():
                ll_cond_step.add_skip_port(k_port, port)
            else:
                print(f"wf {new_workflow.name} pop skip-port {port.name}")
                new_workflow.ports.pop(port.name)
        new_workflow.steps.pop(replace_step.name)

        print(
            f"populate_workflow: (2) Step {ll_cond_step.name} caricato nel wf {new_workflow.name}"
        )
        new_workflow.add_step(ll_cond_step)
        print(
            f"populate_workflow: Rimuovo lo step {replace_step.name} dal wf {new_workflow.name} perché lo rimpiazzo con il nuovo step {ll_cond_step.name}"
        )

    is_there_loop_terminator = False

    for step in new_workflow.steps.values():
        # aggiungere condizione che lo step sia del loop più esterno.
        # la condizione potrebbe essere l'assenza del relativo LoopOutputStep
        # edit. Quando itero aggiungo loopoutputstep, quindi non è un discriminante
        if isinstance(step, BackPropagationTransformer):
            # for port_name in step.output_ports.values(): # potrebbe sostituire questo for
            for port_dep_row in await new_workflow.context.database.get_output_ports(
                step_name_id[step.name]
            ):
                a = port_dep_row["name"]
                b = step.output_ports[port_dep_row["name"]]
                c = wr.port_tokens[step.output_ports[port_dep_row["name"]]]
                if len(wr.port_tokens[step.output_ports[port_dep_row["name"]]]) > 1:
                    for (
                        step_dep_row
                    ) in await new_workflow.context.database.get_steps_from_output_port(
                        port_dep_row["port"]
                    ):
                        step_row = await new_workflow.context.database.get_step(
                            step_dep_row["step"]
                        )
                        if issubclass(
                            get_class_from_name(step_row["type"]), CombinatorStep
                        ):
                            combinator_row = json.loads(step_row["params"])[
                                "combinator"
                            ]
                            if issubclass(
                                get_class_from_name(combinator_row["type"]),
                                LoopTerminationCombinator,
                            ):
                                print(
                                    f"Step {step_row['name']} from id {step_row['id']} will be added soon (1)"
                                )
                                add_step_ids.add(step_row["id"])
                                is_there_loop_terminator = True
                else:
                    pass

    if not is_there_loop_terminator:
        rm_steps = set()
        for step in new_workflow.steps.values():
            if isinstance(step, BackPropagationTransformer):
                rm_steps.add(step.name)
        for sname in rm_steps:
            print(
                f"populate_workflow: Rimozione (1) definitiva step {sname} dal new_workflow {new_workflow.name}"
            )
            new_workflow.steps.pop(sname)

    added_ports = []
    for sid in add_step_ids:
        for port_dep_row in await new_workflow.context.database.get_input_ports(sid):
            port = await Port.load(
                new_workflow.context,
                port_dep_row["port"],
                loading_context,
                new_workflow,
            )
            if port.name not in new_workflow.ports.keys():
                print(
                    f"populate_workflow: wf {new_workflow.name} add_4 port {port.name}"
                )
                new_workflow.add_port(port)
                added_ports.append(port)
        ss = await Step.load(
            new_workflow.context,
            sid,
            loading_context,
            new_workflow,
        )
        print(
            f"populate_workflow: (3) Step {ss.name} (from step id {sid}) caricato nel wf {new_workflow.name}"
        )
        new_workflow.add_step(ss)

    for ss in new_workflow.steps.values():
        for p in added_ports:
            if not (a := p.get_input_steps()):
                for k, val in ss.input_ports.items():
                    if p.name == val:
                        ss.input_ports.pop(k)
                        if isinstance(ss, CombinatorStep):
                            ss.combinator.items.remove(k)
                        new_workflow.ports.pop(val)
                        # ss.input_ports[k] = ss.input_ports["file"]
                        break

    rm_steps = set()
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
