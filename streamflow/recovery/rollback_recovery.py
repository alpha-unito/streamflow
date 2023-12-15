import asyncio
import json
from collections import deque
from enum import Enum
from typing import MutableMapping, MutableSet, Any

from streamflow.core.utils import (
    get_class_from_name,
    contains_id,
    get_tag,
    get_class_fullname,
)
from streamflow.core.exception import FailureHandlingException
from streamflow.cwl.token import CWLFileToken

from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.core.workflow import Token
from streamflow.cwl.transformer import (
    BackPropagationTransformer,
)
from streamflow.log_handler import logger
from streamflow.recovery.utils import (
    get_steps_from_output_port,
    _is_token_available,
    get_key_by_value,
)
from streamflow.workflow.port import ConnectorPort
from streamflow.workflow.step import (
    InputInjectorStep,
    ExecuteStep,
    DeployStep,
)
from streamflow.workflow.token import JobToken


class TokenAvailability(Enum):
    Unavailable = 0
    Available = 1
    FutureAvailable = 2


class ProvenanceToken:
    def __init__(self, token_instance, is_available, port_row, step_rows):
        self.instance = token_instance
        self.is_available = is_available
        self.port_row = port_row
        self.step_rows = step_rows


async def get_execute_step_out_token_ids(next_token_ids, context):
    execute_step_out_token_ids = set()
    for t_id in next_token_ids:
        if t_id > 0:
            port_row = await context.database.get_port_from_token(t_id)
            for step_id_row in await context.database.get_steps_from_output_port(
                port_row["id"]
            ):
                step_row = await context.database.get_step(step_id_row["step"])
                if step_row["type"] == get_class_fullname(ExecuteStep):
                    execute_step_out_token_ids.add(t_id)
        elif t_id == -1:
            return [t_id]
        else:
            raise Exception(f"Token {t_id} not valid")
    return execute_step_out_token_ids


async def evaluate_token_availability(token, step_rows, context):
    if await _is_token_available(token, context):
        is_available = TokenAvailability.Available
    else:
        is_available = TokenAvailability.Unavailable
    for step_row in step_rows:
        port_row = await context.database.get_port_from_token(token.persistent_id)
        if issubclass(get_class_from_name(port_row["type"]), ConnectorPort):
            return TokenAvailability.Unavailable

        if issubclass(
            get_class_from_name(step_row["type"]),
            (
                ExecuteStep,
                InputInjectorStep,
                BackPropagationTransformer,
                DeployStep,
            ),
        ):
            logger.debug(
                f"evaluate_token_availability: Token {token.persistent_id} from Unavailable port. Checking {step_row['name']} step type: {step_row['type']}"
            )
            return is_available
    return TokenAvailability.Unavailable


def is_there_step_type(rows, types):
    for step_row in rows:
        if issubclass(get_class_from_name(step_row["type"]), types):
            return True
    return False


class DirectGraph:
    INIT_GRAPH_FLAG = "init"
    LAST_GRAPH_FLAG = "last"

    def __init__(self):
        self.graph = {}

    def add(self, src, dst):
        if src is None:
            src = DirectGraph.INIT_GRAPH_FLAG
        if dst is None:
            dst = DirectGraph.LAST_GRAPH_FLAG
        logger.debug(f"DG: Added {src} -> {dst}")
        self.graph.setdefault(src, set()).add(dst)
        # if DirectGraph.LAST_GRAPH_FLAG in self.graph[src] and len(self.graph[src]) > 1:
        #     self.graph[src].remove(DirectGraph.LAST_GRAPH_FLAG)
        #     logger.debug(
        #         f"DG: Remove {DirectGraph.LAST_GRAPH_FLAG} from {src} next elems"
        #     )

    def remove(self, vertex):
        self.graph.pop(vertex, None)
        removed = [vertex]
        vertices_without_next = set()
        for k, values in self.graph.items():
            if vertex in values:
                values.remove(vertex)
            if not values:
                vertices_without_next.add(k)
        for vert in vertices_without_next:
            removed.extend(self.remove(vert))

        to_mv = set()
        for k in self.keys():
            if k != DirectGraph.INIT_GRAPH_FLAG and not self.prev(k):
                to_mv.add(k)
        for k in to_mv:
            self.add(None, k)
        return removed

    def replace(self, old_vertex, new_vertex):
        for values in self.graph.values():
            if old_vertex in values:
                values.remove(old_vertex)
                values.add(new_vertex)
        if old_vertex in self.graph.keys():
            self.graph[new_vertex] = self.graph.pop(old_vertex)

    def succ(self, vertex):
        return set(t for t in self.graph.get(vertex, []))

    def prev(self, vertex):
        return set(v for v, next_vs in self.graph.items() if vertex in next_vs)

    def empty(self):
        return False if self.graph else True

    def __getitem__(self, name):
        return self.graph[name]

    def __iter__(self):
        return iter(self.graph)

    def keys(self):
        return self.graph.keys()

    def items(self):
        return self.graph.items()

    def values(self):
        return self.graph.values()

    def __str__(self):
        return f"{json.dumps({k :list(v) for k, v in self.graph.items()}, indent=2)}"


class RollbackDeterministicWorkflowPolicy:
    def __init__(self, context):
        # { port name : [ next port names ] }
        self.dcg_port = DirectGraph()

        # { token id : [ next token ids ] }
        self.dag_tokens = DirectGraph()

        # { port name : [ port ids ] }
        self.port_name_ids = {}

        # { port name : [ token ids ] }
        self.port_tokens = {}

        # { token id : is_available }
        self.token_available = {}

        self.token_instances = {}

        self.context = context

    def is_present(self, t_id):
        for ts in self.port_tokens.values():
            if t_id in ts:
                return True
        return False

    def add(self, port_name, token, is_av):
        self.port_tokens.setdefault(port_name, set())
        if token is None:
            logger.debug(f"add rdwp. Port {port_name} none token")
            return
        logger.debug(
            f"Add token id: {token.persistent_id} tag: {token.tag} is_av: {is_av} in port {port_name}"
        )
        for t_id in self.port_tokens[port_name]:
            if isinstance(token, JobToken):
                if self.token_instances[t_id].value.name == token.value.name:
                    if t_id < token.persistent_id:
                        self.port_tokens[port_name].remove(t_id)
                        if not self.is_present(t_id):
                            logger.debug(
                                f"Replace jobtoken name: {token.value.name} of id: {token.persistent_id} with id: {t_id} on port {port_name}"
                            )
                            self.token_available.pop(t_id, None)
                            self.token_instances.pop(t_id, None)
                        break
                    return False
            elif self.token_instances[t_id].tag == token.tag:
                if t_id < token.persistent_id:
                    self.port_tokens[port_name].remove(t_id)
                    if not self.is_present(t_id):
                        logger.debug(
                            f"Replace token tag: {token.tag} of id: {t_id} with id: {token.persistent_id} on port {port_name}"
                        )
                        self.token_available.pop(t_id, None)
                        self.token_instances.pop(t_id, None)
                    break
                return False
        self.port_tokens[port_name].add(token.persistent_id)
        self.token_available[token.persistent_id] = is_av
        self.token_instances[token.persistent_id] = token
        return True

    def _remove_port_names(self, port_names):
        orphan_tokens = set()  # probably a orphan token
        for port_name in port_names:
            logger.debug(f"Remove port {port_name}")
            for t_id in self.port_tokens.pop(port_name, []):
                orphan_tokens.add(t_id)
            self.port_name_ids.pop(port_name, None)
        for t_id in orphan_tokens:
            if not self.is_present(t_id):
                logger.debug(f"Remove orphan token {t_id}")
                self.token_available.pop(t_id, None)
                self.token_instances.pop(t_id, None)

    def remove(self, port_name):
        logger.debug(f"remove {port_name}")
        self._remove_port_names(self.dcg_port.remove(port_name))

    async def replace_token_and_remove(self, port_name, provenance_token):
        old_token_id = self.get_equal_token(port_name, provenance_token.instance)

        for prev_t_id in self.dag_tokens.prev(old_token_id):
            # self.remove_token_by_id(prev_t_id)
            await self.remove_token(prev_t_id)
            logger.debug(
                f"replace_token_and_remove: from {old_token_id} remove prev token {prev_t_id}"
            )
        self.port_tokens[port_name].remove(old_token_id)
        self.port_tokens[port_name].add(provenance_token.instance.persistent_id)
        self.token_available[provenance_token.instance.persistent_id] = (
            provenance_token.is_available == TokenAvailability.Available
        )
        self.token_instances[
            provenance_token.instance.persistent_id
        ] = provenance_token.instance
        self.dag_tokens.replace(old_token_id, provenance_token.instance.persistent_id)
        logger.debug(
            f"replace_token_and_remove: token id: {provenance_token.instance.persistent_id} av: {provenance_token.is_available == TokenAvailability.Available}"
        )
        pass

    async def get_execute_output_port_names(self, job_token: JobToken):
        port_names = set()
        for t_id in self.dag_tokens.succ(job_token.persistent_id):
            logger.debug(
                f"Token {t_id} è successivo al job_token {job_token.persistent_id}"
            )
            if t_id in (DirectGraph.INIT_GRAPH_FLAG, DirectGraph.LAST_GRAPH_FLAG):
                continue
            port_name = get_key_by_value(t_id, self.port_tokens)
            port_id = max(self.port_name_ids[port_name])

            step_rows = await self.context.database.get_steps_from_output_port(port_id)
            for step_row in await asyncio.gather(
                *(
                    asyncio.create_task(self.context.database.get_step(sr["step"]))
                    for sr in step_rows
                )
            ):
                if issubclass(get_class_from_name(step_row["type"]), ExecuteStep):
                    port_names.add(port_name)
        return list(port_names)

    async def get_execute_output_port_names_old(self, job_token):
        port_names = []
        job_port_name_a = get_key_by_value(job_token.persistent_id, self.port_tokens)
        job_port_name = (
            await self.context.database.get_port_from_token(job_token.persistent_id)
        )["name"]
        if job_port_name != job_port_name_a:
            pass
        logger.debug(
            f"JobToken id: {job_token.persistent_id} name: {job_token.value.name} è prodotto dalla porta {job_port_name}"
        )
        for port_name in self.dcg_port.succ(job_port_name):
            logger.debug(f"Port {port_name} è successiva alla port {job_port_name}")
            if port_name in (DirectGraph.INIT_GRAPH_FLAG, DirectGraph.LAST_GRAPH_FLAG):
                continue
            port_id = min(self.port_name_ids[port_name])
            step_rows = await self.context.database.get_steps_from_output_port(port_id)
            for step_row in await asyncio.gather(
                *(
                    asyncio.create_task(self.context.database.get_step(sr["step"]))
                    for sr in step_rows
                )
            ):
                if issubclass(get_class_from_name(step_row["type"]), ExecuteStep):
                    port_names.append(port_name)
        return port_names

    async def wrapper_get_execute_output_port_names(self, job_token):
        res_new = await self.get_execute_output_port_names(job_token)
        res_old = await self.get_execute_output_port_names_old(job_token)

        if set(res_new) != set(res_old):
            pass

        return res_new

    def remove_token_by_id(self, token_id_to_remove: int):
        logger.debug(
            f"remove_token_by_id: remove token id {token_id_to_remove} from visited"
        )
        ports_to_remove = set()
        for port_name, token_ids in self.port_tokens.items():
            if token_id_to_remove in token_ids:
                token_ids.remove(token_id_to_remove)
                logger.debug(
                    f"remove_token_by_id: remove token id {token_id_to_remove} from {port_name}"
                )
            if len(token_ids) == 0:
                ports_to_remove.add(port_name)
        for port_name in ports_to_remove:
            self.remove(port_name)
        self.token_available.pop(token_id_to_remove, None)
        self.token_instances.pop(token_id_to_remove, None)

    async def sync_running_jobs(self, workflow):
        logger.debug(f"INIZIO sync (wf {workflow.name}) RUNNING JOBS")
        map_job_port = {}
        job_token_names = [
            token.value.name
            for token in self.token_instances.values()
            if isinstance(token, JobToken)
        ]
        logger.debug(f"GT: {self.dag_tokens}\nGP: {self.dcg_port}")
        for job_token in [
            token
            for token in self.token_instances.values()
            if isinstance(token, JobToken)
        ]:
            job_request = await self.context.failure_manager.setup_job_request(
                job_token.value.name, default_is_running=False
            )
            async with job_request.lock:
                if job_request.is_running:
                    logger.debug(
                        f"Sync Job {job_token.value.name} (wf {workflow.name}): JobRequest is running on wf {job_request.workflow.name}. Other jobs: {job_token_names}"
                    )
                    output_port_names = await self.get_execute_output_port_names(
                        job_token
                    )
                    logger.debug(f"Lista output_port_names: {output_port_names}")
                    for output_port_name in output_port_names:
                        port_recovery = self.context.failure_manager.add_waiter(
                            job_token.value.name,
                            output_port_name,
                            map_job_port.get(job_token.value.name, None),
                        )
                        logger.debug(
                            f"Created port {port_recovery.port.name} for wf {workflow.name} with waiting token {port_recovery.waiting_token}"
                        )
                        map_job_port.setdefault(job_token.value.name, port_recovery)
                    if len(job_token_names) == 1:
                        logger.debug(f"New workflow {workflow.name} will be empty.")
                        pass
                        # raise FailureHandlingException(
                        #     "There is only job job in this rollback, but it is already running in another workflow. Something is wrong."
                        # )
                    execute_step_out_token_ids = await get_execute_step_out_token_ids(
                        # [
                        #     row["depender"]
                        #     for row in await self.context.database.get_dependers(
                        #         job_token.persistent_id
                        #     )
                        # ],
                        self.dag_tokens.succ(job_token.persistent_id),
                        self.context,
                    )
                    logger.debug(
                        f"Lista execute_step_out_token_ids: {execute_step_out_token_ids}"
                    )
                    for t_id in execute_step_out_token_ids:
                        for prev_t_id in self.dag_tokens.prev(t_id):
                            # consider the token t_id and get its prev tokens.
                            # If they have in next only t_id, then remove prev.
                            if (
                                (tokens := self.dag_tokens.succ(prev_t_id))
                                and len(tokens) == 1
                                and t_id in tokens
                            ):
                                logger.debug(
                                    f"Sync remove token {prev_t_id} (dependee of {t_id})"
                                )
                                # self.remove_token_by_id(prev_t_id)
                                await self.remove_token(prev_t_id)
                    pass
                elif job_request.token_output and all(
                    [
                        await _is_token_available(t, self.context)
                        for t in job_request.token_output.values()
                    ]
                ):
                    # search execute token after job token, replace this token with job_requ token. then remove all the prev tokens
                    logger.debug(
                        f"Sync Job {job_token.value.name} (wf {workflow.name}): JobRequest has token_output { { k : v.persistent_id for k, v in job_request.token_output.items()} }"
                    )
                    for port_name in await self.get_execute_output_port_names(
                        job_token
                    ):
                        new_token = job_request.token_output[port_name]
                        port_row = await self.context.database.get_port_from_token(
                            new_token.persistent_id
                        )
                        step_rows = await get_steps_from_output_port(
                            port_row["id"], self.context
                        )
                        logger.debug(
                            f"Sync Job {job_token.value.name} (wf {workflow.name}): Replace {self.get_equal_token(port_name, new_token)} with {new_token.persistent_id}"
                        )
                        await self.replace_token_and_remove(
                            port_name,
                            ProvenanceToken(
                                new_token,
                                TokenAvailability.Available,
                                port_row,
                                step_rows,
                            ),
                        )
                else:
                    logger.debug(
                        f"Sync Job {job_token.value.name} (wf {workflow.name}): JobRequest set to running and job_token and token_output to None."
                        f"\n\t- Prev value job_token: {job_request.job_token}\n\t- Prev value token_output: {job_request.token_output}"
                    )
                    job_request.is_running = True
                    job_request.job_token = None
                    job_request.token_output = None
                    job_request.workflow = workflow
                    await self.context.failure_manager.update_job_status(
                        job_token.value.name, job_request.lock
                    )
            # end lock
        # end for job token
        logger.debug(f"FINE sync (wf {workflow.name}) RUNNING JOBS")
        return map_job_port

    # rename it in get_equal_token_id ?
    def get_equal_token(self, port_name, token):
        for t_id in self.port_tokens.get(port_name, []):
            if isinstance(token, JobToken):
                if self.token_instances[t_id].value.name == token.value.name:
                    return t_id
            elif self.token_instances[t_id].tag == token.tag:
                return t_id
        return None

    def _update_token(self, port_name, token, is_av):
        if equal_token_id := self.get_equal_token(port_name, token):
            if equal_token_id >= token.persistent_id:
                logger.debug(
                    f"update_token: port {port_name} ricevuto in input t_id {token.persistent_id}. Pero' uso il token id {equal_token_id} che avevo già"
                )
                return equal_token_id
            self.port_tokens[port_name].remove(equal_token_id)
            self.token_instances.pop(equal_token_id)
            self.token_available.pop(equal_token_id)
            self.dag_tokens.replace(equal_token_id, token.persistent_id)
            logger.debug(
                f"update_token: Sostituisco t_id: {equal_token_id} con t_id: {token.persistent_id}"
            )
        if port_name:  # port name can be None (INIT or LAST)
            self.port_tokens.setdefault(port_name, set()).add(token.persistent_id)
        self.token_instances[token.persistent_id] = token
        self.token_available[token.persistent_id] = is_av
        logger.debug(
            f"update_token: port {port_name} ricevuto in input t_id {token.persistent_id}. E lo userò"
        )
        return token.persistent_id

    async def remove_token(self, token_id):
        if token_id == DirectGraph.INIT_GRAPH_FLAG:
            return
        logger.debug(f"remove_token: id {token_id}")
        prev_t_ids = self.dag_tokens.prev(token_id)
        self.dag_tokens.remove(token_id)
        self.remove_token_by_id(token_id)
        for prev_t_id in prev_t_ids:
            if prev_t_id in self.token_instances.keys() and isinstance(
                self.token_instances[prev_t_id], JobToken
            ):
                logger.debug(f"rimosso {token_id}. remove suo jobtoken {prev_t_id}")
                await self.remove_token(prev_t_id)
                prev_t_ids.remove(prev_t_id)
                break
        for prev_t_id in prev_t_ids:
            if not isinstance(prev_t_id, int):  # init
                continue
            token_row = await self.context.database.get_token(prev_t_id)
            port_row = await self.context.database.get_port_from_token(prev_t_id)
            if issubclass(get_class_from_name(port_row["type"]), ConnectorPort):
                logger.debug(f"Token {prev_t_id} is in a ConnectorPort. Not remove it")
                continue
            tokens = self.dag_tokens.succ(prev_t_id)
            if len(tokens) == 0:
                logger.debug(f"rimosso {token_id}. remove suo prev {prev_t_id}")
                await self.remove_token(prev_t_id)
            else:
                logger.debug(
                    f"rimosso {token_id}, suo prev token {prev_t_id} ha ancora come successivi {tokens}"
                )

    def new_add(self, token_info_a: ProvenanceToken, token_info_b: ProvenanceToken):
        port_name_a = token_info_a.port_row["name"] if token_info_a else None
        port_name_b = token_info_b.port_row["name"] if token_info_b else None

        self.dcg_port.add(port_name_a, port_name_b)

        token_a_id = (
            self._update_token(
                port_name_a,
                token_info_a.instance,
                token_info_a.is_available == TokenAvailability.Available,
            )
            if token_info_a
            else None
        )
        token_b_id = (
            self._update_token(
                port_name_b,
                token_info_b.instance,
                token_info_b.is_available == TokenAvailability.Available,
            )
            if token_info_b
            else None
        )
        token_a = self.token_instances.get(token_a_id, None)
        token_b = self.token_instances.get(token_b_id, None)

        # if not (
        #     next_token_ids_a := self.dag_tokens.graph.get(
        #         token_a.persistent_id if token_a else DirectGraph.INIT_GRAPH_FLAG, None
        #     )
        # ) or (token_b and token_b.persistent_id not in next_token_ids_a):
        #     logger.debug(
        #         f"new_add: {token_a.persistent_id if token_a else None} -> {token_b.persistent_id if token_b else None}"
        #     )
        #     self.dag_tokens.add(
        #         token_a.persistent_id if token_a else None,
        #         token_b.persistent_id if token_b else None,
        #     )
        # else:
        #     pass

        logger.debug(
            f"new_add: {token_a.persistent_id if token_a else None} -> {token_b.persistent_id if token_b else None}"
        )
        self.dag_tokens.add(
            token_a.persistent_id if token_a else None,
            token_b.persistent_id if token_b else None,
        )

        if port_name_a:
            self.port_name_ids.setdefault(port_name_a, set()).add(
                token_info_a.port_row["id"]
            )
        if port_name_b:
            self.port_name_ids.setdefault(port_name_b, set()).add(
                token_info_b.port_row["id"]
            )


class NewProvenanceGraphNavigation:
    def __init__(self, context):
        self.dag_tokens = DirectGraph()
        self.info_tokens: MutableMapping[int, ProvenanceToken] = {}
        self.context = context

    def add(self, src_token: Token | None, dst_token: Token | None):
        self.dag_tokens.add(
            src_token.persistent_id if src_token else src_token,
            dst_token.persistent_id if dst_token else dst_token,
        )

    async def build_unfold_graph(self, init_tokens):
        token_frontier = deque(init_tokens)
        loading_context = DefaultDatabaseLoadingContext()
        job_token = None
        for t in token_frontier:
            # if not isinstance(t, JobToken):
            #     self.add(t, None)
            self.add(t, None)
            if isinstance(t, JobToken):
                job_token = t
        for t in token_frontier:
            if not isinstance(t, JobToken):
                self.add(job_token, t)

        while token_frontier:
            token = token_frontier.popleft()
            port_row = await self.context.database.get_port_from_token(
                token.persistent_id
            )
            step_rows = await get_steps_from_output_port(port_row["id"], self.context)
            # questa condizione si potrebbe mettere come parametro e utilizzarla come taglio. Ovvero, dove l'utente vuole che si fermi la ricerca indietro
            if await self.context.failure_manager.is_running_token(token):
                self.add(None, token)
                is_available = TokenAvailability.Unavailable
                logger.debug(
                    f"new_build_dag: Token id: {token.persistent_id} is running. Added in INIT"
                )
            else:
                if (
                    is_available := await evaluate_token_availability(
                        token, step_rows, self.context
                    )
                ) == TokenAvailability.Available:
                    self.add(None, token)
                    logger.debug(
                        f"new_build_dag: Token id: {token.persistent_id} is available. Added in INIT"
                    )
                else:
                    if prev_tokens := await loading_context.load_prev_tokens(
                        self.context, token.persistent_id
                    ):
                        for prev_token in prev_tokens:
                            self.add(prev_token, token)
                            if (
                                prev_token.persistent_id not in self.info_tokens.keys()
                                and not contains_id(
                                    prev_token.persistent_id, token_frontier
                                )
                            ):
                                token_frontier.append(prev_token)
                        logger.debug(
                            f"new_build_dag: Token id: {token.persistent_id} is not available, it has {[t.persistent_id for t in prev_tokens]} prev tokens "
                        )
                    elif issubclass(
                        get_class_from_name(port_row["type"]), ConnectorPort
                    ):
                        logger.debug(
                            f"Token {token.persistent_id} is in a ConnectorPort"
                        )
                    else:
                        raise FailureHandlingException(
                            f"Token {token.persistent_id} is not available and it does not have prev tokens"
                        )
            self.info_tokens.setdefault(
                token.persistent_id,
                ProvenanceToken(token, is_available, port_row, step_rows),
            )
        for info in self.info_tokens.values():
            logger.debug(
                f"info\n\tid: {info.instance.persistent_id}\n\ttype: {type(info.instance)}\n\tvalue: {info.instance.value.name if isinstance(info.instance, JobToken) else info.instance.value['basename'] if isinstance(info.instance, CWLFileToken) else info.instance.value}"
                f"\n\tis_avai: {info.is_available}\n\tport: {dict(info.port_row)}\n\tsteps: {[ s['name'] for s in info.step_rows] }"
            )
        pass

    async def get_execute_token_ids(self, job_token):
        output_token_ids = set()
        for next_t_id in self.dag_tokens.succ(job_token.persistent_id):
            if not isinstance(next_t_id, int):
                continue
            step_rows = self.info_tokens[next_t_id].step_rows
            if is_there_step_type(step_rows, (ExecuteStep,)):
                output_token_ids.add(next_t_id)
        return output_token_ids

    async def _refold_graphs(self, output_ports):
        rdwp = RollbackDeterministicWorkflowPolicy(self.context)
        for t_id, next_t_ids in self.dag_tokens.items():
            logger.debug(f"dag[{t_id}] = {next_t_ids}")
        for t_id, next_t_ids in self.dag_tokens.items():
            for next_t_id in next_t_ids:
                rdwp.new_add(
                    self.info_tokens.get(t_id, None),
                    self.info_tokens.get(next_t_id, None),
                )

        for out_port in output_ports:
            rdwp.dcg_port.replace(DirectGraph.LAST_GRAPH_FLAG, out_port.name)
            rdwp.dcg_port.add(out_port.name, DirectGraph.LAST_GRAPH_FLAG)
            placeholder = Token(
                None,
                get_tag(
                    self.info_tokens[t].instance
                    for t in self.dag_tokens.prev(DirectGraph.LAST_GRAPH_FLAG)
                    if not isinstance(t, JobToken)
                ),
            )
            placeholder.persistent_id = -1
            rdwp.dag_tokens.replace(
                DirectGraph.LAST_GRAPH_FLAG, placeholder.persistent_id
            )
            rdwp.dag_tokens.add(placeholder.persistent_id, DirectGraph.LAST_GRAPH_FLAG)
            rdwp.token_instances[placeholder.persistent_id] = placeholder
            rdwp.token_available[placeholder.persistent_id] = False
            rdwp.port_name_ids.setdefault(out_port.name, set()).add(
                out_port.persistent_id
            )
            rdwp.port_tokens.setdefault(out_port.name, set()).add(
                placeholder.persistent_id
            )

        job_tokens = set()
        for t_id, next_t_ids in self.dag_tokens.items():
            if isinstance(t_id, int) and isinstance(
                self.info_tokens[t_id].instance, JobToken
            ):
                job_tokens.add(t_id)
            for n_t_id in next_t_ids:
                if isinstance(n_t_id, int) and isinstance(
                    self.info_tokens[n_t_id].instance, JobToken
                ):
                    job_tokens.add(n_t_id)
        job_togroup = []
        for jt_id in job_tokens:
            if (
                isinstance(self.info_tokens[jt_id].instance, JobToken)
                and self.info_tokens[jt_id].instance.value.name == "/togroup/0"
            ):
                job_togroup.append(self.info_tokens[jt_id].instance)
        if job_togroup:
            comparisons = []
            for jg in job_togroup:
                len_nav = len(self.dag_tokens[jg.persistent_id])
                len_rdwp = (
                    len(rdwp.dag_tokens[jg.persistent_id])
                    if jg.persistent_id in rdwp.dag_tokens.keys()
                    else None
                )
                comparisons.append(
                    {
                        "job_token_id": jg.persistent_id,
                        "added-in-rdwp?": jg.persistent_id
                        in rdwp.token_instances.keys(),
                        "len_nav": len_nav,
                        "len_rdwp": len_rdwp,
                    }
                )
            for elem in comparisons:
                if elem["added-in-rdwp?"] and (
                    elem["len_nav"] != elem["len_rdwp"]
                    or elem["len_nav"] != 4
                    or elem["len_rdwp"] != 4
                ):
                    pass
        return rdwp

    async def _refold_graphs_old(self, output_ports):
        rdwp = RollbackDeterministicWorkflowPolicy(self.context)
        for t_id, next_t_ids in self.dag_tokens.items():
            logger.debug(f"dag[{t_id}] = {next_t_ids}")
        for t_id, next_t_ids in self.dag_tokens.items():
            port_name = None
            if isinstance(t_id, int):
                port_row = self.info_tokens[t_id].port_row
                rdwp.port_name_ids.setdefault(port_row["name"], set()).add(
                    self.info_tokens[t_id].port_row["id"]
                )
                # todo introdurre futureavailable anche in RDWP (?)
                rdwp.add(
                    port_row["name"],
                    self.info_tokens[t_id].instance,
                    self.info_tokens[t_id].is_available == TokenAvailability.Available,
                )
                port_name = port_row["name"]
                # logger.debug(f"_refold_graphs: port {port_name} from token {t_id}")
            next_port_names = set()
            for next_t_id in next_t_ids:
                next_port_name = None
                if isinstance(next_t_id, int):
                    next_port_row = self.info_tokens[next_t_id].port_row
                    rdwp.port_name_ids.setdefault(next_port_row["name"], set()).add(
                        next_port_row["id"]
                    )
                    rdwp.add(
                        next_port_row["name"],
                        self.info_tokens[next_t_id].instance,
                        self.info_tokens[next_t_id].is_available
                        == TokenAvailability.Available,
                    )
                    next_port_name = next_port_row["name"]
                    # logger.debug(
                    #     f"_refold_graphs: (next) port {next_port_name} from token {next_t_id}"
                    # )
                if next_port_name is None:
                    # it is an input port of the failed step, so the next ports are the failed step output ports
                    # next_port_names = (p.name for p in output_ports)
                    # if len(next_t_ids) > 1:
                    #     raise Exception(
                    #         "Input tokens of failed step can not have next tokens"
                    #     )
                    for npn in (p.name for p in output_ports):
                        rdwp.dcg_port.add(port_name, npn)
                else:
                    next_port_names.add(next_port_name)
            for next_port_name in next_port_names:
                rdwp.dcg_port.add(port_name, next_port_name)
        for out in output_ports:
            rdwp.dcg_port.add(out.name, None)
            placeholder = Token(
                None,
                get_tag(
                    self.info_tokens[t].instance
                    for t in self.dag_tokens.prev(DirectGraph.LAST_GRAPH_FLAG)
                    if not isinstance(t, JobToken)
                ),
            )
            placeholder.persistent_id = -1
            rdwp.add(out.name, placeholder, False)
            logger.debug(
                f"_refold_graphs. added in port {out.name} placeholder token id: {placeholder.persistent_id} value: {placeholder.value} tag: {placeholder.tag}"
            )
            rdwp.port_name_ids.setdefault(out.name, set()).add(out.persistent_id)
        for k, v in rdwp.dcg_port.items():
            logger.debug(f"_refold_graphs: dcg port[{k}] = {v}")
        return rdwp

    async def refold_graphs(
        self, output_ports, split_last_iteration=True
    ) -> RollbackDeterministicWorkflowPolicy:
        inner_graph, outer_graph = None, None

        res = await self._refold_graphs(output_ports)
        res_old = await self._refold_graphs_old(output_ports)

        if res.dcg_port.graph != res_old.dcg_port.graph:
            pass
        return res
