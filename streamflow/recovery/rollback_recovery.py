from collections import deque
from enum import Enum
from typing import MutableMapping, MutableSet, Any

from streamflow.core.utils import (
    get_class_from_name,
    contains_id,
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
)
from streamflow.workflow.step import (
    InputInjectorStep,
    ExecuteStep,
    DeployStep,
)
from streamflow.workflow.token import JobToken


async def evaluate_token_availability(token, step_rows, context):
    if await _is_token_available(token, context):
        is_available = TokenAvailability.Available
    else:
        is_available = TokenAvailability.Unavailable
    for step_row in step_rows:
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
        self.graph.setdefault(src, set()).add(dst)

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

        for k in self.keys():
            if k != DirectGraph.INIT_GRAPH_FLAG and not self.prev(k):
                self.add(None, k)
        return removed

    def replace(self, vertex_a, vertex_b):
        next_vertices = self.graph.pop(vertex_a)
        for k, values in self.graph.items():
            if vertex_a in values:
                values.remove(vertex_a)
                values.add(vertex_b)
        for next_vertex in next_vertices:
            self.graph.setdefault(vertex_b, set()).add(next_vertex)

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


class RollbackDeterministicWorkflowPolicy:
    def __init__(self, context):
        # { port name : [ next port names ] }
        self.dcg_port = DirectGraph()

        # { port name : [ port ids ] }
        self.port_name_ids = {}

        # { port name : [ token ids ] }
        self.port_tokens = {}

        # { token id : is_available }
        self.token_available = {}

        self.context = context


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

    def remove(self, token):
        if isinstance(token, str):
            return
        for t in self.dag_tokens.remove(token.persistent_id):
            self.info_tokens.pop(t, None)

    def replace(self, present_token: Token, new_provenance_token: ProvenanceToken):
        self.dag_tokens.replace(
            present_token.persistent_id, new_provenance_token.instance.persistent_id
        )
        self.info_tokens.pop(present_token.persistent_id, None)
        self.info_tokens[
            new_provenance_token.instance.persistent_id
        ] = new_provenance_token

    def replace_and_remove(self, prev_token, new_provenance_token):
        self.replace(prev_token, new_provenance_token)
        for prev in self.dag_tokens.prev(new_provenance_token.instance.persistent_id):
            if prev in self.info_tokens.keys():
                self.remove(self.info_tokens[prev].instance)

    async def build_unfold_graph(self, init_tokens):
        token_frontier = deque(init_tokens)
        loading_context = DefaultDatabaseLoadingContext()
        for t in token_frontier:
            self.add(t, None)  # self.add(t, DirectGraph.LAST_GRAPH_FLAG)

        while token_frontier:
            token = token_frontier.popleft()
            port_row = await self.context.database.get_port_from_token(
                token.persistent_id
            )
            step_rows = await get_steps_from_output_port(port_row["id"], self.context)
            # questa condizione si potrebbe mettere come parametro e utilizzarla come taglio. Ovvero, dove l'utente vuole che si fermi la ricerca indietro
            if await self.context.failure_manager.is_running_token(token):
                self.add(None, token)
                is_available = TokenAvailability.FutureAvailable
                logger.debug(
                    f"new_build_dag: Token id {token.persistent_id} is running. Added in INIT"
                )
            else:
                if (
                    is_available := await evaluate_token_availability(
                        token, step_rows, self.context
                    )
                ) == TokenAvailability.Available:
                    self.add(None, token)
                    logger.debug(
                        f"new_build_dag: Token id {token.persistent_id} is available. Added in INIT"
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
                            f"new_build_dag: Token id {token.persistent_id} is not available, it has {[t.persistent_id for t in prev_tokens]} prev tokens "
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

    async def sync_running_jobs(self):
        logger.debug(f"INIZIO sync RUNNING JOBS")
        b = set(
            k
            for k, t in self.info_tokens.items()
            if isinstance(t.instance, JobToken)
            and t.is_available == TokenAvailability.Available
        )
        for job_token in [
            t_info.instance
            for t_info in self.info_tokens.values()
            if isinstance(t_info.instance, JobToken)
        ]:
            if (
                job_token.value.name
                not in self.context.failure_manager.job_requests.keys()
            ):
                continue
            job_request = self.context.failure_manager.job_requests[
                job_token.value.name
            ]
            async with job_request.lock:
                if job_request.is_running:
                    cp = DirectGraph()
                    for k, values in self.dag_tokens.items():
                        for v in values:
                            cp.add(k, v)
                    logger.debug(
                        f"Sync Job {job_token.value.name}: JobRequest is running"
                    )
                    for output_token_id in await self.get_execute_token_ids(job_token):
                        for prev in self.dag_tokens.prev(output_token_id):
                            self.remove(self.info_tokens[prev].instance)
                            logger.debug(
                                f"Sync Job {job_token.value.name}: From JobToken {job_token.persistent_id}, found {output_token_id} execute token and removed {prev} token"
                            )
                    pass
                elif job_request.token_output and all(
                    [
                        await _is_token_available(t, self.context)
                        for t in job_request.token_output.values()
                    ]
                ):
                    # search execute token after job token, replace this token with job_requ token. then remove all the prev tokens
                    logger.debug(
                        f"Sync Job {job_token.value.name}: JobRequest has token_output {job_request.token_output}"
                    )
                    for output_token_id in await self.get_execute_token_ids(job_token):
                        port_name = self.info_tokens[output_token_id].port_row["name"]
                        new_token = job_request.token_output[port_name]
                        port_row = await self.context.database.get_port_from_token(
                            new_token.persistent_id
                        )
                        step_rows = await get_steps_from_output_port(
                            port_row["id"], self.context
                        )
                        logger.debug(
                            f"Sync Job {job_token.value.name}: Replace {self.info_tokens[output_token_id].instance.persistent_id} with {new_token.persistent_id}"
                        )
                        self.replace_and_remove(
                            self.info_tokens[output_token_id].instance,
                            ProvenanceToken(
                                new_token,
                                TokenAvailability.Available,
                                port_row,
                                step_rows,
                            ),
                        )
                else:
                    logger.debug(
                        f"Sync Job {job_token.value.name}: JobRequest set to running while its job_token and token_output to None."
                        f"\n\t- Prev value job_token: {job_request.job_token}\n\t- Prev value token_output: {job_request.token_output}"
                    )
                    # job_request.is_running = True
                    # job_request.job_token = None
                    # job_request.token_output = None
                    # job_request.workflow = None
            logger.debug(
                f"DEVO sync CON JOB TOKEN {job_token.persistent_id} JOB {job_token.value.name}"
            )
        logger.debug(f"FINE sync RUNNING JOBS")

    async def _refold_graphs(self):
        rdwp = RollbackDeterministicWorkflowPolicy(self.context)
        for t_id, next_t_ids in self.dag_tokens.items():
            port_name = None
            if isinstance(t_id, int):
                port_row = self.info_tokens[t_id].port_row
                rdwp.port_name_ids.setdefault(port_row["name"], set()).add(
                    self.info_tokens[t_id].port_row["id"]
                )
                # todo aggiustare inserimento token. se due token hanno stesso tag, mettere token disponibile o più giovane
                # todo introdurre futureavailable anche in RDWP (?)
                rdwp.port_tokens.setdefault(port_row["name"], set()).add(t_id)
                rdwp.token_available.setdefault(
                    t_id,
                    self.info_tokens[t_id].is_available == TokenAvailability.Available,
                )
                port_name = port_row["name"]
            for next_t_id in next_t_ids:
                next_port_name = None
                if isinstance(next_t_id, int):
                    next_port_row = self.info_tokens[next_t_id].port_row
                    rdwp.port_name_ids.setdefault(next_port_row["name"], set()).add(
                        next_port_row["id"]
                    )
                    rdwp.port_tokens.setdefault(next_port_row["name"], set()).add(
                        next_t_id
                    )
                    rdwp.token_available.setdefault(
                        next_t_id,
                        self.info_tokens[next_t_id].is_available
                        == TokenAvailability.Available,
                    )
                    next_port_name = next_port_row["name"]
                rdwp.dcg_port.add(port_name, next_port_name)
        return rdwp

    async def refold_graphs(
        self, split_last_iteration=True
    ) -> RollbackDeterministicWorkflowPolicy:
        inner_graph, outer_graph = None, None

        return await self._refold_graphs()
