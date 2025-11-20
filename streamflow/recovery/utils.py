from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Iterable, MutableMapping, MutableSequence, MutableSet
from typing import Any

from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import FailureHandlingException
from streamflow.core.recovery import TokenAvailability
from streamflow.core.utils import contains_persistent_id, get_class_from_name
from streamflow.core.workflow import Token
from streamflow.log_handler import logger
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.persistence.utils import load_dependee_tokens
from streamflow.workflow.step import ExecuteStep, ScheduleStep, TransferStep
from streamflow.workflow.token import JobToken


async def create_graph_mapper(
    context: StreamFlowContext, provenance: ProvenanceGraph
) -> GraphMapper:
    mapper = GraphMapper(context)
    queue = deque(provenance.dag_tokens.prev(DirectGraph.LEAF))
    visited = set()
    while queue:
        token_id = queue.popleft()
        visited.add(token_id)
        for prev_token_id in provenance.dag_tokens.prev(token_id):
            if prev_token_id not in visited and prev_token_id not in queue:
                queue.append(prev_token_id)
            mapper.add(
                provenance.info_tokens.get(prev_token_id, None),
                provenance.info_tokens.get(token_id, None),
            )
    return mapper


class DirectGraph:
    ROOT = "root"
    LEAF = "leaf"

    def __init__(self, name: str):
        """
        A directed graph which maintains a single connected structure that always starts from ROOT
        and terminates at LEAF.
        ROOT is a special node that serves as the single entry point of the graph.
        LEAF is special node that serves as the single exit point of the graph.
        """
        self.graph: MutableMapping[Any, MutableSet[Any]] = {
            DirectGraph.ROOT: {DirectGraph.LEAF},
            DirectGraph.LEAF: set(),
        }
        self.name: str = name

    def add(self, src: Any | None, dst: Any | None) -> None:
        src = src or DirectGraph.ROOT
        dst = dst or DirectGraph.LEAF
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"{self.name} Graph: Added {src} -> {dst}")
        # The node is a root
        if src not in self.graph.keys():
            self.graph[DirectGraph.ROOT].add(src)
        # The node is no more a leaf
        elif DirectGraph.LEAF in self.graph[src]:
            self.graph[src].remove(DirectGraph.LEAF)
        # Add the edge
        self.graph.setdefault(src, set()).add(dst)
        # The node is a leaf
        if dst not in self.graph.keys():
            self.graph.setdefault(dst, set()).add(DirectGraph.LEAF)
        # The node is no more a root node
        if src != DirectGraph.ROOT and dst in self.graph[DirectGraph.ROOT]:
            self.graph[DirectGraph.ROOT].remove(dst)
        # Remove the leaf from the root when the first node is added
        if (
            src != DirectGraph.ROOT or dst != DirectGraph.LEAF
        ) and DirectGraph.LEAF in self.graph[DirectGraph.ROOT]:
            self.graph[DirectGraph.ROOT].remove(DirectGraph.LEAF)
        if not self.graph[DirectGraph.ROOT]:  # todo: debug code. remove me.
            raise FailureHandlingException("root empty")

    def empty(self) -> bool:
        return False if self.graph else True

    def items(self):
        return self.graph.items()

    def keys(self):
        return self.graph.keys()

    def move_to_root(self, vertex: Any) -> MutableSequence[Any]:
        """
        Move a vertex to be a direct successor of ROOT. This implies that all the edges with its
        previous vertices are deleted. All vertices on the path from ROOT to the vertex that have
        no other successors are deleted. Returns list of all deleted vertices.
        """
        if vertex in (DirectGraph.ROOT, DirectGraph.LEAF):
            raise FailureHandlingException(f"Impossible to move {vertex}")

        deleted = []
        to_delete = []
        # Remove the vertex from all its current predecessors
        for node, values in list(self.graph.items()):
            if vertex in values:
                values.discard(vertex)
                if len(values) == 0 and node != DirectGraph.ROOT:
                    to_delete.append(node)
        self.graph[DirectGraph.ROOT].add(vertex)
        for node in to_delete:
            if node not in deleted:
                deleted.extend(self.remove(node))
        return deleted

    def prev(self, vertex: Any) -> MutableSet[Any]:
        """Return the previous nodes of the vertex."""
        return {v for v, next_vs in self.graph.items() if vertex in next_vs}

    def remove(self, vertex: Any) -> MutableSequence[Any]:
        """
        Remove a vertex from the graph.
        When a node is removed, all nodes that become unreachable (from the root)
        are also removed. Nodes without successors are automatically connected to LEAF.
        Nodes without predecessors are automatically connected from ROOT.
        Returns list of all removed vertices.
        """
        if vertex in (DirectGraph.ROOT, DirectGraph.LEAF):
            raise FailureHandlingException(
                f"Impossible to remove {vertex} from the graph"
            )
        removed = []
        to_remove = [vertex]
        while to_remove:
            # Skip if already removed
            if (current := to_remove.pop(0)) in removed:
                continue
            if current in (DirectGraph.ROOT, DirectGraph.LEAF):
                raise FailureHandlingException(
                    f"Impossible to remove {vertex} from the graph"
                )
            logger.info(f"dg removing {current}")
            self.graph.pop(current)
            removed.append(current)
            # Find and remove references to current node, and identify dead-end nodes
            dead_end_nodes = set()
            for node, values in list(self.graph.items()):
                if current in values:
                    values.remove(current)
                if (
                    len(values) == 0
                    and node not in (DirectGraph.ROOT, DirectGraph.LEAF)
                    and dead_end_nodes not in to_remove
                    and dead_end_nodes not in removed
                ):
                    dead_end_nodes.add(node)
            to_remove.extend(dead_end_nodes)

        # Fix graph structure
        for node in self.graph.keys():
            # No successors
            if len(self.graph[node]) == 0:
                self.graph[node].add(DirectGraph.LEAF)
            # No predecessors
            if node != DirectGraph.ROOT:
                if not any(node in values for values in self.graph.values()):
                    self.graph[DirectGraph.ROOT].add(node)
        return removed

    def replace(self, old_vertex: Any, new_vertex: Any) -> None:
        for values in self.graph.values():
            if old_vertex in values:
                values.remove(old_vertex)
                values.add(new_vertex)
        if old_vertex in self.graph.keys():
            self.graph[new_vertex] = self.graph.pop(old_vertex)

    def succ(self, vertex: Any) -> MutableSet[Any]:
        """Return the next nodes of the vertex. A new instance of the list is created"""
        return {t for t in self.graph.get(vertex, [])}

    def values(self):
        return self.graph.values()

    def __getitem__(self, name):
        return self.graph[name]

    def __iter__(self):
        return iter(self.graph)

    def __str__(self) -> str:
        return (
            "{\n"
            + "\n".join(
                [
                    f"{k} : {[str(v) for v in values]}"
                    for k, values in self.graph.items()
                ]
            )
            + "\n}\n"
        )


class GraphMapper:
    def __init__(self, context: StreamFlowContext):
        self.dcg_port: DirectGraph = DirectGraph("Dependencies")
        self.dag_tokens: DirectGraph = DirectGraph("Provenance")
        # port name : port ids
        self.port_name_ids: MutableMapping[str, MutableSet[int]] = {}
        # port name : token ids
        self.port_tokens: MutableMapping[str, MutableSet[int]] = {}
        self.token_available: MutableMapping[int, bool] = {}
        # token id : token instance
        self.token_instances: MutableMapping[int, Token] = {}
        self.context: StreamFlowContext = context

    def _update_token(self, port_name: str, token: Token, is_available: bool) -> int:
        # Check if there is the same token in the port
        if equal_token_id := self.get_equal_token(port_name, token):
            # Check if the token is newer or available
            if self.token_available[equal_token_id]:
                return equal_token_id
            elif is_available:
                self.replace_token(port_name, token, is_available)
                self.move_token_to_root(token.persistent_id)
                # self.remove_token(token.persistent_id, preserve_token=True)
                return token.persistent_id
            else:
                return equal_token_id
        else:
            # Add port and token relation
            if port_name not in (DirectGraph.ROOT, DirectGraph.LEAF):
                self.port_tokens.setdefault(port_name, set()).add(token.persistent_id)
            self.token_instances[token.persistent_id] = token
            self.token_available[token.persistent_id] = is_available
            return token.persistent_id

    async def get_schedule_port_name(self, job_token: JobToken) -> str:
        port_name = next(
            port
            for port, token_ids in self.port_tokens.items()
            if job_token.persistent_id in token_ids
        )
        # Get newest port
        port_id = max(self.port_name_ids[port_name])
        step_rows = await self.context.database.get_input_steps(port_id)
        step_rows = await asyncio.gather(
            *(
                asyncio.create_task(self.context.database.get_step(row["step"]))
                for row in step_rows
            )
        )
        if len(step_rows) != 1:
            raise FailureHandlingException(
                f"Job {job_token.value.name} with token {job_token.persistent_id} has multiple steps"
            )
        if not issubclass(get_class_from_name(step_rows[0]["type"]), ScheduleStep):
            raise FailureHandlingException(
                f"Job {job_token.value.name} with token {job_token.persistent_id} must have a schedule step. Got {step_rows[0]['type']}"
            )
        if port_name in (DirectGraph.ROOT, DirectGraph.LEAF):
            raise FailureHandlingException(f"Impossible to get port: {port_name}")
        return port_name

    def add(
        self, token_info_a: ProvenanceToken | None, token_info_b: ProvenanceToken | None
    ) -> None:
        # Add ports into the dependency graph
        if token_info_a:
            port_name_a = token_info_a.port_name
            self.port_name_ids.setdefault(port_name_a, set()).add(token_info_a.port_id)
        else:
            port_name_a = None
        if token_info_b:
            port_name_b = token_info_b.port_name
            self.port_name_ids.setdefault(port_name_b, set()).add(token_info_b.port_id)
        else:
            port_name_b = None
        self.dcg_port.add(port_name_a, port_name_b)

        # Add (or update) tokens into the provenance graph
        token_a_id = (
            self._update_token(
                port_name_a,
                token_info_a.instance,
                token_info_a.is_available,
            )
            if token_info_a
            else None
        )
        token_b_id = (
            self._update_token(
                port_name_b,
                token_info_b.instance,
                token_info_b.is_available,
            )
            if token_info_b
            else None
        )
        self.dag_tokens.add(token_a_id, token_b_id)

    def get_equal_token(self, port_name: str, token: Token) -> int | None:
        """
        This method returns the token id of the corresponding Token present inside the port.
        Two tokens are equal if there are in the same port and having same tag.
        Special case are the JobToken, in this case there are equals if their jobs have the same name
        """
        for token_id in self.port_tokens.get(port_name, []):
            if isinstance(token, JobToken):
                if self.token_instances[token_id].value.name == token.value.name:
                    return token_id
            elif self.token_instances[token_id].tag == token.tag:
                return token_id
        return None

    async def get_output_tokens(self, job_token_id: int) -> Iterable[int]:
        execute_step_out_token_ids = set()
        for token_id in [
            t
            for t in self.dag_tokens.succ(job_token_id)
            if t not in (DirectGraph.ROOT, DirectGraph.LEAF)
        ]:
            port_row = await self.context.database.get_port_from_token(token_id)
            for step_id_row in await self.context.database.get_input_steps(
                port_row["id"]
            ):
                step_row = await self.context.database.get_step(step_id_row["step"])
                if issubclass(get_class_from_name(step_row["type"]), ExecuteStep):
                    execute_step_out_token_ids.add(token_id)
        return execute_step_out_token_ids

    async def get_output_ports(self, job_token: JobToken) -> MutableSequence[str]:
        port_names = set()
        try:
            p = next(
                port
                for port, token_ids in self.port_tokens.items()
                if job_token.persistent_id in token_ids
            )
        except StopIteration as e:
            raise FailureHandlingException(e)
        for port_name in self.dcg_port.succ(p):
            if port_name in (DirectGraph.ROOT, DirectGraph.LEAF):
                continue
            # Get newest port
            port_id = max(self.port_name_ids[port_name])
            step_rows = await self.context.database.get_input_steps(port_id)
            for step_row in await asyncio.gather(
                *(
                    asyncio.create_task(self.context.database.get_step(row["step"]))
                    for row in step_rows
                )
            ):
                if issubclass(
                    get_class_from_name(step_row["type"]), (ExecuteStep, TransferStep)
                ):
                    port_names.add(port_name)
        return list(port_names)

    async def get_port_and_step_ids(
        self, output_port_names: Iterable[str]
    ) -> MutableSet[int]:
        port_ids = {
            min(self.port_name_ids[port_name])
            for port_name in self.port_tokens.keys()
            if port_name not in output_port_names
        }
        step_ids = {
            dependency_row["step"]
            for dependency_rows in await asyncio.gather(
                *(
                    asyncio.create_task(self.context.database.get_input_steps(port_id))
                    for port_id in port_ids
                )
            )
            for dependency_row in dependency_rows
        }
        # Remove steps with missing input ports
        # A port may have multiple input steps, so it is important to load only the necessary steps.
        step_to_remove = set()
        for step_id, dependency_rows in zip(
            step_ids,
            await asyncio.gather(
                *(
                    asyncio.create_task(self.context.database.get_input_ports(step_id))
                    for step_id in step_ids
                )
            ),
        ):
            for port_row in await asyncio.gather(
                *(
                    asyncio.create_task(
                        self.context.database.get_port(row_dependency["port"])
                    )
                    for row_dependency in dependency_rows
                )
            ):
                if port_row["name"] not in self.port_tokens.keys():
                    step_to_remove.add(step_id)
        for step_id in step_to_remove:
            if logger.isEnabledFor(logging.DEBUG):
                step_name = (await self.context.database.get_step(step_id))["name"]
                logger.debug(f"Removing step {step_name}")
            step_ids.remove(step_id)
        return step_ids

    def remove_port(self, port_name: str) -> MutableSequence[str]:
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"Removing port {port_name}")
        orphan_tokens = set()
        removed_ports = self.dcg_port.remove(port_name)
        for next_port_name in removed_ports:
            if next_port_name != port_name and logger.isEnabledFor(logging.INFO):
                logger.info(
                    f"Removed port {next_port_name} by deleting port {port_name}"
                )
            for token_id in self.port_tokens.pop(next_port_name, []):
                orphan_tokens.add(token_id)
            self.port_name_ids.pop(next_port_name, None)
        for token_id in orphan_tokens:
            self.remove_token(token_id)
        return removed_ports

    def move_token_to_root(self, token_id: int) -> None:
        empty_ports = set()
        for removed_token_id in self.dag_tokens.move_to_root(token_id):
            if logger.isEnabledFor(logging.INFO):
                logger.info(
                    f"Removed token {removed_token_id} caused by moving {token_id} to root"
                )
            self.token_available.pop(removed_token_id, None)
            self.token_instances.pop(removed_token_id, None)
            # Remove ports
            for port_name, token_list in self.port_tokens.items():
                if removed_token_id in token_list:
                    self.port_tokens[port_name].remove(removed_token_id)
                if len(self.port_tokens[port_name]) == 0:
                    empty_ports.add(port_name)
        removed_ports = []
        for port_name in empty_ports:
            if port_name not in removed_ports:
                removed_ports.extend(self.remove_port(port_name))

    def remove_token(self, token_id: int) -> None:
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"Removing token {token_id}")
        removed = [token_id, *self.dag_tokens.remove(token_id)]
        for removed_token_id in removed:
            if removed_token_id != token_id and logger.isEnabledFor(logging.INFO):
                logger.info(
                    f"Removed token with id {removed_token_id} by deleting token with id {token_id}"
                )
            self.token_available.pop(token_id, None)
            self.token_instances.pop(token_id, None)
            # Remove ports
            empty_ports = set()
            for port_name, token_list in self.port_tokens.items():
                if token_id in token_list:
                    self.port_tokens[port_name].remove(token_id)
                if len(self.port_tokens[port_name]) == 0:
                    empty_ports.add(port_name)
            for port_name in empty_ports:
                self.remove_port(port_name)

    def replace_token(self, port_name: str, token: Token, is_available: bool) -> None:
        if (old_token_id := self.get_equal_token(port_name, token)) is None:
            raise FailureHandlingException(
                f"Unable to find a token for replacement with {token.persistent_id}."
            )
        if old_token_id == token.persistent_id:
            if self.token_available[old_token_id] != is_available:
                raise FailureHandlingException(
                    f"Availability mismatch for token {old_token_id}. "
                    f"Expected: {self.token_available[old_token_id]}, Got: {is_available}."
                )
            elif logger.isEnabledFor(logging.INFO):
                logger.info(
                    f"Token {old_token_id} is already in desired state. Skipping replacement."
                )
            return
        elif logger.isEnabledFor(logging.INFO):
            logger.info(f"Replacing {old_token_id} with {token.persistent_id}")
        # Replace
        self.dag_tokens.replace(old_token_id, token.persistent_id)
        # Remove old token
        self.port_tokens[port_name].remove(old_token_id)
        self.token_available.pop(old_token_id)
        self.token_instances.pop(old_token_id)
        # Add new token
        self.port_tokens.setdefault(port_name, set()).add(token.persistent_id)
        self.token_instances[token.persistent_id] = token
        self.token_available[token.persistent_id] = is_available
        # Remove previous dependencies
        self.move_token_to_root(token.persistent_id)


def token_to_str(k, g):
    return (
        f"{k}\n"
        f"{g.info_tokens[k].instance.tag if k in g.info_tokens else ''}\n"
        f"{g.info_tokens[k].is_available if k in g.info_tokens else ''}\n"
        f"{g.info_tokens[k].port_name if k in g.info_tokens else ''}"
    )


class ProvenanceGraph:
    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context
        self.dag_tokens: DirectGraph = DirectGraph("Provenance")
        self.info_tokens: MutableMapping[int, ProvenanceToken] = {}

    def add(self, src_token: Token | None, dst_token: Token | None) -> None:
        self.dag_tokens.add(
            src_token.persistent_id if src_token is not None else src_token,
            dst_token.persistent_id if dst_token is not None else dst_token,
        )

    async def build_graph(self, inputs: Iterable[Token]) -> None:
        """
        The provenance graph represents the execution and is always a DAG.
        To traverse the provenance graph, a breadth-first search is performed
        starting from the input tokens and moving backward. At the end of the search,
        we obtain a tree where the root node represents the tokens whose data are available
        in a specific location, and the leaves correspond to the input tokens.
        """
        token_frontier = deque(inputs)
        loading_context = DefaultDatabaseLoadingContext()
        for t in token_frontier:
            self.add(t, None)

        while token_frontier:
            token = token_frontier.popleft()
            port_row = await self.context.database.get_port_from_token(
                token.persistent_id
            )
            # The token is a `JobToken` and its job is running on another recovered workflow
            if (
                isinstance(token, JobToken)
                and (await self.context.failure_manager.is_recovered(token.value.name))
                == TokenAvailability.FutureAvailable
            ):
                is_available = False
                self.add(None, token)
            elif is_available := await token.is_available(context=self.context):
                self.add(None, token)
            else:
                # Token is not available, get previous tokens
                if prev_tokens := await load_dependee_tokens(
                    token.persistent_id, self.context, loading_context
                ):
                    for prev_token in prev_tokens:
                        self.add(prev_token, token)
                        # Check if the token has already been visited
                        if (
                            prev_token.persistent_id not in self.info_tokens.keys()
                            and not contains_persistent_id(
                                prev_token.persistent_id, token_frontier
                            )
                        ):
                            token_frontier.append(prev_token)
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(
                            f"Token with id {token.persistent_id} is not available, "
                            f"its previous tokens are {[t.persistent_id for t in prev_tokens]}"
                        )
                else:
                    raise FailureHandlingException(
                        f"Token with id {token.persistent_id} is not available and it does not have previous tokens"
                    )
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"Token id {token.persistent_id} is {'' if is_available else 'not '}available"
                )
            self.info_tokens.setdefault(
                token.persistent_id,
                ProvenanceToken(
                    instance=token,
                    is_available=is_available,
                    port_id=port_row["id"],
                    port_name=port_row["name"],
                ),
            )


class ProvenanceToken:
    __slots__ = ("instance", "is_available", "port_id", "port_name")

    def __init__(
        self,
        instance: Token,
        is_available: bool,
        port_id: int,
        port_name: str,
    ):
        self.instance: Token = instance
        self.is_available: bool = is_available
        self.port_id: int = port_id
        self.port_name: str = port_name
