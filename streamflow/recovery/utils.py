from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Iterable, MutableMapping, MutableSequence, MutableSet
from typing import TypeVar

from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import FailureHandlingException
from streamflow.core.recovery import TokenAvailability
from streamflow.core.utils import contains_persistent_id, get_class_from_name
from streamflow.core.workflow import Token
from streamflow.log_handler import logger
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.persistence.utils import load_dependee_tokens
from streamflow.workflow.step import ExecuteStep, TransferStep
from streamflow.workflow.token import JobToken

T = TypeVar("T")


async def create_graph_mapper(
    context: StreamFlowContext, provenance: ProvenanceGraph
) -> GraphMapper:
    mapper = GraphMapper(context)
    queue = deque(provenance.dag_tokens.get_sinks())
    visited = set()
    while queue:
        token_id = queue.popleft()
        visited.add(token_id)
        mapper.add(provenance.info_tokens.get(token_id, None))
        for prev_token_id in provenance.dag_tokens.predecessors(token_id):
            if prev_token_id not in visited and prev_token_id not in queue:
                queue.append(prev_token_id)
            mapper.add(
                provenance.info_tokens.get(prev_token_id, None),
                provenance.info_tokens.get(token_id, None),
            )
    return mapper


class DirectedGraph:
    __slots__ = ("_successors", "_predecessors", "name")

    def __init__(self, name: str) -> None:
        self.name: str = name
        self._successors: MutableMapping[T, MutableSet[T]] = {}
        self._predecessors: MutableMapping[T, MutableSet[T]] = {}

    def _add_node(self, node: T) -> None:
        if node not in self._successors.keys():
            self._successors[node] = set()
            self._predecessors[node] = set()

    def add(self, u: T, v: T | None = None) -> None:
        self._add_node(u)
        if v is not None:
            self._add_node(v)
            self._successors[u].add(v)
            self._predecessors[v].add(u)

    def get_nodes(self) -> MutableSet[T]:
        return set(self._successors.keys())

    def in_degree(self) -> MutableMapping[T, int]:
        return {n: len(nodes) for n, nodes in self._predecessors.items()}

    def out_degree(self) -> MutableMapping[T, int]:
        return {n: len(nodes) for n, nodes in self._successors.items()}

    def predecessors(self, node: T) -> MutableSet[T]:
        return set(self._predecessors[node])

    def remove_nodes(
        self, nodes: MutableSequence[T], prune_dead_end: bool = True
    ) -> MutableSequence[T]:
        """
        Remove a node from the graph.

        When a node is removed, any parent nodes that are left with
        no other children are considered dead-end branches.
        If prune_dead_end is enabled, these nodes are deleted from
        the current node back to the source of the branch.
        """
        removed_nodes = []
        stack = list(nodes)
        while stack:
            if (current := stack.pop()) not in self._successors.keys():
                continue
            removed_nodes.append(current)
            for succ in self._successors[current]:
                self._predecessors[succ].discard(current)
            for pred in self._predecessors[current]:
                self._successors[pred].discard(current)
                # If a parent now has no successors, it is a dead-end
                if prune_dead_end and not (self._successors[pred].difference(stack)):
                    stack.append(pred)
            del self._successors[current]
            del self._predecessors[current]
        return removed_nodes

    def remove_node(self, node: T, prune_dead_end: bool = True) -> MutableSequence[T]:
        return self.remove_nodes([node], prune_dead_end=prune_dead_end)

    def replace(self, old_node: T, new_node: T) -> None:
        """
        Replace an existing node with a new node, preserving all edges.

        If old_node does not exist, the operation is ignored.
        If new_node already exists, a ValueError is raised to prevent
        unintentional merging of nodes.
        """
        if old_node not in self._successors.keys():
            return
        if new_node in self._successors.keys():
            raise ValueError(
                f"Cannot replace: node '{new_node}' already exists in `{self.name}` graph."
            )
        self._add_node(new_node)
        for succ in self._successors[old_node]:
            self._successors[new_node].add(succ)
            self._predecessors[succ].remove(old_node)
            self._predecessors[succ].add(new_node)
        for pred in self._predecessors[old_node]:
            self._predecessors[new_node].add(pred)
            self._successors[pred].remove(old_node)
            self._successors[pred].add(new_node)
        del self._successors[old_node]
        del self._predecessors[old_node]

    def successors(self, node: T) -> MutableSet[T]:
        return set(self._successors[node])

    def __str__(self) -> str:
        return (
            f"{self.name}: {{\n"
            + "\n".join(
                [
                    f"{k} : {[v for v in values]},"
                    for k, values in self._successors.items()
                ]
            )
            + "\n}\n"
        )


class DirectedAcyclicGraph(DirectedGraph):
    def get_sources(self) -> MutableSet[T]:
        return {n for n, nodes in self._predecessors.items() if len(nodes) == 0}

    def get_sinks(self) -> MutableSet[T]:
        return {n for n, nodes in self._successors.items() if len(nodes) == 0}

    def promote_to_source(self, node: T) -> MutableSequence[T]:
        """
        Move a node to be a source node.

        This implies that all the edges with its previous nodes are deleted.
        All nodes on the path from the sources to the target `node` which have no other
        successors are deleted.

        Returns: A list of all deleted vertices.
        """
        if node not in self._successors.keys():
            return []
        to_delete = []
        for pred in list(self._predecessors[node]):
            self._successors[pred].discard(node)
            self._predecessors[node].discard(pred)
            # If the parent now has no successors, it must be deleted
            if not self._successors[pred]:
                to_delete.append(pred)
        return self.remove_nodes(to_delete)


class GraphMapper:
    def __init__(self, context: StreamFlowContext):
        self.dcg_port: DirectedGraph = DirectedGraph("Dependencies")
        self.dag_tokens: DirectedAcyclicGraph = DirectedAcyclicGraph("Provenance")
        # port name : port ids
        self.port_name_ids: MutableMapping[str, MutableSet[int]] = {}
        # port name : token ids
        self.port_tokens: MutableMapping[str, MutableSet[int]] = {}
        self.token_available: MutableMapping[int, bool] = {}
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
                return token.persistent_id
            else:
                return equal_token_id
        else:
            # Add port and token relation
            self.port_tokens.setdefault(port_name, set()).add(token.persistent_id)
            self.token_instances[token.persistent_id] = token
            self.token_available[token.persistent_id] = is_available
            return token.persistent_id

    def add(
        self, token_info_a: ProvenanceToken, token_info_b: ProvenanceToken | None = None
    ) -> None:
        # Add ports into the dependency graph
        port_name_a = token_info_a.port_name
        self.port_name_ids.setdefault(port_name_a, set()).add(token_info_a.port_id)
        if token_info_b:
            port_name_b = token_info_b.port_name
            self.port_name_ids.setdefault(port_name_b, set()).add(token_info_b.port_id)
        else:
            port_name_b = None
        self.dcg_port.add(port_name_a, port_name_b)

        # Add (or update) tokens into the provenance graph
        token_a_id = self._update_token(
            port_name_a,
            token_info_a.instance,
            token_info_a.is_available,
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
        for token_id in self.dag_tokens.successors(job_token_id):
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
        for port_name in self.dcg_port.successors(
            next(
                port
                for port, token_ids in self.port_tokens.items()
                if job_token.persistent_id in token_ids
            )
        ):
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
        # Since a port may have multiple input steps, only load the necessary ones.
        step_to_remove = set()
        for step_id, dependency_rows in zip(
            step_ids,
            await asyncio.gather(
                *(
                    asyncio.create_task(self.context.database.get_input_ports(step_id))
                    for step_id in step_ids
                )
            ),
            strict=True,
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
        return port_name

    def remove_port(self, port_name: str) -> MutableSequence[str]:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Removing port {port_name}")
        orphan_tokens = set()
        removed_ports = self.dcg_port.remove_node(port_name)
        for next_port_name in removed_ports:
            if logger.isEnabledFor(logging.DEBUG) and next_port_name != port_name:
                logger.debug(
                    f"Removed port {next_port_name} by deleting port {port_name}"
                )
            for token_id in self.port_tokens.pop(next_port_name, []):
                orphan_tokens.add(token_id)
            self.port_name_ids.pop(next_port_name, None)
        for token_id in orphan_tokens:
            self.remove_token(token_id)
        return removed_ports

    def move_token_to_root(self, token_id: int) -> None:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Moving token {token_id} to root")
        empty_ports = set()
        for removed_token_id in self.dag_tokens.promote_to_source(token_id):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
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
        removed_ports: list[str] = []
        for port_name in empty_ports:
            if port_name not in removed_ports:
                removed_ports.extend(self.remove_port(port_name))

    def remove_token(self, token_id: int) -> None:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Removing token {token_id}")
        removed = {token_id, *self.dag_tokens.remove_node(token_id)}
        for removed_token_id in removed:
            if logger.isEnabledFor(logging.DEBUG) and removed_token_id != token_id:
                logger.debug(
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
            elif logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"Token {old_token_id} is already in desired state. Skipping replacement."
                )
            return
        elif logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Replacing {old_token_id} with {token.persistent_id}")
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


class ProvenanceGraph:
    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context
        self.dag_tokens: DirectedAcyclicGraph = DirectedAcyclicGraph("Provenance")
        self.info_tokens: MutableMapping[int, ProvenanceToken] = {}

    def add(self, src_token: Token, dst_token: Token | None = None) -> None:
        self.dag_tokens.add(
            src_token.persistent_id,
            dst_token.persistent_id if dst_token is not None else None,
        )

    async def build_graph(self, inputs: Iterable[Token]) -> None:
        """
        The provenance graph represents the execution and is always a DAG.
        To traverse the graph, a breadth-first search is performed
        starting from the input tokens and moving backward. At the end of the search,
        a graph is generated where the root nodes represent the tokens whose data
        are available at a specific location, and the leaves correspond to the input tokens.
        """
        token_frontier = deque(inputs)
        loading_context = DefaultDatabaseLoadingContext()
        for t in token_frontier:
            self.add(t)

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
                self.add(token)
            elif is_available := await token.is_available(context=self.context):
                self.add(token)
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
