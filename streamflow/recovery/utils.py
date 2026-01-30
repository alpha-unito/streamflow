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
    queue = deque(provenance.dag_tokens.get_sinks())
    visited = set()
    while queue:
        token_id = queue.popleft()
        visited.add(token_id)
        mapper.add(provenance.info_tokens.get(token_id, None), None)
        for prev_token_id in provenance.dag_tokens.predecessors(token_id):
            if prev_token_id not in visited and prev_token_id not in queue:
                queue.append(prev_token_id)
            mapper.add(
                provenance.info_tokens.get(prev_token_id, None),
                provenance.info_tokens.get(token_id, None),
            )
    return mapper


class DirectGraph:
    __slots__ = ("_successors", "_predecessors", "name")

    def __init__(self, name: str) -> None:
        self.name: str = name
        self._successors: MutableMapping[Any, MutableSet[Any]] = {}
        self._predecessors: MutableMapping[Any, MutableSet[Any]] = {}

    def _add_node(self, node: Any) -> None:
        if node not in self._successors.keys():
            self._successors[node] = set()
            self._predecessors[node] = set()

    def add(self, u: Any | None, v: Any | None) -> None:
        if u is not None:
            self._add_node(u)
        if v is not None:
            self._add_node(v)
        if u and v:
            self._successors[u].add(v)
            self._predecessors[v].add(u)

    def get_nodes(self) -> MutableSequence[Any]:
        return list(self._successors.keys())

    def in_degree(self) -> MutableMapping[Any, int]:
        return {n: len(nodes) for n, nodes in self._predecessors.items()}

    def out_degree(self) -> MutableMapping[Any, int]:
        return {n: len(nodes) for n, nodes in self._successors.items()}

    def predecessors(self, node: Any) -> MutableSet[Any]:
        return set(self._predecessors[node])

    def remove_nodes(
        self, nodes: MutableSequence[Any], prune_dead_end: bool = True
    ) -> MutableSequence[Any]:
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
                # If a successor now has no predecessors, it is unreachable
                # if not (self._predecessors[succ].difference(stack)):
                #     stack.append(succ)
            for pred in self._predecessors[current]:
                self._successors[pred].discard(current)
                # If a parent now has no successors, it is a dead-end
                if prune_dead_end and not (self._successors[pred].difference(stack)):
                    stack.append(pred)
            del self._successors[current]
            del self._predecessors[current]
        return removed_nodes

    def remove_node(
        self, node: Any, prune_dead_end: bool = True
    ) -> MutableSequence[Any]:
        return self.remove_nodes([node], prune_dead_end=prune_dead_end)

    def replace(self, old_node: Any, new_node: Any) -> None:
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
                f"Cannot replace: node '{new_node}' already exists in the graph."
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

    def successors(self, node: Any) -> MutableSet[Any]:
        return set(self._successors[node])

    def __str__(self) -> str:
        return (
            "{\n"
            + "\n".join(
                [
                    f"{k} : {[v for v in values]},"
                    for k, values in self._successors.items()
                ]
            )
            + "\n}\n"
        )


class DirectAcyclicGraph(DirectGraph):

    def promote_to_source(self, node: Any) -> MutableSequence[Any]:
        """
        Move a node to be a source node.

        This implies that all the edges with its previous nodes are deleted.
        All nodes on the path from the sources to the nodes that have no other
        successors are deleted.

        Returns:
            List of all deleted vertices.
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

    def get_sources(self) -> MutableSet[Any]:
        return {n for n, nodes in self._predecessors.items() if len(nodes) == 0}

    def get_sinks(self) -> MutableSet[Any]:
        return {n for n, nodes in self._successors.items() if len(nodes) == 0}


class GraphMapper:
    def __init__(self, context: StreamFlowContext):
        self.dcg_port: DirectGraph = DirectGraph("Dependencies")
        self.dag_tokens: DirectAcyclicGraph = DirectAcyclicGraph("Provenance")
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
                # self.remove_token(token.persistent_id, preserve_token=True)
                return token.persistent_id
            else:
                return equal_token_id
        else:
            # Add port and token relation
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
        try:
            p = next(
                port
                for port, token_ids in self.port_tokens.items()
                if job_token.persistent_id in token_ids
            )
        except StopIteration as e:
            raise FailureHandlingException(e)
        for port_name in self.dcg_port.successors(p):
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
        # Ports are retrieved to identify the steps required for recovery.
        # A step is retrieved if these ports serve as its outputs.
        # Certain ports visited within the graph are excluded if their
        # associated steps are unnecessary or handled in a different phase.
        # - The output ports of a failed step are not retrieved here,
        #   as the failed step itself is processed by another function.
        # - Ports containing an input token are not retrieved explicitly;
        #   since the required tokens are already present, the related step
        #   does not need to be executed.
        # - A port can be the output of multiple steps. In the case of loops,
        #   some steps might be retrieved that are not strictly necessary.
        #   Therefore, the method checks if the input ports of those retrieved
        #   steps fall between the necessary ports in the graph.
        # Note. An input token is defined as a source node within the
        # DAG of the provenance token graph.
        source_token_ids = self.dag_tokens.get_sources()
        if not all(self.token_available[t] for t in source_token_ids):
            ta = {t: self.token_available[t] for t in source_token_ids}
            logger.info(f"Source tokens must be all available: {ta} (exception for token FutureAvailable)")
            # The root can have the output of a schedule step.
            # The token is a root when a synchronization is made, and it is correct that the token is not available
            # raise FailureHandlingException("Source tokens must be all available")
        port_ids = {
            min(self.port_name_ids[port_name])
            for port_name, token_ids in self.port_tokens.items()
            if port_name not in output_port_names
            and any(t_id not in source_token_ids for t_id in token_ids)
        }
        discarded_ports = {
            port_name
            for port_name, token_ids in self.port_tokens.items()
            if not any(t_id not in source_token_ids for t_id in token_ids)
        }
        logger.info(f"Discarded ports from source tokens: {discarded_ports}")
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
            for dep_row, port_row in zip(
                dependency_rows,
                await asyncio.gather(
                    *(
                        asyncio.create_task(
                            self.context.database.get_port(row_dependency["port"])
                        )
                        for row_dependency in dependency_rows
                    )
                ),
                strict=True,
            ):
                if port_row["name"] not in self.port_tokens.keys():
                    step_row = await self.context.database.get_step(step_id)
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(
                            f"The port {port_row['name']} is missing. "
                            f"However, it is the input, called {dep_row['name']}, of the step {step_row['name']}"
                        )
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
        removed_ports = self.dcg_port.remove_node(port_name)
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
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"Moving token {token_id} to root")
        empty_ports = set()
        for removed_token_id in self.dag_tokens.promote_to_source(token_id):
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
        removed = {token_id, *self.dag_tokens.remove_node(token_id)}
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
        self.dag_tokens: DirectAcyclicGraph = DirectAcyclicGraph("Provenance")
        self.info_tokens: MutableMapping[int, ProvenanceToken] = {}

    def add(self, src_token: Token | None, dst_token: Token | None) -> None:
        self.dag_tokens.add(
            src_token.persistent_id if src_token is not None else src_token,
            dst_token.persistent_id if dst_token is not None else dst_token,
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
            self.add(t, None)

        while token_frontier:
            token = token_frontier.popleft()
            port_row = await self.context.database.get_port_from_token(
                token.persistent_id
            )
            step_names = [
                s["name"]
                for s in await asyncio.gather(
                    *(
                        asyncio.create_task(self.context.database.get_step(row["step"]))
                        for row in await self.context.database.get_input_steps(
                            port_row["id"]
                        )
                    )
                )
            ]
            logger.debug(f"Token with id {token.persistent_id} arrives {step_names}")
            # The token is a `JobToken` and its job is running on another recovered workflow
            if (
                isinstance(token, JobToken)
                and (await self.context.failure_manager.is_recovered(token.value.name))
                == TokenAvailability.FutureAvailable
            ):
                is_available = False
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Token with id {token.persistent_id} will be available"
                    )
                self.add(None, token)
            elif is_available := await token.is_available(context=self.context):
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Token with id {token.persistent_id} is available")
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
