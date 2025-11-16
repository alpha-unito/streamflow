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
from streamflow.workflow.step import ExecuteStep, TransferStep
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
        self.graph: MutableMapping[Any, MutableSet[Any]] = {}
        self.name: str = name

    def add(self, src: Any | None, dst: Any | None) -> None:
        src = src or DirectGraph.ROOT
        dst = dst or DirectGraph.LEAF
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"{self.name} Graph: Added {src} -> {dst}")
        self.graph.setdefault(src, set()).add(dst)

    def empty(self) -> bool:
        return False if self.graph else True

    def items(self):
        return self.graph.items()

    def keys(self):
        return self.graph.keys()

    def prev(self, vertex: Any) -> MutableSet[Any]:
        """Return the previous nodes of the vertex."""
        return {v for v, next_vs in self.graph.items() if vertex in next_vs}

    def remove(self, vertex: Any) -> MutableSequence[Any]:
        self.graph.pop(vertex, None)
        removed = [vertex]
        # Delete nodes which are not connected to the leaves nodes
        dead_end_nodes = set()
        for node, values in self.graph.items():
            if vertex in values:
                values.remove(vertex)
            if len(values) == 0:
                dead_end_nodes.add(node)
        for node in dead_end_nodes:
            removed.extend(self.remove(node))

        # Assign the root node to vertices without parent
        orphan_nodes = set()
        for node in self.keys():
            if node != DirectGraph.ROOT and not self.prev(node):
                orphan_nodes.add(node)
        for node in orphan_nodes:
            self.add(None, node)
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
        # return f"{json.dumps({k : list(v) for k, v in self.graph.items()}, indent=2)}"
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
                self.remove_token(token.persistent_id, preserve_token=True)
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
        for port_name in self.dcg_port.succ(
            next(
                port
                for port, token_ids in self.port_tokens.items()
                if job_token.persistent_id in token_ids
            )
        ):
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

    def remove_port(self, port_name: str) -> None:
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"Remove port {port_name}")
        orphan_tokens = set()
        for next_port_name in self.dcg_port.remove(port_name):
            for token_id in self.port_tokens.pop(next_port_name, []):
                orphan_tokens.add(token_id)
            self.port_name_ids.pop(next_port_name, None)
        for token_id in orphan_tokens:
            self.remove_token(token_id)

    def remove_token(self, token_id: int, preserve_token: bool = True):
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"Remove token id {token_id}")
        if token_id == DirectGraph.ROOT:
            return
        # Remove previous links
        token_leaves = set()
        for prev_token_id in self.dag_tokens.prev(token_id):
            self.dag_tokens[prev_token_id].remove(token_id)
            if len(self.dag_tokens[prev_token_id]) == 0 or (
                isinstance(self.token_instances.get(prev_token_id, None), JobToken)
            ):
                if prev_token_id == DirectGraph.ROOT:
                    raise FailureHandlingException(
                        "Impossible execute a workflow without a ROOT"
                    )
                token_leaves.add(prev_token_id)
        # Delete end-road branches
        for leaf_id in token_leaves:
            self.remove_token(leaf_id)
        # Delete token (if needed)
        if not preserve_token:
            self.token_available.pop(token_id, None)
            self.token_instances.pop(token_id, None)
            self.dag_tokens.remove(token_id)
        if not preserve_token:
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
        self.remove_token(token.persistent_id, preserve_token=True)


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

    async def build_graph(self, inputs: Iterable[Token]):
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
