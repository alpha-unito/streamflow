from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from collections.abc import Iterable, MutableMapping, MutableSequence, MutableSet
from enum import Enum
from typing import Any

from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import FailureHandlingException
from streamflow.core.utils import contains_persistent_id, get_class_from_name
from streamflow.core.workflow import Token
from streamflow.log_handler import logger
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.persistence.utils import load_dependee_tokens
from streamflow.recovery.utils import get_step_instances_from_output_port
from streamflow.workflow.port import ConnectorPort
from streamflow.workflow.step import ExecuteStep
from streamflow.workflow.token import JobToken


async def create_graph_homomorphism(
    context: StreamFlowContext, provenance: ProvenanceGraph, output_ports
) -> GraphHomomorphism:
    mapper = GraphHomomorphism(context)
    queue = deque((DirectGraph.LEAF,))
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


async def evaluate_token_availability(
    token: Token,
    step_rows,
    port_row: MutableMapping[str, Any],
    context: StreamFlowContext,
) -> TokenAvailability:
    if await token.is_available(context):
        dependency_rows = await context.database.get_input_steps(port_row["id"])
        for step_row in step_rows:
            if issubclass(get_class_from_name(port_row["type"]), ConnectorPort):
                return TokenAvailability.Unavailable
            # TODO: remove dependency name and set True the `recoverable` attribute in the SizeTransformer classes
            dependency_name = next(
                iter(
                    dep_row["name"]
                    for dep_row in dependency_rows
                    if dep_row["step"] == step_row["id"]
                )
            )
            if dependency_name == "__size__":
                return TokenAvailability.Available
            elif json.loads(step_row["params"])["recoverable"]:
                logger.debug(f"Token with id {token.persistent_id} is available")
                return TokenAvailability.Available
        logger.debug(f"Token with id {token.persistent_id} is not recoverable")
        return TokenAvailability.Unavailable
    else:
        logger.debug(f"Token with id {token.persistent_id} is not available")
        return TokenAvailability.Unavailable


class TokenAvailability(Enum):
    Unavailable = 0
    Available = 1
    FutureAvailable = 2


class ProvenanceToken:
    def __init__(
        self,
        token_instance: Token,
        is_available: TokenAvailability,
        port_row: MutableMapping[str, Any],
        step_rows: MutableMapping[str, Any],
    ):
        self.instance: Token = token_instance
        self.is_available: TokenAvailability = is_available
        self.port_row: MutableMapping[str, Any] = port_row
        self.step_rows: MutableMapping[str, Any] = step_rows


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
        vertices_without_next = set()
        for k, values in self.graph.items():
            if vertex in values:
                values.remove(vertex)
            if not values:
                vertices_without_next.add(k)
        for vert in vertices_without_next:
            removed.extend(self.remove(vert))

        # Assign the root node to vertices without parent
        to_mv = set()
        for k in self.keys():
            if k != DirectGraph.ROOT and not self.prev(k):
                to_mv.add(k)
        for k in to_mv:
            self.add(None, k)
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


class GraphHomomorphism:
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

    def add(
        self, token_info_a: ProvenanceToken | None, token_info_b: ProvenanceToken | None
    ) -> None:
        # Add ports into the dependency graph
        if token_info_a:
            port_name_a = token_info_a.port_row["name"]
            self.port_name_ids.setdefault(port_name_a, set()).add(
                token_info_a.port_row["id"]
            )
        else:
            port_name_a = None
        if token_info_b:
            port_name_b = token_info_b.port_row["name"]
            self.port_name_ids.setdefault(port_name_b, set()).add(
                token_info_b.port_row["id"]
            )
        else:
            port_name_b = None
        self.dcg_port.add(port_name_a, port_name_b)

        # Add (or update) tokens into the provenance graph
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
        self.dag_tokens.add(token_a_id, token_b_id)

    def _remove_port_names(self, port_names):
        orphan_tokens = set()  # probably an orphan token
        for port_name in port_names:
            logger.debug(f"_remove_port_names: remove port {port_name}")
            for t_id in self.port_tokens.pop(port_name, []):
                orphan_tokens.add(t_id)
            self.port_name_ids.pop(port_name, None)
        for t_id in orphan_tokens:
            logger.debug(f"_remove_port_names: remove orphan token {t_id}")
            self.remove_token(t_id)
            # if not any(t_id in ts for ts in self.port_tokens.values()):
            #     logger.debug(f"Remove orphan token {t_id}")
            #     self.token_available.pop(t_id, None)
            #     self.token_instances.pop(t_id, None)

    def remove_port(self, port_name):
        logger.debug(f"remove_port {port_name}")
        self._remove_port_names(self.dcg_port.remove(port_name))

    async def get_execute_output_port_names(
        self, job_token: JobToken
    ) -> MutableSequence[str]:
        port_names = set()
        for token_id in self.dag_tokens.succ(job_token.persistent_id):
            if token_id in (DirectGraph.ROOT, DirectGraph.LEAF):
                continue
            port_name = next(
                port_name
                for port_name, out_token_id in self.port_tokens.items()
                if token_id == out_token_id
            )
            # Get newest port
            port_id = max(self.port_name_ids[port_name])
            step_rows = await self.context.database.get_input_steps(port_id)
            for step_row in await asyncio.gather(
                *(
                    asyncio.create_task(self.context.database.get_step(row["step"]))
                    for row in step_rows
                )
            ):
                if issubclass(get_class_from_name(step_row["type"]), ExecuteStep):
                    port_names.add(port_name)
        return list(port_names)

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

    def remove_token_prev_links(self, token_id):
        prev_ids = self.dag_tokens.prev(token_id)
        logger.info(f"Remove token link from {token_id} to {prev_ids}")
        token_without_successors = set()
        for prev_token_id in prev_ids:
            self.dag_tokens[prev_token_id].remove(token_id)
            if len(self.dag_tokens[prev_token_id]) == 0 or (
                isinstance(self.token_instances.get(prev_token_id, None), JobToken)
            ):
                if prev_token_id == DirectGraph.ROOT:
                    raise FailureHandlingException(
                        "Impossible execute a workflow without a ROOT"
                    )
                token_without_successors.add(prev_token_id)
        logger.info(
            f"Remove token link: token {token_id} found token_without_successors: {token_without_successors}"
        )
        return token_without_successors

    async def replace_token_and_remove(self, port_name, provenance_token):
        # todo : vedere di unire con update_token perché fanno praticamente la stessa cosa
        token = provenance_token.instance
        old_token_id = self.get_equal_token(port_name, token)
        # if token.persistent_id == old_token_id:
        #     return
        logger.info(f"Replacing {old_token_id} with {token.persistent_id}")
        token_without_successors = self.remove_token_prev_links(old_token_id)
        for t_id in token_without_successors:
            self.remove_token(t_id)

        # remove old token
        self.port_tokens[port_name].remove(old_token_id)
        self.token_available.pop(old_token_id)
        self.token_instances.pop(old_token_id)

        # replace
        self.dag_tokens.replace(old_token_id, token.persistent_id)

        # add new token
        self.port_tokens.setdefault(port_name, set()).add(token.persistent_id)
        self.token_instances[token.persistent_id] = token
        self.token_available[token.persistent_id] = (
            provenance_token.is_available == TokenAvailability.Available
        )

        logger.debug(
            f"replace_token_and_remove: token id: {token.persistent_id} "
            f"is_available: {provenance_token.is_available == TokenAvailability.Available}"
        )

    def remove_token(self, token_id):
        if token_id == DirectGraph.ROOT:
            return
        if succ_ids := self.dag_tokens.succ(token_id):
            # noop
            logger.info(f"WARN. Deleting {token_id} but it has successors: {succ_ids}")
            # raise Exception(
            #     f"Impossible remove token {token_id} because it has successors: {succ_ids}"
            # )
        logger.info(f"Remove token: token {token_id}")
        self.token_available.pop(token_id, None)
        self.token_instances.pop(token_id, None)
        token_without_successors = self.remove_token_prev_links(token_id)
        self.dag_tokens.remove(token_id)
        for t_id in token_without_successors:
            self.remove_token(t_id)
        empty_ports = set()
        for port_name, token_list in self.port_tokens.items():
            if token_id in token_list:
                self.port_tokens[port_name].remove(token_id)
            if len(self.port_tokens[port_name]) == 0:
                empty_ports.add(port_name)
        for port_name in empty_ports:
            logger.info(
                f"Remove token: token {token_id} found to remove port {port_name}"
            )
            self.remove_port(port_name)

    def _update_token(self, port_name: str, token: Token, is_available: bool) -> int:
        # Check if there is the same token in the port
        if equal_token_id := self.get_equal_token(port_name, token):
            # Check if the token is newer or available
            if (
                equal_token_id > token.persistent_id
                or self.token_available[equal_token_id]
            ):
                # Current saved token (`equal_token_id`) is available, thus
                # it is not necessary to substitute with the new token
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Current token: {equal_token_id} is chosen on port {port_name}. "
                        f"Discarded token {token.persistent_id}"
                    )
                return equal_token_id
            if is_available:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Replaced token {equal_token_id} with {token.persistent_id} on port {port_name}"
                    )
                self.remove_token(equal_token_id)
                # for t_id in self.remove_token_prev_links(equal_token_id):
                #     self.remove_token(t_id)
                self.dag_tokens.add(None, equal_token_id)
            self.port_tokens[port_name].remove(equal_token_id)
            self.token_instances.pop(equal_token_id)
            self.token_available.pop(equal_token_id)
            self.dag_tokens.replace(equal_token_id, token.persistent_id)
        # Add port and token relation
        if port_name:  # port name can be None (ROOT or LEAF)
            self.port_tokens.setdefault(port_name, set()).add(token.persistent_id)
        self.token_instances[token.persistent_id] = token
        self.token_available[token.persistent_id] = is_available
        return token.persistent_id

    async def get_port_and_step_ids(
        self, output_port_names: Iterable[str]
    ) -> tuple[MutableSet[int], MutableSet[int]]:
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

        # Remove steps with some missing input ports
        # A port can have multiple input steps. It is necessary to load only the needed steps
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
            for row_port in await asyncio.gather(
                *(
                    asyncio.create_task(
                        self.context.database.get_port(row_dependency["port"])
                    )
                    for row_dependency in dependency_rows
                )
            ):
                if row_port["name"] not in self.port_tokens.keys():
                    step_to_remove.add(step_id)
        for step_id in step_to_remove:
            step_ids.remove(step_id)
        return port_ids, step_ids


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
        The provenance graph represent the execution, and is always a DAG.
        Visit the provenance graph with a breadth-first search and is done
        backward starting from the input tokens. At the end of the search,
        we have a tree where at root there are token which data are available
        in some location and leaves will be the input tokens.
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
            # Get steps which has the output port (nb. a port can have multiple steps due to the loop case)
            step_rows = await get_step_instances_from_output_port(
                port_row["id"], self.context
            )
            if (
                isinstance(token, JobToken)
                and (
                    is_available := await self.context.failure_manager.is_running_token(
                        token
                    )
                )
                == TokenAvailability.FutureAvailable
            ):
                self.add(None, token)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Job token {token.value.name} with id {token.persistent_id} is running"
                    )
            else:
                if (
                    is_available := await evaluate_token_availability(
                        token, step_rows, port_row, self.context
                    )
                ) == TokenAvailability.Available:
                    self.add(None, token)
                else:
                    if prev_tokens := await load_dependee_tokens(
                        token.persistent_id, self.context, loading_context
                    ):
                        for prev_token in prev_tokens:
                            self.add(prev_token, token)
                            if (
                                prev_token.persistent_id not in self.info_tokens.keys()
                                and not contains_persistent_id(
                                    prev_token.persistent_id, token_frontier
                                )
                            ):
                                token_frontier.append(prev_token)
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(
                                f"Token with id {token.persistent_id} is not available. "
                                f"Its previous tokens are {[t.persistent_id for t in prev_tokens]}"
                            )
                    elif issubclass(
                        get_class_from_name(port_row["type"]), ConnectorPort
                    ):
                        pass  # do nothing
                    else:
                        raise FailureHandlingException(
                            f"Token with id {token.persistent_id} is not available and it does not have previous tokens"
                        )
            self.info_tokens.setdefault(
                token.persistent_id,
                ProvenanceToken(token, is_available, port_row, step_rows),
            )
