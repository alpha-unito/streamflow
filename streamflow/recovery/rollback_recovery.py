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
from streamflow.core.utils import (
    contains_persistent_id,
    get_class_from_name,
    get_class_fullname,
)
from streamflow.core.workflow import Token
from streamflow.log_handler import logger
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.persistence.utils import load_dependee_tokens
from streamflow.recovery.utils import (
    get_key_by_value,
    get_step_instances_from_output_port,
    load_and_add_ports,
    load_missing_ports,
)
from streamflow.workflow.combinator import LoopTerminationCombinator
from streamflow.workflow.port import ConnectorPort
from streamflow.workflow.step import (
    CombinatorStep,
    ExecuteStep,
    InputInjectorStep,
    LoopOutputStep,
)
from streamflow.workflow.token import JobToken

#                               t_a 0  t_b 0 t_c 0
#                                 |     |    |
#                                   t_a 0.0


#       sum...      combinator
#           \       /
#            merge 0.0
#               |
#            combinator
#               |
#           split 0.1
#       /       |       \
# sum 0.1.0     ...      sum 0.1.x
#       \       |       /
#           merge 0.1


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

    # for out_port in output_ports:
    #     mapper.dcg_port.replace(DirectGraph.LEAF, out_port.name)
    #     mapper.dcg_port.add(out_port.name, DirectGraph.LEAF)
    #     # Add a token to emulate the token not produced in the failed step
    #     placeholder = Token(
    #         None,
    #         get_tag(
    #             provenance.info_tokens[t].instance
    #             for t in provenance.dag_tokens.prev(DirectGraph.LEAF)
    #             if not isinstance(t, JobToken)
    #         ),
    #     )
    #     placeholder.persistent_id = -1
    #
    #     mapper.dag_tokens.replace(DirectGraph.LEAF, placeholder.persistent_id)
    #     mapper.dag_tokens.add(placeholder.persistent_id, DirectGraph.LEAF)
    #     mapper.token_instances[placeholder.persistent_id] = placeholder
    #     mapper.token_available[placeholder.persistent_id] = False
    #     mapper.port_name_ids.setdefault(out_port.name, set()).add(
    #         out_port.persistent_id
    #     )
    #     mapper.port_tokens.setdefault(out_port.name, set()).add(
    #         placeholder.persistent_id
    #     )
    return mapper


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


async def get_execute_step_out_token_ids(next_token_ids, context):
    execute_step_out_token_ids = set()
    for t_id in next_token_ids:
        if t_id > 0:
            port_row = await context.database.get_port_from_token(t_id)
            for step_id_row in await context.database.get_input_steps(port_row["id"]):
                step_row = await context.database.get_step(step_id_row["step"])
                if step_row["type"] == get_class_fullname(ExecuteStep):
                    execute_step_out_token_ids.add(t_id)
        elif t_id == -1:
            return [t_id]
        else:
            raise Exception(f"Token {t_id} not valid")
    return execute_step_out_token_ids


async def evaluate_token_availability(
    token: Token,
    step_rows,
    port_row: MutableMapping[str, Any],
    context: StreamFlowContext,
):
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


def is_there_step_type(rows, types):
    for step_row in rows:
        if issubclass(get_class_from_name(step_row["type"]), types):
            return True
    return False


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

    def is_present(self, t_id):
        for ts in self.port_tokens.values():
            if t_id in ts:
                return True
        return False

    def _remove_port_names(self, port_names):
        orphan_tokens = set()  # probably a orphan token
        for port_name in port_names:
            logger.debug(f"_remove_port_names: remove port {port_name}")
            for t_id in self.port_tokens.pop(port_name, []):
                orphan_tokens.add(t_id)
            self.port_name_ids.pop(port_name, None)
        for t_id in orphan_tokens:
            logger.debug(f"_remove_port_names: remove orphan token {t_id}")
            self.remove_token(t_id)
            # if not self.is_present(t_id):
            #     logger.debug(f"Remove orphan token {t_id}")
            #     self.token_available.pop(t_id, None)
            #     self.token_instances.pop(t_id, None)

    def remove_port(self, port_name):
        logger.debug(f"remove_port {port_name}")
        self._remove_port_names(self.dcg_port.remove(port_name))

    async def get_execute_output_port_names(self, job_token: JobToken):
        port_names = set()
        for t_id in self.dag_tokens.succ(job_token.persistent_id):
            logger.debug(
                f"Token {t_id} è successivo al job_token {job_token.persistent_id}"
            )
            if t_id in (DirectGraph.ROOT, DirectGraph.LEAF):
                continue
            port_name = get_key_by_value(t_id, self.port_tokens)
            port_id = max(self.port_name_ids[port_name])

            step_rows = await self.context.database.get_input_steps(port_id)
            for step_row in await asyncio.gather(
                *(
                    asyncio.create_task(self.context.database.get_step(sr["step"]))
                    for sr in step_rows
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

    async def get_port_and_step_ids(self, exclude_ports):
        steps = set()
        ports = {
            min(self.port_name_ids[port_name])
            for port_name in self.port_tokens.keys()
            if port_name not in exclude_ports
        }
        for row_dependencies in await asyncio.gather(
            *(
                asyncio.create_task(self.context.database.get_input_steps(port_id))
                for port_id in ports
            )
        ):
            for row_dependency in row_dependencies:
                step_name = (
                    await self.context.database.get_step(row_dependency["step"])
                )["name"]
                logger.debug(
                    f"get_port_and_step_ids: Step {step_name} (id {row_dependency['step']}) "
                    f"rescued from the port {get_key_by_value(row_dependency['port'], self.port_name_ids)} "
                    f"(id {row_dependency['port']})"
                )
                steps.add(row_dependency["step"])
        rows_dependencies = await asyncio.gather(
            *(
                asyncio.create_task(self.context.database.get_input_ports(step_id))
                for step_id in steps
            )
        )
        step_to_remove = set()
        for step_id, row_dependencies in zip(steps, rows_dependencies):
            for row_port in await asyncio.gather(
                *(
                    asyncio.create_task(
                        self.context.database.get_port(row_dependency["port"])
                    )
                    for row_dependency in row_dependencies
                )
            ):
                if row_port["name"] not in self.port_tokens.keys():
                    step_name = (await self.context.database.get_step(step_id))["name"]
                    logger.debug(
                        f"get_port_and_step_ids: Step {step_name} (id {step_id}) removed because "
                        f"port {row_port['name']} is not present in port_tokens. "
                        f"Is it present in ports to load? {row_port['id'] in ports}"
                    )
                    step_to_remove.add(step_id)
        for s_id in step_to_remove:
            steps.remove(s_id)
        return ports, steps

    async def populate_workflow(
        self,
        port_ids: Iterable[int],
        step_ids: Iterable[int],
        failed_step,
        new_workflow,
        loading_context,
    ):
        logger.debug(
            f"populate_workflow: wf {new_workflow.name} dag[INIT] "
            f"{[get_key_by_value(t_id, self.port_tokens) for t_id in self.dag_tokens[DirectGraph.ROOT]]}"
        )
        logger.debug(
            f"populate_workflow: wf {new_workflow.name} port {new_workflow.ports.keys()}"
        )
        await load_and_add_ports(port_ids, new_workflow, loading_context)

        step_name_id = await self.load_and_add_steps(
            step_ids, new_workflow, loading_context
        )

        await load_missing_ports(new_workflow, step_name_id, loading_context)

        # add failed step into new_workflow
        logger.debug(
            f"populate_workflow: wf {new_workflow.name} add_3.0 step {failed_step.name}"
        )
        new_step = await loading_context.load_step(
            new_workflow.context,
            failed_step.persistent_id,
        )
        new_workflow.steps[new_step.name] = new_step
        for port in await asyncio.gather(
            *(
                asyncio.create_task(
                    loading_context.load_port(new_workflow.context, p.persistent_id)
                )
                for p in failed_step.get_output_ports().values()
                if p.name not in new_workflow.ports.keys()
            )
        ):
            logger.debug(
                f"populate_workflow: wf {new_workflow.name} add_3 port {port.name}"
            )
            new_workflow.ports[port.name] = port

        # fixing skip ports in loop-terminator
        for step in new_workflow.steps.values():
            if isinstance(step, CombinatorStep) and isinstance(
                step.combinator, LoopTerminationCombinator
            ):
                dependency_names = set()
                for dep_name, port_name in step.input_ports.items():
                    # Some data are available so added directly in the LoopCombinatorStep inputs.
                    # In this case, LoopTerminationCombinator must not wait on ports where these data are created.
                    if port_name not in new_workflow.ports.keys():
                        dependency_names.add(dep_name)
                for name in dependency_names:
                    step.input_ports.pop(name)
                    step.combinator.items.remove(name)

        # remove steps which have not input ports loaded in new workflow
        steps_to_remove = set()
        for step in new_workflow.steps.values():
            if isinstance(step, InputInjectorStep):
                continue
            for p_name in step.input_ports.values():
                if p_name not in new_workflow.ports.keys():
                    # problema nato dai loop. Vengono caricati nel new_workflow tutti gli step che hanno come output
                    # le port nel grafo. Però nei loop, più step hanno stessa porta di output
                    # (forward, backprop, loop-term). per capire se lo step sia necessario controlliamo che anche
                    # le sue port di input siano state caricate
                    logger.debug(
                        f"populate_workflow: Remove step {step.name} from wf {new_workflow.name} "
                        f"because input port {p_name} is missing"
                    )
                    steps_to_remove.add(step.name)
            if step.name not in steps_to_remove and isinstance(
                step, type(None)  # TODO BackPropagationTransformer
            ):
                loop_terminator = False
                for port in step.get_output_ports().values():
                    for prev_step in port.get_input_steps():
                        if isinstance(prev_step, CombinatorStep) and isinstance(
                            prev_step.combinator, LoopTerminationCombinator
                        ):
                            loop_terminator = True
                            break
                    if loop_terminator:
                        break
                if not loop_terminator:
                    logger.debug(
                        f"populate_workflow: Remove step {step.name} from wf {new_workflow.name} because "
                        f"it is missing a prev step like LoopTerminationCombinator"
                    )
                    steps_to_remove.add(step.name)
        for step_name in steps_to_remove:
            logger.debug(
                f"populate_workflow: Rimozione (2) definitiva step {step_name} dal new_workflow {new_workflow.name}"
            )
            new_workflow.steps.pop(step_name)
        logger.debug("populate_workflow: Finish")

    async def load_and_add_steps(self, step_ids, new_workflow, loading_context):
        new_step_ids = set()
        step_name_id = {}
        for sid, step in zip(
            step_ids,
            await asyncio.gather(
                *(
                    asyncio.create_task(
                        loading_context.load_step(new_workflow.context, step_id)
                    )
                    for step_id in step_ids
                )
            ),
        ):
            logger.debug(f"Loaded step {step.name} (id {sid})")
            step_name_id[step.name] = sid

            # if there are not the input ports in the workflow, the step is not added
            if not (set(step.input_ports.values()) - set(new_workflow.ports.keys())):
                if isinstance(step, type(None)):  # OutputForwardTransformer):
                    port_id = min(self.port_name_ids[step.get_output_port().name])
                    for (
                        step_dep_row
                    ) in await new_workflow.context.database.get_output_steps(port_id):
                        step_row = await new_workflow.context.database.get_step(
                            step_dep_row["step"]
                        )
                        if step_row[
                            "name"
                        ] not in new_workflow.steps.keys() and issubclass(
                            get_class_from_name(step_row["type"]), LoopOutputStep
                        ):
                            logger.debug(
                                f"Step {step_row['name']} from id {step_row['id']} will be added soon (2)"
                            )
                            new_step_ids.add(step_row["id"])
                elif isinstance(step, type(None)):  # BackPropagationTransformer):
                    # for port_name in step.output_ports.values(): # potrebbe sostituire questo for
                    for (
                        port_dep_row
                    ) in await new_workflow.context.database.get_output_ports(
                        step_name_id[step.name]
                    ):
                        # if there are more iterations
                        if (
                            len(
                                self.port_tokens[
                                    step.output_ports[port_dep_row["name"]]
                                ]
                            )
                            > 1
                        ):
                            for (
                                step_dep_row
                            ) in await new_workflow.context.database.get_input_steps(
                                port_dep_row["port"]
                            ):
                                step_row = await new_workflow.context.database.get_step(
                                    step_dep_row["step"]
                                )
                                if issubclass(
                                    get_class_from_name(step_row["type"]),
                                    CombinatorStep,
                                ) and issubclass(
                                    get_class_from_name(
                                        json.loads(step_row["params"])["combinator"][
                                            "type"
                                        ]
                                    ),
                                    LoopTerminationCombinator,
                                ):
                                    logger.debug(
                                        f"Step {step_row['name']} from id {step_row['id']} will be added soon (1)"
                                    )
                                    new_step_ids.add(step_row["id"])
                logger.debug(
                    f"populate_workflow: (1) Step {step.name} loaded in the wf {new_workflow.name}"
                )
                new_workflow.steps[step.name] = step
            else:
                logger.debug(
                    f"populate_workflow: Step {step.name} is not loaded because in the wf {new_workflow.name}"
                    f"the ports {set(step.input_ports.values()) - set(new_workflow.ports.keys())} are missing."
                    f"It is present in the workflow: {step.name in new_workflow.steps.keys()}"
                )
        for sid, other_step in zip(
            new_step_ids,
            await asyncio.gather(
                *(
                    asyncio.create_task(
                        loading_context.load_step(new_workflow.context, step_id)
                    )
                    for step_id in new_step_ids
                )
            ),
        ):
            logger.debug(
                f"populate_workflow: (2) Step {other_step.name} (from step id {sid}) loaded "
                f"in the wf {new_workflow.name}"
            )
            step_name_id[other_step.name] = sid
            new_workflow.steps[other_step.name] = other_step
        logger.debug("populate_workflow: Step caricati")
        return step_name_id


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
