from __future__ import annotations

import logging
from collections.abc import MutableMapping, MutableSet

from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import FailureHandlingException
from streamflow.core.workflow import Job, Step, Token, Workflow
from streamflow.log_handler import logger
from streamflow.persistence.loading_context import WorkflowBuilder
from streamflow.recovery.rollback_recovery import (
    DirectGraph,
    GraphMapper,
    ProvenanceGraph,
    create_graph_mapper,
)
from streamflow.recovery.utils import populate_workflow
from streamflow.workflow.port import FilterTokenPort
from streamflow.workflow.step import ScatterStep
from streamflow.workflow.token import IterationTerminationToken, TerminationToken
from streamflow.workflow.utils import get_job_token


class RollbackRecoveryPolicy:
    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context

    async def recover_workflow(self, failed_job: Job, failed_step: Step) -> Workflow:
        workflow = failed_step.workflow
        workflow_builder = WorkflowBuilder(deep_copy=False)
        new_workflow = await workflow_builder.load_workflow(
            workflow.context, workflow.persistent_id
        )
        # Retrieve tokens
        provenance = ProvenanceGraph(workflow.context)
        job_token = get_job_token(
            failed_job.name, failed_step.get_input_port("__job__").token_list
        )
        await provenance.build_graph(
            inputs=(
                job_token,
                *failed_job.inputs.values(),
            )
        )
        mapper = await create_graph_mapper(
            self.context, provenance, failed_step.get_output_ports().values()
        )
        # Synchronize across multiple recovery workflows
        await self.context.failure_manager.sync_workflows(mapper, new_workflow)
        # Populate new workflow
        steps = await mapper.get_port_and_step_ids(failed_step.output_ports.values())
        await populate_workflow(
            steps, failed_step, new_workflow, workflow_builder, failed_job
        )
        await _inject_tokens(
            new_workflow,
            mapper.dcg_port[DirectGraph.ROOT],
            mapper.port_tokens,
            mapper.token_instances,
            mapper.token_available,
        )
        await _set_step_states(new_workflow, mapper)
        return new_workflow


async def _inject_tokens(
    new_workflow: Workflow,
    init_ports: MutableSet[str],
    port_tokens: MutableMapping[str, MutableSet[int]],
    token_instances: MutableMapping[int, Token],
    token_available: MutableMapping[int, bool],
) -> None:
    for port_name in init_ports:
        token_list = [
            token_instances[token_id]
            for token_id in port_tokens[port_name]
            if token_id not in (DirectGraph.ROOT, DirectGraph.LEAF)
            and token_available[token_id]
        ]
        token_list.sort(key=lambda x: x.tag, reverse=False)
        if len(
            tags := {(token.persistent_id, token.tag) for token in token_list}
        ) != len(token_list):
            raise FailureHandlingException(
                f"Port {port_name} has multiple tokens with same tag (id, tag): {tags}"
            )
        if any(
            isinstance(token, (TerminationToken, IterationTerminationToken))
            for token in token_list
        ):
            raise FailureHandlingException("Impossible to load a TerminationToken")
        port = new_workflow.ports[port_name]
        for token in token_list:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Injecting token {token.tag} of port {port.name}")
            port.put(token)
        if len(port.token_list) > 0 and len(port.token_list) == len(
            port_tokens[port_name]
        ):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Injecting termination token on port {port.name}")
            port.put(TerminationToken())


async def _set_step_states(new_workflow: Workflow, mapper: GraphMapper) -> None:
    for step in new_workflow.steps.values():
        if isinstance(step, ScatterStep):
            port = step.get_output_port()
            missing_tokens = tuple(
                mapper.token_instances[token_id].tag
                for token_id in mapper.port_tokens[port.name]
                if not mapper.token_available[token_id]
            )
            new_workflow.ports[port.name] = FilterTokenPort(
                new_workflow,
                port.name,
                filter_function=lambda t, tokens=missing_tokens: t.tag in tokens,
            )
            new_workflow.ports[port.name].token_list = port.token_list
