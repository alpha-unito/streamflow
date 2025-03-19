from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable, MutableMapping, MutableSequence, MutableSet
from typing import cast

from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import FailureHandlingException
from streamflow.core.utils import get_class_from_name, get_tag
from streamflow.core.workflow import Job, Step, Token, Workflow
from streamflow.log_handler import logger
from streamflow.persistence.loading_context import WorkflowBuilder
from streamflow.recovery.rollback_recovery import (
    DirectGraph,
    GraphMapper,
    ProvenanceGraph,
    TokenAvailability,
    create_graph_mapper,
)
from streamflow.workflow.port import (
    ConnectorPort,
    FilterTokenPort,
    InterWorkflowPort,
    JobPort,
)
from streamflow.workflow.step import ExecuteStep, ScatterStep
from streamflow.workflow.token import (
    IterationTerminationToken,
    JobToken,
    TerminationToken,
)
from streamflow.workflow.utils import get_job_token


async def _get_output_tokens(
    next_token_ids: MutableSequence[int], context: StreamFlowContext
) -> MutableSet[int]:
    execute_step_out_token_ids = set()
    for token_id in next_token_ids:
        port_row = await context.database.get_port_from_token(token_id)
        for step_id_row in await context.database.get_input_steps(port_row["id"]):
            step_row = await context.database.get_step(step_id_row["step"])
            if issubclass(get_class_from_name(step_row["type"]), ExecuteStep):
                execute_step_out_token_ids.add(token_id)
    return execute_step_out_token_ids


async def _inject_tokens(
    new_workflow: Workflow,
    init_ports: MutableSet[str],
    port_tokens: MutableMapping[str, MutableSet[int]],
    token_instances: MutableMapping[int, Token],
    token_available: MutableMapping[int, bool],
) -> None:
    for port_name in init_ports:
        token_list = sorted(
            [
                token_instances[token_id]
                for token_id in port_tokens[port_name]
                if token_id not in (DirectGraph.ROOT, DirectGraph.LEAF)
                and token_available[token_id]
            ],
            key=lambda x: x.tag,
        )
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


async def _populate_workflow(
    step_ids: Iterable[int],
    failed_step: Step,
    new_workflow: Workflow,
    workflow_builder: WorkflowBuilder,
    failed_job: Job,
) -> None:
    await asyncio.gather(
        *(
            asyncio.create_task(
                workflow_builder.load_step(new_workflow.context, step_id)
            )
            for step_id in step_ids
        )
    )
    # Add failed step into new_workflow
    await workflow_builder.load_step(
        new_workflow.context,
        failed_step.persistent_id,
    )
    # Instantiate ports capable of moving tokens across workflows
    for port in new_workflow.ports.values():
        if not isinstance(port, (JobPort, ConnectorPort)):
            new_workflow.create_port(InterWorkflowPort, port.name)
    for port in failed_step.get_output_ports().values():
        cast(InterWorkflowPort, new_workflow.ports[port.name]).add_inter_port(
            port, border_tag=get_tag(failed_job.inputs.values())
        )


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
        mapper = await create_graph_mapper(self.context, provenance)
        # Synchronize across multiple recovery workflows
        await self.sync_workflows(mapper, new_workflow)
        # Populate new workflow
        steps = await mapper.get_port_and_step_ids(failed_step.output_ports.values())
        await _populate_workflow(
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

    async def sync_workflows(self, mapper: GraphMapper, workflow: Workflow) -> None:
        for job_token in list(
            # todo: visit the jobtoken bottom-up in the graph
            filter(lambda t: isinstance(t, JobToken), mapper.token_instances.values())
        ):
            job_name = job_token.value.name
            retry_request = self.context.failure_manager.get_request(job_name)
            if (
                is_available := await self.context.failure_manager.is_recovered(
                    job_name
                )
            ) == TokenAvailability.FutureAvailable:
                # The `retry_request` is the current job running, instead
                # the `job_token` is the token to remove in the graph because
                # the workflow will depend on the already running job
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Synchronize rollbacks: job {job_name} is running")
                # todo: create a unit test for this case
                for port_name in await mapper.get_output_ports(job_token):
                    if port_name in retry_request.workflow.ports.keys():
                        cast(
                            InterWorkflowPort, retry_request.workflow.ports[port_name]
                        ).add_inter_port(
                            workflow.create_port(cls=InterWorkflowPort, name=port_name)
                        )
                # Remove tokens recovered in other workflows
                for token_id in await _get_output_tokens(
                    [
                        t
                        for t in mapper.dag_tokens.succ(job_token.persistent_id)
                        if t not in (DirectGraph.ROOT, DirectGraph.LEAF)
                    ],
                    self.context,
                ):
                    mapper.remove_token(token_id, preserve_token=True)
            elif is_available == TokenAvailability.Available:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Synchronize rollbacks: job {job_token.value.name} output available"
                    )
                # Search execute token after job token, replace this token with job_req token.
                # Then remove all the prev tokens
                for port_name in await mapper.get_output_ports(job_token):
                    if port_name in retry_request.output_tokens.keys():
                        new_token = retry_request.output_tokens[port_name]
                        await mapper.replace_token(
                            port_name,
                            new_token,
                            True,
                        )
                        mapper.remove_token(
                            new_token.persistent_id, preserve_token=True
                        )
            else:
                await self.context.failure_manager.update_request(job_name)
                retry_request.workflow = workflow
