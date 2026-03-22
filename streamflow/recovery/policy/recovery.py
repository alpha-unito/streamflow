from __future__ import annotations

import asyncio
import logging
from collections.abc import MutableSequence, MutableSet
from typing import cast

from streamflow.core.exception import FailureHandlingException
from streamflow.core.recovery import RecoveryPolicy
from streamflow.core.utils import get_tag
from streamflow.core.workflow import Job, Step, Token, Workflow
from streamflow.log_handler import logger
from streamflow.persistence.loading_context import WorkflowBuilder
from streamflow.recovery.utils import (
    GraphMapper,
    ProvenanceGraph,
    TokenAvailability,
    create_graph_mapper,
)
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.port import (
    BoundaryAction,
    ConnectorPort,
    InterWorkflowJobPort,
    InterWorkflowPort,
    JobPort,
)
from streamflow.workflow.token import JobToken
from streamflow.workflow.utils import get_job_token


def _get_recovery_port(
    token_id: int,
    mapper: GraphMapper,
    original_workflow: Workflow,
    recovery_workflow: Workflow,
) -> InterWorkflowPort:
    port_name = next(
        curr_port
        for curr_port, curr_tokens in mapper.port_tokens.items()
        if token_id in curr_tokens
    )
    if port_name not in recovery_workflow.ports.keys() or not isinstance(
        recovery_workflow.ports[port_name], InterWorkflowPort
    ):
        port = recovery_workflow.create_port(
            cls=type(
                original_workflow.ports[port_name]
            ),  # retry_request.workflow.ports[port_name]),
            name=port_name,
        )
    else:
        port = recovery_workflow.ports[port_name]
    return cast(InterWorkflowPort, port)


async def _inject_tokens(
    failed_job: Job,
    failed_step: Step,
    mapper: GraphMapper,
    workflow: Workflow,
) -> None:
    workflow_output_ports = {v: k for k, v in failed_step.output_ports.items()}
    # Step output ports are in the mapper if a loop exists. Otherwise, they are
    # missing and must be added manually to set the boundary rule
    for port_name in mapper.port_tokens.keys() | workflow_output_ports.keys():
        token_list = (
            sorted(
                [
                    mapper.token_instances[token_id]
                    for token_id in mapper.port_tokens[port_name]
                    if mapper.token_availability[token_id]
                ],
                key=lambda x: x.tag,
            )
            # Discard workflow output port
            if port_name in mapper.port_tokens.keys()
            else ()
        )
        if len(tags := {token.tag for token in token_list}) != len(token_list):
            raise FailureHandlingException(
                f"Port {port_name} has multiple tokens with same tag: {tags}"
            )
        port = workflow.ports[port_name]

        # Set token tag boundary rules
        # All ports in a recovery workflow are InterWorkflowPorts (except DeployStep ports)
        # because future recovery workflows may attach to them.
        if isinstance(port, InterWorkflowPort):
            # Handle output ports of failed step
            if port.name in workflow_output_ports.keys():
                # Propagate recovered tokens to original workflow ports
                cast(InterWorkflowPort, workflow.ports[port.name]).add_inter_port(
                    port=failed_step.get_output_port(workflow_output_ports[port.name]),
                    boundary_tags=[get_tag(failed_job.inputs.values())],
                    boundary_action=BoundaryAction.PROPAGATE,
                )
                # Terminate execution in the recovery workflow
                cast(InterWorkflowPort, workflow.ports[port.name]).add_inter_port(
                    port=workflow.ports[port.name],
                    boundary_tags=[get_tag(failed_job.inputs.values())],
                    boundary_action=BoundaryAction.TERMINATE,
                )
            else:
                port.add_inter_port(
                    port=port,
                    boundary_tags=[
                        mapper.token_instances[token_id].tag
                        for token_id in mapper.port_tokens[port_name]
                    ],
                    boundary_action=(
                        BoundaryAction.PROPAGATE | BoundaryAction.TERMINATE
                    ),
                )
        # Inject tokens
        for token in token_list:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Injecting token {token.tag} of port {port.name}")
            port.put(token)


async def _populate_workflow(
    failed_step: Step,
    step_ids: MutableSet[int],
    workflow: Workflow,
    workflow_builder: WorkflowBuilder,
) -> None:
    await asyncio.gather(
        *(
            asyncio.create_task(workflow_builder.load_step(workflow.context, step_id))
            for step_id in step_ids
        )
    )
    # Add the failed step to the new workflow
    await workflow_builder.load_step(workflow.context, failed_step.persistent_id)
    # Instantiate ports that can transfer tokens between workflows
    for port in workflow.ports.values():
        if not isinstance(
            port, (ConnectorPort, InterWorkflowJobPort, InterWorkflowPort)
        ):
            workflow.create_port(
                (
                    InterWorkflowJobPort
                    if isinstance(port, JobPort)
                    else InterWorkflowPort
                ),
                port.name,
            )


class RollbackRecoveryPolicy(RecoveryPolicy):
    async def _sync_workflows(
        self,
        acquired_jobs: MutableSequence[str],
        failed_job: str,
        job_names: MutableSet[str],
        job_tokens: MutableSequence[Token],
        mapper: GraphMapper,
        workflow: Workflow,
    ) -> None:
        for job_name in job_names:
            retry_request = self.context.failure_manager.get_request(job_name)
            job_name = retry_request.name
            await retry_request.lock.acquire()
            acquired_jobs.append(job_name)
            if (
                await self.context.failure_manager.is_recovered(job_name)
                == TokenAvailability.FutureAvailable
            ):
                retry_request.lock.release()
                acquired_jobs.remove(job_name)
                job_token = get_job_token(job_name, job_tokens)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Aligning rollbacks for failed job {failed_job}: Job {job_name} is currently executing."
                    )
                available_tokens = []
                for token_id in (
                    mapper.dag_tokens.successors(job_token.persistent_id)
                    if mapper.dag_tokens.contains(job_token.persistent_id)
                    else []
                ):
                    mapper.move_token_to_root(token_id)
                    available_tokens.append(token_id)
                for token_id in (
                    # Some tokens could be discarded by the `move_token_to_root` method
                    curr_token
                    for curr_token in available_tokens
                    if curr_token in mapper.token_instances.keys()
                ):
                    new_port = _get_recovery_port(
                        token_id, mapper, retry_request.workflow, workflow
                    )
                    cast(
                        InterWorkflowPort,
                        retry_request.workflow.ports[new_port.name],
                    ).add_inter_port(
                        port=new_port,
                        boundary_tags=[job_token.tag],
                        boundary_action=BoundaryAction.PROPAGATE,
                    )
            else:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Aligning rollbacks for failed job {failed_job}: Job {job_name} rollback"
                    )
                retry_request.workflow = workflow
                await self.context.failure_manager.update_request(job_name)

    async def recover(self, failed_job: Job, failed_step: Step) -> None:
        workflow = failed_step.workflow
        workflow_builder = WorkflowBuilder(deep_copy=False)
        new_workflow = await workflow_builder.load_workflow(
            workflow.context, workflow.persistent_id
        )
        # Retrieve tokens
        provenance = ProvenanceGraph(workflow.context)
        await provenance.build_graph(
            inputs=[
                *failed_job.inputs.values(),
                *(
                    p.token_list[0]
                    for p in failed_step.get_input_ports().values()
                    if isinstance(p, ConnectorPort)
                ),
                *(
                    get_job_token(failed_job.name, p.token_list)
                    for p in failed_step.get_input_ports().values()
                    if isinstance(p, JobPort)
                ),
            ]
        )
        mapper = await create_graph_mapper(self.context, provenance)
        # Synchronize between multiple recovery workflows
        job_tokens = list(
            filter(lambda t: isinstance(t, JobToken), mapper.token_instances.values())
        )
        acquired_jobs = []
        try:
            await self._sync_workflows(
                acquired_jobs=acquired_jobs,
                failed_job=failed_job.name,
                job_names={*(t.value.name for t in job_tokens), failed_job.name},
                job_tokens=job_tokens,
                mapper=mapper,
                workflow=new_workflow,
            )
            if mapper.dag_tokens.empty():
                raise FailureHandlingException(
                    f"Impossible to recover {failed_job.name}: empty token graph"
                )
            # Populate new workflow
            step_ids = await mapper.get_step_ids(failed_step.output_ports.values())
            await _populate_workflow(
                failed_step=failed_step,
                step_ids=step_ids,
                workflow=new_workflow,
                workflow_builder=workflow_builder,
            )
        finally:
            for job_name in acquired_jobs:
                if (
                    job_lock := self.context.failure_manager.get_request(job_name).lock
                ).locked():
                    job_lock.release()
        await _inject_tokens(
            failed_job=failed_job,
            failed_step=failed_step,
            mapper=mapper,
            workflow=new_workflow,
        )
        # Resume steps
        for step in new_workflow.steps.values():
            await step.restore(
                on_tokens={
                    port.name: [
                        mapper.token_instances[token_id]
                        for token_id in mapper.port_tokens[port.name]
                        if not mapper.token_availability[token_id]
                    ]
                    for port in step.get_output_ports().values()
                    if port.name in mapper.port_tokens.keys()
                }
            )
        # Execute
        if len(new_workflow.steps) == 0:
            raise FailureHandlingException("Empty recovery workflow")
        await new_workflow.save(new_workflow.context)
        executor = StreamFlowExecutor(new_workflow)
        await executor.run()
