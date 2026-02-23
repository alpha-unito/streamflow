from __future__ import annotations

import asyncio
import logging
import posixpath
from collections.abc import Iterable, MutableSequence, MutableSet
from functools import cmp_to_key
from typing import cast

from streamflow.core.exception import FailureHandlingException
from streamflow.core.recovery import RecoveryPolicy
from streamflow.core.utils import compare_tags, get_job_tag, get_tag
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
    ConnectorPort,
    InterWorkflowJobPort,
    InterWorkflowPort,
    JobPort,
    TerminationType,
)
from streamflow.workflow.step import GatherStep
from streamflow.workflow.token import JobToken
from streamflow.workflow.utils import get_job_token


async def _execute_recover_workflow(new_workflow: Workflow, failed_step: Step) -> None:
    if len(new_workflow.steps) == 0:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"Workflow {new_workflow.name} is empty. "
                f"Waiting output ports {list(failed_step.output_ports.values())}"
            )
        if set(new_workflow.ports.keys()) != set(failed_step.output_ports.values()):
            raise FailureHandlingException(
                f"Recovery workflow for {failed_step.name} step has no all the output ports"
            )
        await asyncio.gather(
            *(
                asyncio.create_task(
                    new_workflow.ports[name].get(
                        posixpath.join(failed_step.name, dependency)
                    )
                )
                for name, dependency in failed_step.output_ports.items()
            )
        )
    else:
        try:
            await new_workflow.save(new_workflow.context)
            executor = StreamFlowExecutor(new_workflow)
            await executor.run()
        except BaseException as e:
            logger.info(f"Error {e}: {type(e)}")
            for step in new_workflow.steps.values():
                logger.debug(f"Step {step.name} exits with status {step.status.name}")
            raise


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
                    if mapper.token_available[token_id]
                ],
                key=lambda x: x.tag,
            )
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
                    failed_step.get_output_port(workflow_output_ports[port.name]),
                    boundary_tag=get_tag(failed_job.inputs.values()),
                    termination_type=TerminationType.PROPAGATE,
                )
                # Terminate execution in the recovery workflow
                cast(InterWorkflowPort, workflow.ports[port.name]).add_inter_port(
                    workflow.ports[port.name],
                    boundary_tag=get_tag(failed_job.inputs.values()),
                    termination_type=TerminationType.TERMINATE,
                )

            # Other InterWorkflowPorts have one default rule: when a token of
            # the boundary rule arrives, the activating token and a grateful
            # termination token are propagated in the recovery workflow.
            # The boundary tag is the largest tag within the mapper.

            # Exceptions:
            # - ScheduleStep output ports have no default rule because JobTokens
            #   lack tags (all use tag 0).
            # - GatherStep input ports have no default rule. This step requires
            #   all the input tokens, not just the token with the largest tags.
            #   Otherwise, if the largest tag is already available as
            #   an input token, the inter port triggers immediate termination.
            elif not isinstance(port, InterWorkflowJobPort) and not any(
                isinstance(s, GatherStep) and s.input_ports["__size__"] != port.name
                for s in port.get_output_steps()
            ):
                port.add_inter_port(
                    port,
                    boundary_tag=max(
                        [
                            mapper.token_instances[token_id].tag
                            for token_id in mapper.port_tokens[port_name]
                        ],
                        key=cmp_to_key(compare_tags),
                    ),
                    termination_type=(
                        TerminationType.PROPAGATE | TerminationType.TERMINATE
                    ),
                )
        # Inject tokens
        for token in token_list:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"Injecting token {token.persistent_id} {token.tag} of port {port.name} ({type(port)})"
                )
            port.put(token)


async def _populate_workflow(
    failed_step: Step,
    step_ids: Iterable[int],
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
    async def _recover_workflow(self, failed_job: Job, failed_step: Step) -> Workflow:
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
        await self._sync_workflows(
            {*(t.value.name for t in job_tokens), failed_job.name},
            job_tokens,
            mapper,
            new_workflow,
        )
        # Populate new workflow
        steps = await mapper.get_port_and_step_ids(failed_step.output_ports.values())
        await _populate_workflow(
            failed_step=failed_step,
            step_ids=steps,
            workflow=new_workflow,
            workflow_builder=workflow_builder,
        )
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
                        if not mapper.token_available[token_id]
                    ]
                    for port in step.get_output_ports().values()
                    if port.name in mapper.port_tokens.keys()
                }
            )
        return new_workflow

    async def _sync_workflows(
        self,
        job_names: MutableSet[str],
        job_tokens: MutableSequence[Token],
        mapper: GraphMapper,
        workflow: Workflow,
    ) -> None:
        for job_name in job_names:
            retry_request = self.context.failure_manager.get_request(job_name)
            if (
                is_available := await self.context.failure_manager.is_recovered(
                    job_name
                )
            ) == TokenAvailability.FutureAvailable:
                job_token = get_job_token(job_name, job_tokens)
                # The `retry_request` is the current job running, instead
                # the `job_token` is the token to remove in the graph because
                # the workflow will depend on the already running job
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Synchronize rollbacks: job {job_name} is running")
                # todo: create a unit test for this case
                for port_name in await mapper.get_output_ports(job_token):
                    if port_name in retry_request.workflow.ports.keys():
                        cast(
                            InterWorkflowJobPort,
                            retry_request.workflow.ports[port_name],
                        ).add_inter_port(
                            workflow.create_port(
                                cls=InterWorkflowJobPort, name=port_name
                            ),
                            boundary_tag=get_job_tag(job_token.value.name),
                            termination_type=(
                                TerminationType.PROPAGATE | TerminationType.TERMINATE
                            ),
                        )
                # Remove tokens recovered in other workflows
                for token_id in await mapper.get_output_tokens(job_token.persistent_id):
                    mapper.move_token_to_root(token_id)
            elif is_available == TokenAvailability.Available:
                job_token = get_job_token(job_name, job_tokens)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Synchronize rollbacks: job {job_token.value.name} output available"
                    )
                # Search execute token after job token, replace this token with job_req token.
                # Then remove all the prev tokens
                for port_name in await mapper.get_output_ports(job_token):
                    if port_name in retry_request.output_tokens.keys():
                        new_token = retry_request.output_tokens[port_name]
                        mapper.replace_token(
                            port_name,
                            new_token,
                            True,
                        )
                        mapper.move_token_to_root(new_token.persistent_id)
            else:
                await self.context.failure_manager.update_request(job_name)
                retry_request.workflow = workflow

    async def recover(self, failed_job: Job, failed_step: Step) -> None:
        # Create recover workflow
        new_workflow = await self._recover_workflow(failed_job, failed_step)
        # Execute new workflow
        await _execute_recover_workflow(new_workflow, failed_step)
