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
    mapper: GraphMapper,
    new_workflow: Workflow,
    failed_step_output_ports: MutableSequence[str],
) -> None:
    for port_name in mapper.port_tokens.keys():
        token_list = sorted(
            [
                mapper.token_instances[token_id]
                for token_id in mapper.port_tokens[port_name]
                if mapper.token_available[token_id]
            ],
            key=lambda x: x.tag,
        )
        if len(
            tags := {(token.persistent_id, token.tag) for token in token_list}
        ) != len(token_list):
            raise FailureHandlingException(
                f"Port {port_name} has multiple tokens with same tag (id, tag): {tags}"
            )
        port = new_workflow.ports[port_name]

        # TerminationType.PROPAGATE_AND_TERMINATE
        # The inter port propagates termination upon receiving a token
        # with a tag matching the boundary tag, after propagating the
        # received token. The boundary tag is defined as the largest
        # tag within the mapper.
        #
        # There are some exceptions:
        # - The propagate-and-terminate condition is not added to the
        #   output ports of the `ScheduleStep` because JobTokens lack
        #   tags. Since they all use tag 0, enabling this condition
        #   would trigger termination as soon as the first token is
        #   generated.
        # - The condition is not added to the output ports of a failed
        #   step because they are handled differently. They must
        #   terminate themselves and propagate the token to the port
        #   of the original failed step.
        # - The input ports of the `GatherStep` are handled differently
        #   because if the token with the maximum tag is already
        #   available as an input token, the inter port triggers
        #   termination immediately. This bypasses the recovery process
        #   for any missing or lost tokens.
        if (
            isinstance(port, InterWorkflowPort)
            and not isinstance(port, InterWorkflowJobPort)
            and port.name not in failed_step_output_ports
            and not any(
                isinstance(s, GatherStep) and s.input_ports["__size__"] != port.name
                for s in port.get_output_steps()
            )
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
                termination_type=TerminationType.PROPAGATE | TerminationType.TERMINATE,
            )
        for token in token_list:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"Injecting token {token.persistent_id} {token.tag} of port {port.name} ({type(port)})"
                )
            port.put(token)


async def _populate_workflow(
    failed_job: Job,
    failed_step: Step,
    step_ids: Iterable[int],
    new_workflow: Workflow,
    workflow_builder: WorkflowBuilder,
) -> None:
    await asyncio.gather(
        *(
            asyncio.create_task(
                workflow_builder.load_step(new_workflow.context, step_id)
            )
            for step_id in step_ids
        )
    )
    # Add the failed step to the new workflow
    await workflow_builder.load_step(new_workflow.context, failed_step.persistent_id)
    # Instantiate ports that can transfer tokens between workflows
    for port in new_workflow.ports.values():
        if not isinstance(
            port, (ConnectorPort, InterWorkflowJobPort, InterWorkflowPort)
        ):
            new_workflow.create_port(
                (
                    InterWorkflowJobPort
                    if isinstance(port, JobPort)
                    else InterWorkflowPort
                ),
                port.name,
            )
    for port in failed_step.get_output_ports().values():
        cast(InterWorkflowPort, new_workflow.ports[port.name]).add_inter_port(
            port,
            boundary_tag=get_tag(failed_job.inputs.values()),
            termination_type=TerminationType.PROPAGATE,
        )
        cast(InterWorkflowPort, new_workflow.ports[port.name]).add_inter_port(
            new_workflow.ports[port.name],
            boundary_tag=get_tag(failed_job.inputs.values()),
            termination_type=TerminationType.TERMINATE,
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
            failed_job=failed_job,
            failed_step=failed_step,
            step_ids=steps,
            new_workflow=new_workflow,
            workflow_builder=workflow_builder,
        )
        await _inject_tokens(
            mapper, new_workflow, [p for p in failed_step.output_ports.values()]
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
