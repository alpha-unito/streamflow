from __future__ import annotations

import asyncio
import logging
import posixpath
from collections.abc import Iterable, MutableSequence, MutableSet
from typing import cast

from streamflow.core.exception import FailureHandlingException
from streamflow.core.recovery import RecoveryPolicy
from streamflow.core.utils import compare_tags, get_job_tag, get_tag
from streamflow.core.workflow import Job, Status, Step, Token, Workflow
from streamflow.log_handler import logger
from streamflow.persistence.loading_context import WorkflowBuilder
from streamflow.recovery.utils import (
    DirectGraph,
    GraphMapper,
    ProvenanceGraph,
    TokenAvailability,
    create_graph_mapper,
)
from streamflow.token_printer import dag_workflow
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.port import (
    ConnectorPort,
    FilterTokenPort,
    InterWorkflowJobPort,
    InterWorkflowPort,
    JobPort,
)
from streamflow.workflow.step import LoopCombinatorStep, ScatterStep
from streamflow.workflow.token import (
    IterationTerminationToken,
    JobToken,
    TerminationToken,
)
from streamflow.workflow.utils import get_job_token


async def _execute_recover_workflow(new_workflow: Workflow, failed_step: Step) -> None:
    if len(new_workflow.steps) == 0:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"Workflow {new_workflow.name} is empty. "
                f"Waiting output ports {list(failed_step.output_ports.values())}"
            )
        if set(new_workflow.ports.keys()) != set(failed_step.output_ports.values()):
            raise FailureHandlingException("Recovered workflow construction invalid")
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
        await new_workflow.save(new_workflow.context)
        executor = StreamFlowExecutor(new_workflow)
        await executor.run()


async def _inject_tokens(
    mapper: GraphMapper, inter_ports: MutableSequence[str], new_workflow: Workflow
) -> None:
    for port_name in mapper.dcg_port[DirectGraph.ROOT]:
        token_list = sorted(
            [
                mapper.token_instances[token_id]
                for token_id in mapper.port_tokens[port_name]
                if token_id not in (DirectGraph.ROOT, DirectGraph.LEAF)
                and mapper.token_available[token_id]
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
        if any(isinstance(s, LoopCombinatorStep) for s in port.get_output_steps()):
            pass
        # if (
        #     any(isinstance(s, LoopCombinatorStep) for s in port.get_output_steps())
        #     and len(port.get_input_steps()) >= 3
        # ):
        #     if logger.isEnabledFor(logging.DEBUG):
        #         logger.debug(f"Injecting termination token on port {port.name}")
        #     port.put(TerminationToken(Status.SKIPPED))
        if len(port.token_list) > 0 and len(port.token_list) == len(
            mapper.port_tokens[port_name]
        ):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Injecting termination token on port {port.name}")
            port.put(TerminationToken(Status.SKIPPED))
    for port_ in new_workflow.ports.values():
        if (
            port_.name not in inter_ports
            and len(port_.get_input_steps()) == 0
            and not any(isinstance(t, TerminationToken) for t in port_.token_list)
            and len(port_.get_output_steps()) > 0
        ):
            raise FailureHandlingException(
                f"Port {port_.name} is an input, but it does not have a termination token."
            )


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
    # Add the failed step to the new workflow
    await workflow_builder.load_step(
        new_workflow.context,
        failed_step.persistent_id,
    )
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
            port, boundary_tag=get_tag(failed_job.inputs.values()), terminate=False
        )


async def _set_step_states(mapper: GraphMapper, new_workflow: Workflow) -> None:
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
        elif isinstance(step, LoopCombinatorStep):
            smallest_tag = None
            for p in step.get_input_ports().values():
                if p.token_list:
                    if smallest_tag is None:
                        smallest_tag = p.token_list[0].tag
                    elif compare_tags(p.token_list[0].tag, smallest_tag) > 0:
                        smallest_tag = p.token_list[0].tag
            await cast(LoopCombinatorStep, step).set_iteration(tag=smallest_tag)


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
        inter_ports, job_names = await self._sync_workflows(
            job_names={*(t.value.name for t in job_tokens), failed_job.name},
            job_tokens=job_tokens,
            mapper=mapper,
            workflow=new_workflow,
        )
        # Populate new workflow
        steps = await mapper.get_port_and_step_ids(failed_step.output_ports.values())
        await _populate_workflow(
            steps, failed_step, new_workflow, workflow_builder, failed_job
        )
        for job_name in job_names:
            self.context.failure_manager.get_request(job_name).workflow_ready.set()
        await _inject_tokens(mapper, inter_ports, new_workflow)
        dag_workflow(failed_step.workflow, "ft-original-workflow")
        dag_workflow(new_workflow, "ft-new-workflow")
        await _set_step_states(mapper, new_workflow)
        return new_workflow

    async def _sync_workflows(
        self,
        job_names: MutableSet[str],
        job_tokens: MutableSequence[Token],
        mapper: GraphMapper,
        workflow: Workflow,
    ) -> tuple[MutableSequence[str], MutableSequence[str]]:
        inter_ports, new_job_names = [], []
        for job_name in job_names:
            retry_request = self.context.failure_manager.get_request(job_name)
            if (
                is_available := await self.context.failure_manager.is_recovered(
                    job_name
                )
            ) == TokenAvailability.FutureAvailable:
                job_token = get_job_token(job_name, job_tokens)
                # `retry_request` represents the currently running job.
                # `job_token` refers to the token that needs to be removed from the graph,
                # as the workflow depends on the already running job.
                if logger.isEnabledFor(logging.DEBUG):
                    if not (is_wf_ready := retry_request.workflow_ready.is_set()):
                        logger.debug(
                            f"Synchronizing rollbacks: Job '{job_name}' is waiting for the rollback workflow to be ready."
                        )
                    else:
                        logger.debug(
                            f"Synchronizing rollbacks: Job '{job_name}' is currently executing."
                        )
                else:
                    is_wf_ready = True
                await retry_request.workflow_ready.wait()
                if logger.isEnabledFor(logging.DEBUG) and not is_wf_ready:
                    logger.debug(
                        f"Synchronizing rollbacks: Job '{job_name}' has resumed after the rollback workflow is ready."
                    )
                for port_name in await mapper.get_output_ports(job_token):
                    if port_name in retry_request.workflow.ports.keys():
                        cast(
                            InterWorkflowPort, retry_request.workflow.ports[port_name]
                        ).add_inter_port(
                            workflow.create_port(cls=InterWorkflowPort, name=port_name),
                            boundary_tag=get_job_tag(job_token.value.name),
                            terminate=True,
                        )
                        inter_ports.append(port_name)
                # Remove tokens that will be recovered in other workflows
                for token_id in await mapper.get_output_tokens(job_token.persistent_id):
                    mapper.remove_token(token_id, preserve_token=True)
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
                        mapper.remove_token(
                            new_token.persistent_id, preserve_token=True
                        )
            else:
                await self.context.failure_manager.update_request(job_name)
                retry_request.workflow = workflow
                retry_request.workflow_ready.clear()
                new_job_names.append(job_name)
        return inter_ports, new_job_names

    async def recover(self, failed_job: Job, failed_step: Step) -> None:
        # Create recover workflow
        new_workflow = await self._recover_workflow(failed_job, failed_step)
        # Execute new workflow
        await _execute_recover_workflow(new_workflow, failed_step)
