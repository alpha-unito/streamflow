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
from streamflow.core.workflow import Job, Status, Step, Token, Workflow
from streamflow.cwl.step import CWLConditionalStep
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
from streamflow.workflow.step import ConditionalStep, GatherStep, LoopCombinatorStep
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
        assert set(new_workflow.ports.keys()) == set(failed_step.output_ports.values())
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
        assert not any(
            isinstance(token, (TerminationToken, IterationTerminationToken))
            for token in token_list
        )
        port = new_workflow.ports[port_name]
        added_inter_port = False
        if (
            isinstance(port, InterWorkflowPort)
            # and any(
            #     isinstance(s, LoopCombinatorStep) for s in port.get_output_steps()
            # )
        ):
            added_inter_port = True
            if (
                not isinstance(port, InterWorkflowJobPort)
                and port.name not in failed_step_output_ports
            ):
                if False and any(
                    isinstance(s, GatherStep) for s in port.get_output_steps()
                ):
                    assert len(port.get_output_steps()) == 1
                    g_step = next(iter(port.get_output_steps()))
                    b_tag = max(
                        [
                            mapper.token_instances[token_id].tag
                            for token_id in mapper.port_tokens[port_name]
                        ],
                        key=cmp_to_key(lambda x, y: compare_tags(x, y)),
                    )
                    s_port = cast(GatherStep, g_step).get_size_port()
                    num_tokens = max(
                        [
                            mapper.token_instances[token_id]
                            for token_id in mapper.port_tokens[s_port.name]
                        ],
                        key=cmp_to_key(lambda x, y: compare_tags(x.tag, y.tag)),
                    )
                    b_tag = ".".join(
                        [*b_tag.split(".")[:-1], str(num_tokens.value - 1)]
                    )
                    # a_tag = max(
                    #     [
                    #         mapper.token_instances[token_id].tag
                    #         for token_id in mapper.port_tokens[port_name]
                    #     ],
                    #     key=cmp_to_key(lambda x, y: compare_tags(x, y)),
                    # )
                    pass
                else:
                    b_tag = max(
                        [
                            mapper.token_instances[token_id].tag
                            for token_id in mapper.port_tokens[port_name]
                        ],
                        key=cmp_to_key(lambda x, y: compare_tags(x, y)),
                    )
                port.add_inter_port(
                    port,
                    boundary_tag=b_tag,
                    termination_type=TerminationType.PROPAGATE_AND_TERMINATE,
                )
        for token in token_list:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"Injecting token {token.persistent_id} {token.tag} of port {port.name} ({type(port)})"
                )
            port.put(token)
        if not added_inter_port:
            port.put(TerminationToken(Status.COMPLETED))
    logger.info("Injected all tokens")


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
        sync_port_names = []
        job_names = await self._sync_workflows(
            job_names={*(t.value.name for t in job_tokens), failed_job.name},
            job_tokens=job_tokens,
            mapper=mapper,
            workflow=new_workflow,
            sync_port_names=sync_port_names,
        )
        # Populate new workflow
        steps = await mapper.get_port_and_step_ids(failed_step.output_ports.values())
        await _populate_workflow(
            steps, failed_step, new_workflow, workflow_builder, failed_job
        )
        for job_name in job_names:
            self.context.failure_manager.get_request(job_name).workflow_ready.set()

        # import datetime
        # import os
        #
        # from streamflow.token_printer import dag_workflow
        #
        # dag_workflow(
        #     new_workflow,
        #     title=posixpath.join(
        #         os.getcwd(),
        #         "dev",
        #         str(datetime.datetime.now()).replace(" ", "_").replace(":", "."),
        #         "wf" + failed_job.name.replace(posixpath.sep, "."),
        #     ),
        # )
        logger.info(
            f"Failed step {failed_job.name} has output ports: {[p for p in failed_step.output_ports.values()]}"
        )
        await _inject_tokens(
            mapper, new_workflow, [p for p in failed_step.output_ports.values()]
        )
        # Resume steps
        skip_ports = []
        for s in new_workflow.steps.values():
            if isinstance(s, ConditionalStep):
                # DEBUG: CWL dependent implementation
                skip_ports.extend(cast(CWLConditionalStep, s).skip_ports.values())
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
            # DEBUG
            # Some ports do not have a termination token because they can have
            # available tokens and must wait until a recovery step of another
            # recovery workflow generates the missing token.
            for port in (
                p
                for p in new_workflow.ports.values()
                if len(p.get_input_steps()) == 0
                and p.name not in sync_port_names
                and p.name not in skip_ports
            ):
                if len(port.token_list) == 0:
                    logger.info(f"Port {port.name} has no tokens")
                    raise FailureHandlingException(f"Port {port.name} has no tokens")
                # elif len(port.token_list) == 1:
                #     logger.info(f"Port {port.name} has 1 token")
                #     raise FailureHandlingException(f"Port {port.name} has 1 token")
            for p in new_workflow.ports.values():
                if len(steps := p.get_input_steps()) == 1:
                    for s in steps:
                        if "back-prop" in s.name and len(p.token_list) == 0:
                            logger.debug(
                                f"Step {s.name} has no input token in its input port {p.name}"
                            )
        if [
            p
            for p in failed_step.get_output_ports().values()
            if isinstance(p, InterWorkflowPort)
            and next(
                (
                    v
                    for v in p.boundaries.values()
                    if v[0] is p and v[1] == TerminationType.PROPAGATE_AND_TERMINATE
                ),
                None,
            )
        ]:
            pass
        if [
            p
            for p in new_workflow.steps[failed_step.name].get_output_ports().values()
            if isinstance(p, InterWorkflowPort)
            and next(
                (
                    v
                    for v in p.boundaries.values()
                    if v[0] is p and v[1] == TerminationType.PROPAGATE_AND_TERMINATE
                ),
                None,
            )
        ]:
            pass
        return new_workflow

    async def _sync_workflows(
        self,
        job_names: MutableSet[str],
        job_tokens: MutableSequence[Token],
        mapper: GraphMapper,
        workflow: Workflow,
        sync_port_names: MutableSequence[str],
    ) -> MutableSequence[str]:
        new_job_names = []
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
                            f"Synchronizing rollbacks: Job {job_name} is waiting for the rollback workflow to be ready."
                        )
                    else:
                        logger.debug(
                            f"Synchronizing rollbacks: Job {job_name} is currently executing."
                        )
                else:
                    is_wf_ready = True
                await retry_request.workflow_ready.wait()
                if logger.isEnabledFor(logging.DEBUG) and not is_wf_ready:
                    logger.debug(
                        f"Synchronizing rollbacks: Job {job_name} has resumed after the rollback workflow is ready."
                    )
                port_name = await mapper.get_schedule_port_name(job_token)
                cast(
                    InterWorkflowJobPort, retry_request.workflow.ports[port_name]
                ).add_inter_port(
                    workflow.create_port(cls=InterWorkflowJobPort, name=port_name),
                    boundary_tag=get_job_tag(job_token.value.name),
                    termination_type=TerminationType.PROPAGATE_AND_TERMINATE,
                )
                # Synchronized schedule step
                mapper.move_token_to_root(job_token.persistent_id)
                sync_port_names.append(port_name)
                # TODO: Handle the synchronous execution step if it is present in both the
                #  running recovery workflow and the current recovery workflow
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
                retry_request.workflow_ready.clear()
                new_job_names.append(job_name)
        return new_job_names

    async def recover(
        self, failed_job: Job, failed_step: Step, counter_wf: int
    ) -> None:
        # Create recover workflow
        new_workflow = await self._recover_workflow(failed_job, failed_step)
        # import os
        # new_steps = {}
        # for s in new_workflow.steps.values():
        #     s.name = str(os.path.join(s.name, str(counter_wf)))
        #     new_steps[s.name] = s
        # new_workflow.steps = new_steps
        # Execute new workflow
        await _execute_recover_workflow(new_workflow, failed_step)
        for p in failed_step.get_output_ports().values():
            if get_job_tag(failed_job.name) not in (t.tag for t in p.token_list):
                # DEBUG: Il tag può non essere nella port di recupero SE
                #   è una InterWorkflowPort in cui tra i boundaries contiene se stessa
                #   e la cui condizione di terminazione è Terminate (o detto in altri modi non è Propagate)
                if not (
                    isinstance(p, InterWorkflowPort)
                    and next(
                        ip[1]
                        for ip in p.boundaries[get_job_tag(failed_job.name)]
                        if ip[0] is p
                    )
                    == TerminationType.TERMINATE
                ):
                    pass
        return
