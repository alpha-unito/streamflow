from __future__ import annotations

import asyncio
import logging
from collections.abc import MutableSequence, MutableSet
from typing import NamedTuple, cast

from streamflow.core.exception import FailureHandlingException
from streamflow.core.recovery import RecoveryPolicy
from streamflow.core.utils import get_job_tag, get_tag
from streamflow.core.workflow import Job, Port, Status, Step, Token, Workflow
from streamflow.log_handler import logger
from streamflow.persistence.loading_context import WorkflowBuilder
from streamflow.recovery.utils import (
    GraphMapper,
    ProvenanceGraph,
    create_graph_mapper,
)
from streamflow.token_printer import dag_workflow, graph_figure
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


class AlignInfo(NamedTuple):
    port_name: str
    workflow: Workflow
    running_port: Port
    new_port: Port


async def _inject_tokens(
    failed_job: Job,
    failed_step: Step,
    mapper: GraphMapper,
    workflow: Workflow,
    align_ports,
) -> None:
    workflow_output_ports = {v: k for k, v in failed_step.output_ports.items()}

    if {p.port_name for p in align_ports} - mapper.port_tokens.keys():
        raise FailureHandlingException(
            "Alignment ports are not present in the recovery workflow"
        )

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
                logger.debug(
                    f"Injecting token {token.persistent_id} {token.tag} of port {port.name}"
                )
            port.put(token)


async def _populate_workflow(
    failed_step: Step,
    step_ids: MutableSequence[int],
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

    async def _align_workflow_ports(
        self,
        acquired_jobs: MutableSequence[str],
        failed_job: str,
        job_names: MutableSet[str],
        job_tokens: MutableSequence[Token],
        mapper: GraphMapper,
        workflow: Workflow,
    ) -> MutableSequence[str]:
        align_ports = []
        for job_name in job_names:
            retry_request = self.context.failure_manager.get_request(job_name)
            job_name = retry_request.name
            await retry_request.lock.acquire()
            acquired_jobs.append(job_name)
            if self.context.scheduler.get_allocation(job_name).status in (
                Status.ROLLBACK,
                Status.RUNNING,
                Status.FIREABLE,
            ):
                retry_request.lock.release()
                acquired_jobs.remove(job_name)
                job_token = get_job_token(job_name, job_tokens)
                logger.debug(
                    f"Aligning rollbacks for failed job {failed_job}: Job {job_name} is currently executing."
                )
                inter_wf_ports = []
                for port_name in mapper.get_output_ports(job_token):
                    if port_name in retry_request.workflow.ports.keys():
                        inter_wf_ports.append(port_name)
                        # port_name can be deleted from the previous iteration of move_token_to_root
                        # Adding a topological sorting, can resolve the problem?
                        for token_id in list(mapper.port_tokens.get(port_name, [])):
                            if mapper.token_instances[token_id].tag == job_token.tag:
                                mapper.move_token_to_root(token_id)
                for port_name in (
                    # Filter the port still necessary
                    # Some ports can be discarded given by the move token to root method
                    pn
                    for pn in inter_wf_ports
                    if pn in mapper.port_tokens.keys()
                ):
                    if port_name not in workflow.ports.keys() or not isinstance(
                        workflow.ports[port_name], InterWorkflowPort
                    ):
                        new_port = workflow.create_port(
                            cls=type(retry_request.workflow.ports[port_name]),
                            name=port_name,
                        )
                    else:
                        new_port = workflow.ports[port_name]

                    align_ports.append(
                        AlignInfo(
                            port_name=port_name,
                            workflow=workflow,
                            running_port=retry_request.workflow.ports[port_name],
                            new_port=new_port,
                        )
                    )
                    cast(
                        InterWorkflowPort,
                        retry_request.workflow.ports[port_name],
                    ).add_inter_port(
                        port=new_port,
                        boundary_tags=[get_job_tag(job_token.value.name)],
                        boundary_action=(
                            BoundaryAction.PROPAGATE  # | BoundaryAction.TERMINATE
                        ),
                    )
            else:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Aligning rollbacks for failed job {failed_job}: Job {job_name} rollback"
                    )
                retry_request.workflow = workflow
                await self.context.failure_manager.update_request(job_name)
                # if retry_request.workflow is not None:
                #     for p in retry_request.workflow.ports.values():
                #         if isinstance(p, InterWorkflowPort):
                #             for pending_rule in p.pending_rules():
                #                 if p.name not in workflow.ports.keys():
                #                     new_port = workflow.create_port(
                #                         type(p), name=p.name
                #                     )
                #                 else:
                #                     new_port = workflow.ports[p.name]
                #                 new_port.add_inter_port(
                #                     port=pending_rule.port,
                #                     boundary_tags=pending_rule.tags,
                #                     boundary_action=pending_rule.action,
                #                 )
        return [a for a in align_ports if a.port_name in mapper.port_tokens.keys()]

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

        #######################################
        import datetime
        import os

        figure_name = f"/home/alberto/Work/Repositories/streamflow/dev/plots/{datetime.datetime.now().timestamp()}_{failed_job.name[1:].replace(os.sep, '.')}"
        graph_figure(mapper.dag_tokens,f"{figure_name}-pre-token")
        #######################################

        logger.info(f"RECOVER {failed_job.name}: {mapper.dag_tokens}")
        logger.info(f"RECOVER {failed_job.name}: {mapper.dcg_ports}")
        # Align simultaneous recovery workflow
        job_tokens = list(
            filter(lambda t: isinstance(t, JobToken), mapper.token_instances.values())
        )
        acquired_jobs = []
        try:
            align_ports = await self._align_workflow_ports(
                acquired_jobs=acquired_jobs,
                job_names={*(t.value.name for t in job_tokens), failed_job.name},
                job_tokens=job_tokens,
                mapper=mapper,
                workflow=new_workflow,
                failed_job=failed_job.name,
            )
            logger.info(f"Failed job {failed_job.name} alignment completed")
            if failed_job.name not in acquired_jobs:
                raise FailureHandlingException(
                    "DEBUG. It is possible that the failed job is not the list. Raised exception just to check the status"
                )
                # logger.info("Failed step not in the recovery workflow")
            # Populate new workflow
            step_ids = await mapper.get_step_ids(failed_step.output_ports.values())
            logger.info(f"Failed job {failed_job.name} retrieved step ids")
            await _populate_workflow(
                failed_step=failed_step,
                step_ids=step_ids,
                workflow=new_workflow,
                workflow_builder=workflow_builder,
            )
            logger.info(f"Failed job {failed_job.name} populated recovery workflow")
        finally:
            for job_name in acquired_jobs:
                if (
                    job_lock := self.context.failure_manager.get_request(job_name).lock
                ).locked():
                    job_lock.release()

        #######################################
        graph_figure(
            mapper.dag_tokens,
            f"{figure_name}-post-token",
        )
        dag_workflow(new_workflow,figure_name)
        #######################################

        await _inject_tokens(
            failed_job=failed_job,
            failed_step=failed_step,
            mapper=mapper,
            workflow=new_workflow,
            align_ports=align_ports,
        )
        logger.info(f"Failed job {failed_job.name} injected tokens")
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
        logger.info(f"Failed job {failed_job.name} restored steps")

        if len(new_workflow.steps) == 0:
            raise FailureHandlingException("Empty recovery workflow")
        try:
            await new_workflow.save(new_workflow.context)
            logger.info(
                f"Failed job {failed_job.name} runs {len(new_workflow.steps)} steps"
            )
            executor = StreamFlowExecutor(new_workflow)
            await executor.run()
        except BaseException as e:
            logger.info(f"Error {e}: {type(e)}")
            for step in new_workflow.steps.values():
                if step.status != Status.COMPLETED:
                    logger.debug(
                        f"Recovery step {step.name} exits with status {step.status.name}"
                    )
            raise
