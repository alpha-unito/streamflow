from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import MutableMapping, MutableSequence, MutableSet
from importlib.resources import files
from typing import cast

from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import FailureHandlingException
from streamflow.core.recovery import FailureManager, RecoveryRequest, recoverable
from streamflow.core.utils import get_tag
from streamflow.core.workflow import Job, Port, Status, Step, Token, Workflow
from streamflow.log_handler import logger
from streamflow.persistence.loading_context import WorkflowBuilder
from streamflow.recovery.utils import GraphMapper, ProvenanceGraph, create_graph_mapper
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
) -> Port:
    port_name = next(
        curr_port
        for curr_port, curr_tokens in mapper.port_tokens.items()
        if token_id in curr_tokens
    )
    if port_name not in recovery_workflow.ports.keys() or not isinstance(
        recovery_workflow.ports[port_name], InterWorkflowPort
    ):
        return recovery_workflow.create_port(
            cls=type(original_workflow.ports[port_name]),
            name=port_name,
        )
    else:
        return recovery_workflow.ports[port_name]


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


class RollbackFailureManager(FailureManager):
    def __init__(
        self,
        context: StreamFlowContext,
        max_retries: int | None = None,
        retry_delay: int | None = None,
    ):
        super().__init__(context)
        self.max_retries: int | None = max_retries
        self.retry_delay: int | None = retry_delay
        self._retry_requests: MutableMapping[str, RecoveryRequest] = {}

    @recoverable
    async def _do_handle_failure(self, job: Job, step: Step) -> None:
        # Delay rescheduling to manage temporary failures (e.g. connection lost)
        if self.retry_delay is not None:
            await asyncio.sleep(self.retry_delay)
        try:
            await self._recover(job, step)
            if logger.isEnabledFor(logging.INFO):
                logger.info(f"COMPLETED Recovery execution of failed job {job.name}")
        except FailureHandlingException as e:
            if logger.isEnabledFor(logging.INFO):
                logger.info(f"FAILED Recovery execution of failed job {job.name}")
            raise e

    async def _synchronize_workflows(
        self,
        failed_job: str,
        job_tokens: MutableSequence[Token],
        mapper: GraphMapper,
        retry_requests: MutableSequence[RecoveryRequest],
        workflow: Workflow,
    ) -> None:
        for retry_request in retry_requests:
            job_name = retry_request.name
            if await self.context.failure_manager.is_recovering(job_name):
                job_token = get_job_token(job_name, job_tokens)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Synchronizing rollbacks for failed job {failed_job}: Job {job_name} is currently executing."
                    )
                available_tokens = set()
                for token_id in (
                    mapper.dag_tokens.successors(job_token.persistent_id)
                    if mapper.dag_tokens.contains(job_token.persistent_id)
                    else []
                ):
                    mapper.move_token_to_root(token_id)
                    available_tokens.add(token_id)
                # Some tokens could be discarded by the `move_token_to_root` method
                for token_id in available_tokens & mapper.token_instances.keys():
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
                        f"Synchronizing rollbacks for failed job {failed_job}: Job {job_name} rollback"
                    )
                await self.context.failure_manager.update_request(job_name)
                retry_request.workflow = workflow

    async def _recover(self, failed_job: Job, failed_step: Step) -> None:
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
        retry_requests = [
            self.context.failure_manager.get_request(job_name)
            for job_name in {*(t.value.name for t in job_tokens), failed_job.name}
        ]
        async with contextlib.AsyncExitStack() as exit_stack:
            for request in sorted(retry_requests, key=id):
                await exit_stack.enter_async_context(request.lock)
            await self._synchronize_workflows(
                failed_job=failed_job.name,
                job_tokens=job_tokens,
                mapper=mapper,
                retry_requests=retry_requests,
                workflow=new_workflow,
            )
            if mapper.dag_tokens.empty():
                raise FailureHandlingException(
                    f"Impossible to recover {failed_job.name}: empty token graph"
                )
            # Populate new workflow
            await _populate_workflow(
                failed_step=failed_step,
                step_ids=await mapper.get_step_ids(failed_step.output_ports.values()),
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

    async def close(self) -> None:
        pass

    def get_request(self, job_name: str) -> RecoveryRequest:
        if job_name in self._retry_requests.keys():
            return self._retry_requests[job_name]
        else:
            return self._retry_requests.setdefault(job_name, RecoveryRequest(job_name))

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("rollback_failure_manager.json")
            .read_text("utf-8")
        )

    async def is_recovering(self, job_name: str) -> bool:
        return self.context.scheduler.get_allocation(job_name).status in (
            Status.ROLLBACK,
            Status.RUNNING,
            Status.FIREABLE,
        )

    async def recover(self, job: Job, step: Step, exception: BaseException) -> None:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"Handling {type(exception).__name__} failure for job {job.name}"
            )
        await self.context.scheduler.notify_status(job.name, Status.RECOVERY)
        await self._do_handle_failure(job, step)

    async def notify(
        self,
        output_port: str,
        output_token: Token,
        job_token: JobToken | None = None,
    ) -> None:
        pass

    async def update_request(self, job_name: str) -> None:
        retry_request = self._retry_requests[job_name]
        if self.max_retries is None or retry_request.version < self.max_retries:
            retry_request.version += 1
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"Updated Job {job_name} after {retry_request.version} retries (max retries {self.max_retries})"
                )
            await self.context.scheduler.notify_status(job_name, Status.ROLLBACK)
        else:
            logger.error(
                f"FAILED Job {job_name} {retry_request.version} times. Execution aborted"
            )
            raise FailureHandlingException(
                f"FAILED Job {job_name} {retry_request.version} times. Execution aborted"
            )


class DummyFailureManager(FailureManager):
    async def close(self) -> None:
        pass

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("dummy_failure_manager.json")
            .read_text("utf-8")
        )

    def get_request(self, job_name: str) -> RecoveryRequest:
        pass

    async def is_recovering(self, job_name: str) -> bool:
        return False

    async def recover(self, job: Job, step: Step, exception: BaseException) -> None:
        if logger.isEnabledFor(logging.WARNING):
            logger.warning(
                f"Job {job.name} failure can not be recovered. Failure manager is not enabled."
            )
        raise exception

    async def notify(
        self,
        output_port: str,
        output_token: Token,
        job_token: JobToken | None = None,
    ) -> None:
        pass

    async def update_request(self, job_name: str) -> None:
        pass
