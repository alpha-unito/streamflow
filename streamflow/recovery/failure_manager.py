from __future__ import annotations

import asyncio
import logging
from collections.abc import MutableMapping, MutableSequence
from importlib.resources import files

from streamflow.core.command import CommandOutput
from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import FailureHandlingException, WorkflowException
from streamflow.core.recovery import FailureManager
from streamflow.core.utils import get_max_tag, increase_tag
from streamflow.core.workflow import Job, Status, Step, Token, Workflow
from streamflow.log_handler import logger
from streamflow.recovery.recovery import RollbackRecoveryPolicy
from streamflow.recovery.rollback_recovery import GraphMapper, TokenAvailability
from streamflow.recovery.utils import (
    PortRecovery,
    RetryRequest,
    execute_recover_workflow,
    get_output_tokens,
)
from streamflow.workflow.port import FilterTokenPort, InterWorkflowPort
from streamflow.workflow.token import JobToken, TerminationToken


class DefaultFailureManager(FailureManager):
    def __init__(
        self,
        context: StreamFlowContext,
        max_retries: int | None = None,
        retry_delay: int | None = None,
    ):
        super().__init__(context)
        self.max_retries: int = max_retries
        self.retry_delay: int | None = retry_delay
        self.retry_requests: MutableMapping[str, RetryRequest] = {}
        self.recoverable_tokens: MutableSequence[int] = []

    def _add_wait(
        self,
        job_name: str,
        port_name: str,
        workflow: Workflow,
        mapper: GraphMapper,
        port_recovery: PortRecovery | None = None,
    ) -> PortRecovery:
        if port_recovery is None:
            if (port := workflow.ports.get(port_name)) is not None:
                # todo: to check
                max_tag = (
                    get_max_tag(
                        {
                            mapper.token_instances[t_id]
                            for t_id in mapper.port_tokens.get(port_name, [])
                            if t_id > 0
                        }
                    )
                    or "0"
                )
                stop_tag = increase_tag(max_tag)
                port = InterWorkflowPort(
                    FilterTokenPort(
                        workflow,
                        port_name,
                        stop_tags=[stop_tag],
                    )
                )
            port_recovery = PortRecovery(port)
            self.retry_requests[job_name].queue.append(port_recovery)
        port_recovery.waiting_token += 1
        return port_recovery

    async def _do_handle_failure(self, job: Job, step: Step) -> None:
        # Delay rescheduling to manage temporary failures (e.g. connection lost)
        if self.retry_delay is not None:
            await asyncio.sleep(self.retry_delay)
        try:
            rollback = RollbackRecoveryPolicy(self.context)
            # Generate new workflow
            new_workflow = await rollback.recover_workflow(job, step)
            # Execute new workflow
            await execute_recover_workflow(new_workflow, step)
            if logger.isEnabledFor(logging.INFO):
                logger.info(f"COMPLETED Recovery execution of failed job {job.name}")
        # When receiving a FailureHandlingException, simply fail
        except FailureHandlingException as e:
            logger.exception(e)
            raise
        # Recovery exceptions
        except WorkflowException as e:
            logger.exception(e)
            await self.handle_exception(job, step, e)
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logger.exception(e)
            raise

    async def _increase_request(
        self, retry_request: RetryRequest, job_token: JobToken, workflow: Workflow
    ) -> None:
        job_name = job_token.value.name
        async with retry_request.lock:
            retry_request.is_running = True
            retry_request.job_token = None
            retry_request.token_output = {}
            retry_request.workflow = workflow
        if (
            self.max_retries is None
            or self.retry_requests[job_token.value.name].version < self.max_retries
        ):
            self.retry_requests[job_name].version += 1
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"Updated Job {job_name} at {self.retry_requests[job_name].version} times"
                )
            # Free resources scheduler
            await self.context.scheduler.notify_status(job_name, Status.ROLLBACK)
        else:
            logger.error(
                f"FAILED Job {job_name} {self.retry_requests[job_name].version} times. Execution aborted"
            )
            raise FailureHandlingException(
                f"FAILED Job {job_name} {self.retry_requests[job_name].version} times. Execution aborted"
            )

    async def _sync_running_jobs(
        self,
        job_token: JobToken,
        mapper: GraphMapper,
        workflow: Workflow,
        job_ports: MutableMapping[str, PortRecovery],
    ) -> None:
        job_name = job_token.value.name
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Synchronize rollbacks: job {job_name} is running")
        # todo: create a unit test for this case and check if it works well
        for output_port_name in await mapper.get_output_ports(job_token):
            port_recovery = self._add_wait(
                job_name,
                output_port_name,
                workflow,
                mapper,
                job_ports.get(job_name, None),
            )
            job_ports.setdefault(job_name, port_recovery)
            workflow.ports[port_recovery.port.name] = port_recovery.port
        # Remove tokens recovered in other workflows
        for token_id in await get_output_tokens(
            mapper.dag_tokens.succ(job_token.persistent_id),
            self.context,
        ):
            mapper.remove_token(token_id, preserve_token=True)

    async def close(self):
        pass

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("default_failure_manager.json")
            .read_text("utf-8")
        )

    async def handle_exception(
        self, job: Job, step: Step, exception: BaseException
    ) -> None:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"Handling {type(exception).__name__} failure for job {job.name}"
            )
        if job.name in self.retry_requests.keys():
            self.retry_requests[job.name].is_running = False
        await self._do_handle_failure(job, step)

    async def handle_failure(
        self, job: Job, step: Step, command_output: CommandOutput
    ) -> None:
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"Handling command failure for job {job.name}")
        if job.name in self.retry_requests.keys():
            self.retry_requests[job.name].is_running = False
        await self._do_handle_failure(job, step)

    async def is_recovered(self, token: JobToken) -> TokenAvailability:
        if request := self.retry_requests.get(token.value.name):
            async with request.lock:
                if request.is_running:
                    # todo: fix
                    # return TokenAvailability.FutureAvailable
                    return TokenAvailability.Unavailable
                elif len(request.token_output) > 0 and all(
                    await asyncio.gather(
                        *(
                            asyncio.create_task(t.is_available(self.context))
                            for t in request.token_output.values()
                        )
                    )
                ):
                    return TokenAvailability.Available
        return TokenAvailability.Unavailable

    async def notify(
        self,
        output_port: str,
        output_token: Token,
        recoverable: bool,
        job_token: JobToken | None = None,
    ) -> None:
        if recoverable:
            self.recoverable_tokens.append(output_token.persistent_id)
        if job_token is not None:
            job_name = job_token.value.name
            if job_name in self.retry_requests.keys():
                async with self.retry_requests[job_name].lock:
                    self.retry_requests[job_name].job_token = job_token
                    self.retry_requests[job_name].token_output.setdefault(
                        output_port, output_token
                    )
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(
                            f"Job {job_name} is notifying on port {output_port}. "
                            f"There are {len(self.retry_requests[job_name].queue)} workflows in waiting"
                        )
                    for elem in list(self.retry_requests[job_name].queue):
                        if elem.port.name == output_port:
                            elem.port.put(output_token)
                            elem.waiting_token -= 1
                            if elem.waiting_token == 0:
                                elem.port.put(TerminationToken())
                                self.retry_requests[job_name].queue.remove(elem)
                    self.retry_requests[job_name].is_running = False

    async def sync_workflows(self, mapper: GraphMapper, workflow: Workflow) -> None:
        job_ports = {}
        for job_token in filter(
            lambda t: isinstance(t, JobToken), mapper.token_instances.values()
        ):
            job_name = job_token.value.name
            retry_request = self.retry_requests.setdefault(job_name, RetryRequest())
            if (
                is_available := await self.is_recovered(job_token)
            ) == TokenAvailability.FutureAvailable:
                await self._sync_running_jobs(job_token, mapper, workflow, job_ports)
            elif is_available == TokenAvailability.Available:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Synchronize rollbacks: job {job_token.value.name} output available"
                    )
                # todo: create a unit test for this case and check if it works well
                # Search execute token after job token, replace this token with job_req token.
                # Then remove all the prev tokens
                for port_name in await mapper.get_output_ports(job_token):
                    new_token = retry_request.token_output[port_name]
                    await mapper.replace_token(
                        port_name,
                        new_token,
                        True,
                    )
                    mapper.remove_token(new_token.persistent_id, preserve_token=True)
            else:
                await self._increase_request(retry_request, job_token, workflow)


class DummyFailureManager(FailureManager):

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("dummy_failure_manager.json")
            .read_text("utf-8")
        )

    async def close(self):
        pass

    async def handle_exception(
        self, job: Job, step: Step, exception: BaseException
    ) -> None:
        if logger.isEnabledFor(logging.WARNING):
            logger.warning(
                f"Job {job.name} failure can not be recovered. Failure manager is not enabled."
            )
        raise exception

    async def handle_failure(
        self, job: Job, step: Step, command_output: CommandOutput
    ) -> None:
        if logger.isEnabledFor(logging.WARNING):
            logger.warning(
                f"Job {job.name} failure can not be recovered. Failure manager is not enabled."
            )
        raise FailureHandlingException(
            f"FAILED Job {job.name} with error:\n\t{command_output.value}"
        )

    async def is_recovered(self, token: JobToken) -> TokenAvailability:
        pass

    async def notify(
        self,
        output_port: str,
        output_token: Token,
        recoverable: bool,
        job_token: JobToken | None = None,
    ) -> None:
        pass

    async def sync_workflows(self, mapper: GraphMapper, workflow: Workflow) -> None:
        pass
