from __future__ import annotations

import asyncio
import logging
from collections.abc import MutableMapping, MutableSequence
from importlib.resources import files

from streamflow.core.command import CommandOutput
from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import FailureHandlingException, WorkflowException
from streamflow.core.recovery import FailureManager, RetryRequest, TokenAvailability
from streamflow.core.workflow import Job, Status, Step, Token
from streamflow.log_handler import logger
from streamflow.recovery.recovery import RollbackRecoveryPolicy
from streamflow.recovery.utils import execute_recover_workflow
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

    async def close(self) -> None:
        pass

    def get_request(self, job_name: str) -> RetryRequest:
        if job_name in self.retry_requests.keys():
            return self.retry_requests[job_name]
        else:
            return self.retry_requests.setdefault(job_name, RetryRequest())

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
        await self.context.scheduler.notify_status(job.name, Status.RECOVERY)
        await self._do_handle_failure(job, step)

    async def handle_failure(
        self, job: Job, step: Step, command_output: CommandOutput
    ) -> None:
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"Handling command failure for job {job.name}")
        await self.context.scheduler.notify_status(job.name, Status.RECOVERY)
        await self._do_handle_failure(job, step)

    async def is_recovered(self, job_name: str) -> TokenAvailability:
        if request := self.retry_requests.get(job_name):
            async with request.lock:
                if self.context.scheduler.get_allocation(job_name).status in (
                    Status.ROLLBACK,
                    Status.RUNNING,
                    Status.FIREABLE,
                ):
                    return TokenAvailability.Unavailable
                    # return TokenAvailability.FutureAvailable
                elif len(request.output_tokens) > 0 and all(
                    await asyncio.gather(
                        *(
                            asyncio.create_task(t.is_available(self.context))
                            for t in request.output_tokens.values()
                        )
                    )
                ):
                    return TokenAvailability.Available
        return TokenAvailability.Unavailable

    def is_recoverable(self, token: Token) -> bool:
        return token.persistent_id in self.recoverable_tokens

    async def notify(
        self,
        output_port: str,
        output_token: Token,
        recoverable: bool = True,
        job_token: JobToken | None = None,
    ) -> None:
        if recoverable:
            self.recoverable_tokens.append(output_token.persistent_id)
        if job_token is not None:
            job_name = job_token.value.name
            if job_name in self.retry_requests.keys():
                async with self.retry_requests[job_name].lock:
                    self.retry_requests[job_name].job_token = job_token
                    self.retry_requests[job_name].output_tokens.setdefault(
                        output_port, output_token
                    )
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(
                            f"Job {job_name} is notifying on port {output_port}. "
                            f"There are {len(self.retry_requests[job_name].queue)} workflows in waiting"
                        )
                    for waiting_port in list(self.retry_requests[job_name].queue):
                        if waiting_port.port.name == output_port:
                            waiting_port.port.put(output_token)
                            waiting_port.waiting_token -= 1
                            if waiting_port.waiting_token == 0:
                                waiting_port.port.put(TerminationToken())
                                self.retry_requests[job_name].queue.remove(waiting_port)

    async def update_request(self, job_name: str) -> None:
        retry_request = self.retry_requests[job_name]
        async with retry_request.lock:
            retry_request.job_token = None
            retry_request.output_tokens = {}
        if self.max_retries is None or retry_request.version < self.max_retries:
            retry_request.version += 1
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Updated Job {job_name} at {retry_request.version} times")
            # Free resources scheduler
            await self.context.scheduler.notify_status(job_name, Status.ROLLBACK)
        else:
            logger.error(
                f"FAILED Job {job_name} {retry_request.version} times. Execution aborted"
            )
            raise FailureHandlingException(
                f"FAILED Job {job_name} {retry_request.version} times. Execution aborted"
            )


class DummyFailureManager(FailureManager):

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("dummy_failure_manager.json")
            .read_text("utf-8")
        )

    async def close(self) -> None:
        pass

    def get_request(self, job_name: str) -> RetryRequest:
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

    async def is_recovered(self, job_name: str) -> TokenAvailability:
        return TokenAvailability.Unavailable

    def is_recoverable(self, token: Token) -> bool:
        return False

    async def notify(
        self,
        output_port: str,
        output_token: Token,
        recoverable: bool = True,
        job_token: JobToken | None = None,
    ) -> None:
        pass

    async def update_request(self, job_name: str) -> None:
        pass
