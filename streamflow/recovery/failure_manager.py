from __future__ import annotations

import asyncio
import logging
from collections.abc import MutableMapping
from importlib.resources import files

from streamflow.core.command import CommandOutput
from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import (
    FailureHandlingException,
    WorkflowExecutionException,
    WorkflowTransferException,
)
from streamflow.core.recovery import FailureManager
from streamflow.core.workflow import Job, Status, Step, Token
from streamflow.log_handler import logger
from streamflow.recovery.recovery import RollbackRecoveryPolicy
from streamflow.recovery.rollback_recovery import TokenAvailability
from streamflow.recovery.utils import RetryRequest, execute_recover_workflow
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

    async def _do_handle_failure(self, job: Job, step: Step) -> CommandOutput:
        # Delay rescheduling to manage temporary failures (e.g. connection lost)
        if self.retry_delay is not None:
            await asyncio.sleep(self.retry_delay)
        try:
            new_workflow = await self._recover_jobs(job, step)
            command_output = CommandOutput(
                value=None,
                status=(
                    new_workflow.steps[step.name].status
                    if new_workflow.steps.keys()
                    else Status.COMPLETED
                ),
            )
            # When receiving a FailureHandlingException, simply fail
        except FailureHandlingException as e:
            logger.exception(e)
            raise
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        except WorkflowTransferException as e:
            logger.exception(e)
            raise
        except Exception as e:
            logger.exception(e)
            raise
        return command_output

    async def _handle_failure_transfer(
        self, job: Job, step: Step, port_name: str
    ) -> Token:
        if job.name in self.retry_requests.keys():
            async with self.retry_requests[job.name].lock:
                self.retry_requests[job.name].is_running = False
        if self.retry_delay is not None:
            await asyncio.sleep(self.retry_delay)
        try:
            new_workflow = await self._recover_jobs(job, step)
            token = next(
                iter(
                    new_workflow.steps[step.name].get_output_port(port_name).token_list
                )
            )
        # When receiving a FailureHandlingException, simply fail
        except FailureHandlingException as e:
            logger.exception(e)
            raise
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        except (WorkflowTransferException, WorkflowExecutionException) as e:
            logger.exception(e)
            return await self.handle_exception(job, step, e)
        except Exception as e:
            logger.exception(e)
            raise e
        return token

    async def _recover_jobs(self, failed_job: Job, failed_step: Step):
        rollback = RollbackRecoveryPolicy(self.context)
        # Generate new workflow
        new_workflow = await rollback.recover_workflow(failed_job, failed_step)
        # Execute new workflow
        await execute_recover_workflow(new_workflow, failed_step)
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"COMPLETED Recovery execution of failed job {failed_job.name}")
        return new_workflow

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
    ) -> CommandOutput:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"Handling {type(exception).__name__} failure for job {job.name}"
            )
        if job.name in self.retry_requests.keys():
            self.retry_requests[job.name].is_running = False
        if isinstance(exception, WorkflowTransferException):
            return await self._handle_failure_transfer(job, step, exception)
        else:
            return await self._do_handle_failure(job, step)

    async def handle_failure(
        self, job: Job, step: Step, command_output: CommandOutput
    ) -> CommandOutput:
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"Handling command failure for job {job.name}")

        if job.name in self.retry_requests.keys():
            self.retry_requests[job.name].is_running = False
        return await self._do_handle_failure(job, step)

    async def is_running_token(self, token: Token) -> TokenAvailability:
        if request := self.retry_requests.get(token.value.name):
            async with request.lock:
                if request.is_running:
                    return TokenAvailability.FutureAvailable
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

    async def notify_jobs(
        self, job_token: JobToken, output_port: str, output_token: Token
    ) -> None:
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
                elems = []
                for elem in self.retry_requests[job_name].queue:
                    if elem.port.name == output_port:
                        elem.port.put(output_token)
                        elem.waiting_token -= 1
                        if elem.waiting_token == 0:
                            elems.append(elem)
                            elem.port.put(TerminationToken())
                for elem in elems:
                    self.retry_requests[job_name].queue.remove(elem)
                self.retry_requests[job_name].is_running = False

    async def update_job_status(self, job_name: str) -> None:
        if (
            self.max_retries is None
            or self.retry_requests[job_name].version < self.max_retries
        ):
            self.retry_requests[job_name].version += 1
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"Updated Job {job_name} at {self.retry_requests[job_name].version} times"
                )
            # free resources scheduler
            await self.context.scheduler.notify_status(job_name, Status.ROLLBACK)
        else:
            logger.error(
                f"FAILED Job {job_name} {self.retry_requests[job_name].version} times. Execution aborted"
            )
            raise FailureHandlingException(
                f"FAILED Job {job_name} {self.retry_requests[job_name].version} times. Execution aborted"
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

    async def close(self):
        pass

    async def handle_exception(
        self, job: Job, step: Step, exception: BaseException
    ) -> CommandOutput:
        if logger.isEnabledFor(logging.WARNING):
            logger.warning(
                f"Job {job.name} failure can not be recovered. Failure manager is not enabled."
            )
        raise exception

    async def handle_failure(
        self, job: Job, step: Step, command_output: CommandOutput
    ) -> CommandOutput:
        if logger.isEnabledFor(logging.WARNING):
            logger.warning(
                f"Job {job.name} failure can not be recovered. Failure manager is not enabled."
            )
        raise FailureHandlingException(
            f"FAILED Job {job.name} with error:\n\t{command_output.value}"
        )

    async def notify_jobs(
        self, job_token: JobToken, output_port: str, output_token: Token
    ):
        pass

    async def update_job_status(self, job_name: str) -> None:
        pass
