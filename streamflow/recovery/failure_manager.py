from __future__ import annotations

import asyncio
import logging
from collections.abc import MutableMapping
from importlib.resources import files

from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import FailureHandlingException
from streamflow.core.recovery import (
    FailureManager,
    RetryRequest,
    TokenAvailability,
    recoverable,
)
from streamflow.core.utils import get_job_tag
from streamflow.core.workflow import Job, Status, Step, Token
from streamflow.log_handler import logger
from streamflow.recovery.policy.recovery import RollbackRecoveryPolicy
from streamflow.workflow.token import JobToken


class DefaultFailureManager(FailureManager):
    def __init__(
        self,
        context: StreamFlowContext,
        max_retries: int | None = None,
        retry_delay: int | None = None,
    ):
        super().__init__(context)
        self.max_retries: int | None = max_retries
        self.retry_delay: int | None = retry_delay
        self._retry_requests: MutableMapping[str, RetryRequest] = {}

    @recoverable
    async def _do_handle_failure(self, job: Job, step: Step) -> None:
        # Delay rescheduling to manage temporary failures (e.g. connection lost)
        if self.retry_delay is not None:
            await asyncio.sleep(self.retry_delay)
        await RollbackRecoveryPolicy(self.context).recover(job, step)
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"COMPLETED Recovery execution of failed job {job.name}")

    async def close(self) -> None:
        pass

    def get_request(self, job_name: str) -> RetryRequest:
        if job_name in self._retry_requests.keys():
            return self._retry_requests[job_name]
        else:
            return self._retry_requests.setdefault(job_name, RetryRequest())

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("default_failure_manager.json")
            .read_text("utf-8")
        )

    async def recover(self, job: Job, step: Step, exception: BaseException) -> None:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"Handling {type(exception).__name__} failure for job {job.name}"
            )
        await self.context.scheduler.notify_status(job.name, Status.RECOVERY)
        await self._do_handle_failure(job, step)

    async def is_recovered(self, job_name: str) -> TokenAvailability:
        if request := self._retry_requests.get(job_name):
            async with request.lock:
                if self.context.scheduler.get_allocation(job_name).status in (
                    Status.ROLLBACK,
                    Status.RUNNING,
                    Status.FIREABLE,
                ):
                    return TokenAvailability.FutureAvailable
                elif request.workflow and request.workflow_ready.is_set():
                    output_tokens = []
                    for port_name in request.output_ports:
                        port = request.workflow.ports[port_name]
                        if t := next(
                            (
                                t
                                for t in port.token_list
                                if t.tag == get_job_tag(job_name)
                            ),
                            None,
                        ):
                            output_tokens.append(t)
                    if len(output_tokens) > 0 and all(
                        await asyncio.gather(
                            *(
                                asyncio.create_task(t.is_available(self.context))
                                for t in output_tokens
                            )
                        )
                    ):
                        logger.info(f"Job token {job_name} is available")
                        return TokenAvailability.Available
        return TokenAvailability.Unavailable

    async def notify(
        self,
        output_port: str,
        output_token: Token,
        job_token: JobToken | None = None,
    ) -> None:
        if job_token is not None:
            job_name = job_token.value.name
            if job_name in self._retry_requests.keys():
                async with self._retry_requests[job_name].lock:
                    self._retry_requests[job_name].job_token = job_token

    async def update_request(self, job_name: str) -> None:
        retry_request = self._retry_requests[job_name]
        async with retry_request.lock:
            retry_request.job_token = None
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

    def get_request(self, job_name: str) -> RetryRequest:
        pass

    async def recover(self, job: Job, step: Step, exception: BaseException) -> None:
        if logger.isEnabledFor(logging.WARNING):
            logger.warning(
                f"Job {job.name} failure can not be recovered. Failure manager is not enabled."
            )
        raise exception

    async def is_recovered(self, job_name: str) -> TokenAvailability:
        return TokenAvailability.Unavailable

    async def notify(
        self,
        output_port: str,
        output_token: Token,
        job_token: JobToken | None = None,
    ) -> None:
        pass

    async def update_request(self, job_name: str) -> None:
        pass
