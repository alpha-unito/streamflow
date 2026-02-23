from __future__ import annotations

import asyncio
import functools
from abc import ABC, abstractmethod
from collections.abc import MutableMapping
from enum import IntEnum
from typing import TYPE_CHECKING

from streamflow.core.context import SchemaEntity
from streamflow.core.exception import UnrecoverableWorkflowException
from streamflow.core.workflow import Job, Step
from streamflow.log_handler import logger
from streamflow.workflow.token import JobToken

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.data import DataLocation
    from streamflow.core.workflow import Token, Workflow


def recoverable(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        if (step := next((arg for arg in args if isinstance(arg, Step)), None)) is None:
            if (
                step := next(
                    (arg for arg in kwargs.values() if isinstance(arg, Step)), None
                )
            ) is None:
                raise ValueError(
                    "The wrapped function must take a `Step` object as argument"
                )
        if (job := next((arg for arg in args if isinstance(arg, Job)), None)) is None:
            if (
                job := next(
                    (arg for arg in kwargs.values() if isinstance(arg, Job)), None
                )
            ) is None:
                raise ValueError(
                    "The wrapped function must take a `Job` object as argument"
                )
        try:
            await func(*args, **kwargs)
        # Propagate unrecoverable exceptions without handling
        except (
            asyncio.CancelledError,
            KeyboardInterrupt,
            UnrecoverableWorkflowException,
        ) as e:
            logger.exception(e)
            raise
        # When receiving a generic exception, try to handle it
        except Exception as e:
            logger.exception(e)
            try:
                await step.workflow.context.failure_manager.recover(job, step, e)
            # If failure cannot be recovered, fail
            except Exception as ie:
                if ie != e:
                    logger.exception(ie)
                raise

    return wrapper


class CheckpointManager(SchemaEntity):
    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    def register(self, data_location: DataLocation) -> None: ...


class FailureManager(SchemaEntity):
    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    def get_request(self, job_name: str) -> RetryRequest: ...

    @abstractmethod
    async def recover(self, job: Job, step: Step, exception: BaseException) -> None: ...

    @abstractmethod
    async def is_recovered(self, job_name: str) -> TokenAvailability: ...

    @abstractmethod
    async def notify(
        self,
        output_port: str,
        output_token: Token,
        job_token: JobToken | None = None,
    ) -> None: ...

    @abstractmethod
    async def update_request(self, job_name: str) -> None: ...


class RecoveryPolicy(ABC):
    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context

    @abstractmethod
    async def recover(self, failed_job: Job, failed_step: Step) -> None: ...


class AsyncRLock:
    def __init__(self) -> None:
        self._owner: asyncio.Task | None = None
        self._count: int = 0
        self._lock: asyncio.Lock = asyncio.Lock()

    async def acquire(self) -> bool:
        if self._owner == (current_task := asyncio.current_task()):
            self._count += 1
            return True
        await self._lock.acquire()
        self._owner = current_task
        self._count = 1
        return True

    def release(self) -> None:
        if self._owner != asyncio.current_task():
            raise RuntimeError("Cannot release a lock you don't own")

        self._count -= 1
        if self._count == 0:
            self._owner = None
            self._lock.release()

    async def __aenter__(self) -> AsyncRLock:
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.release()


class RetryRequest:
    __slots__ = (
        "job_token",
        "lock",
        "name",
        "output_tokens",
        "version",
        "workflow",
        "workflow_ready",
    )

    def __init__(self, name: str) -> None:
        self.job_token: JobToken | None = None
        self.lock: AsyncRLock = AsyncRLock()
        self.name: str = name
        self.output_tokens: MutableMapping[str, Token] = {}
        self.version: int = 1
        self.workflow: Workflow | None = None
        self.workflow_ready: asyncio.Event = asyncio.Event()


class TokenAvailability(IntEnum):
    Unavailable = 0
    Available = 1
    FutureAvailable = 2
