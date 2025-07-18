from __future__ import annotations

import asyncio
import functools
from abc import abstractmethod
from collections.abc import MutableMapping
from enum import IntEnum
from typing import TYPE_CHECKING

from streamflow.core.context import SchemaEntity
from streamflow.core.exception import FailureHandlingException
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
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        # When receiving a CancelledError, mark the step as Cancelled
        except asyncio.CancelledError:
            raise
        # When receiving a FailureHandling exception, mark the step as Failed
        except FailureHandlingException:
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


class RecoveryPolicy:
    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context

    @abstractmethod
    async def recover(self, failed_job: Job, failed_step: Step) -> None: ...


class RetryRequest:
    __slots__ = ("job_token", "lock", "output_tokens", "version", "workflow")

    def __init__(self) -> None:
        self.job_token: JobToken | None = None
        self.lock: asyncio.Lock = asyncio.Lock()
        self.output_tokens: MutableMapping[str, Token] = {}
        self.version: int = 1
        self.workflow: Workflow | None = None


class TokenAvailability(IntEnum):
    Unavailable = 0
    Available = 1
    FutureAvailable = 2
