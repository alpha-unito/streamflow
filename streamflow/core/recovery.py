from __future__ import annotations

import asyncio
import functools
from abc import abstractmethod
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
    def get_request(self, job_name: str) -> RecoveryRequest: ...

    @abstractmethod
    async def is_recovering(self, job_name: str) -> bool: ...

    @abstractmethod
    async def notify(
        self,
        output_port: str,
        output_token: Token,
        job_token: JobToken | None = None,
    ) -> None: ...

    @abstractmethod
    async def recover(self, job: Job, step: Step, exception: BaseException) -> None: ...

    @abstractmethod
    async def update_request(self, job_name: str) -> None: ...


class RecoveryPolicy(ABC):
    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context

    @abstractmethod
    async def recover(self, failed_job: Job, failed_step: Step) -> None: ...


class RecoveryRequest:
    __slots__ = ("lock", "name", "version", "workflow")

    def __init__(self, name: str) -> None:
        self.lock: asyncio.Lock = asyncio.Lock()
        self.name: str = name
        self.version: int = 1
        self.workflow: Workflow | None = None
