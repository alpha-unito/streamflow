from __future__ import annotations

import asyncio
from abc import abstractmethod
from collections.abc import MutableMapping
from enum import IntEnum
from typing import TYPE_CHECKING

from streamflow.core.context import SchemaEntity
from streamflow.workflow.token import JobToken

if TYPE_CHECKING:
    from streamflow.core.command import CommandOutput
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.data import DataLocation
    from streamflow.core.workflow import Job, Step, Token, Workflow


class CheckpointManager(SchemaEntity):
    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context

    @abstractmethod
    async def close(self): ...

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
    async def handle_exception(
        self, job: Job, step: Step, exception: BaseException
    ) -> None: ...

    @abstractmethod
    async def handle_failure(
        self, job: Job, step: Step, command_output: CommandOutput
    ) -> None: ...

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

    def __init__(self):
        self.job_token: JobToken | None = None
        self.lock: asyncio.Lock = asyncio.Lock()
        self.output_tokens: MutableMapping[str, Token] = {}
        self.version: int = 1
        self.workflow: Workflow | None = None


class TokenAvailability(IntEnum):
    Unavailable = 0
    Available = 1
    FutureAvailable = 2
