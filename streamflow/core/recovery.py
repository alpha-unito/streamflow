from __future__ import annotations

import asyncio
from abc import abstractmethod
from collections.abc import MutableMapping, MutableSequence
from enum import IntEnum
from typing import TYPE_CHECKING

from streamflow.core.context import SchemaEntity
from streamflow.workflow.token import JobToken

if TYPE_CHECKING:
    from streamflow.core.command import CommandOutput
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.data import DataLocation
    from streamflow.core.workflow import Job, Port, Step, Token, Workflow


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

    # TODO update documentation

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
    def is_recoverable(self, token: Token) -> bool: ...

    @abstractmethod
    async def notify(
        self,
        output_port: str,
        output_token: Token,
        recoverable: bool = True,
        job_token: JobToken | None = None,
    ) -> None: ...

    @abstractmethod
    async def update_request(self, job_name: str) -> None: ...


class PortRecovery:
    __slots__ = ("port", "waiting_token")

    def __init__(self, port: Port):
        self.port: Port = port
        self.waiting_token: int = 0


class RetryRequest:
    __slots__ = ("job_token", "output_tokens", "lock", "queue", "version", "workflow")

    def __init__(self):
        self.job_token: JobToken | None = None
        self.output_tokens: MutableMapping[str, Token] = {}
        self.lock: asyncio.Lock = asyncio.Lock()
        # Other workflows can queue to the output port of the step while the job is running.
        self.queue: MutableSequence[PortRecovery] = []
        self.version: int = 1
        self.workflow: Workflow | None = None


class TokenAvailability(IntEnum):
    Unavailable = 0
    Available = 1
    FutureAvailable = 2
