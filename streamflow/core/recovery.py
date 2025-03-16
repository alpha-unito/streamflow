from __future__ import annotations

from abc import abstractmethod
from collections.abc import MutableMapping
from typing import TYPE_CHECKING

from streamflow.core.context import SchemaEntity
from streamflow.workflow.token import JobToken

if TYPE_CHECKING:
    from typing import Any

    from streamflow.core.context import StreamFlowContext
    from streamflow.core.data import DataLocation
    from streamflow.core.workflow import CommandOutput, Job, Step, Token


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
    async def close(self): ...

    @abstractmethod
    async def handle_exception(
        self, job: Job, step: Step, exception: BaseException
    ) -> None: ...

    @abstractmethod
    async def handle_failure(
        self, job: Job, step: Step, command_output: CommandOutput
    ) -> None: ...

    @abstractmethod
    async def notify(
        self,
        output_port: str,
        output_token: Token,
        recoverable: bool,
        job_token: JobToken | None = None,
    ) -> None: ...

    @abstractmethod
    async def update_job_status(self, job_name: str) -> None: ...


class ReplayRequest:
    __slots__ = ("sender", "target", "version")

    def __init__(self, sender: str, target: str, version: int = 1):
        self.sender: str = sender
        self.target: str = target
        self.version: int = version


class ReplayResponse:
    __slots__ = ("job", "outputs", "version")

    def __init__(
        self, job: str, outputs: MutableMapping[str, Any] | None, version: int = 1
    ):
        self.job: str = job
        self.outputs: MutableMapping[str, Any] = outputs
        self.version: int = version
