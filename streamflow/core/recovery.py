from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING

from streamflow.core.context import SchemaEntity

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.data import DataLocation
    from streamflow.core.workflow import Job, CommandOutput, Step
    from typing import MutableMapping, Any


class CheckpointManager(SchemaEntity):
    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context

    @abstractmethod
    async def close(self):
        ...

    @abstractmethod
    def register(self, data_location: DataLocation) -> None:
        ...


class FailureManager(SchemaEntity):
    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context

    @abstractmethod
    async def close(self):
        ...

    @abstractmethod
    async def handle_exception(
        self, job: Job, step: Step, exception: BaseException
    ) -> CommandOutput:
        ...

    @abstractmethod
    async def handle_failure(
        self, job: Job, step: Step, command_output: CommandOutput
    ) -> CommandOutput:
        ...


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
