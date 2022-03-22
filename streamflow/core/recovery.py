from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from streamflow.core.data import DataLocation

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.workflow import Job, CommandOutput, Token, Step
    from typing import Optional, MutableMapping, Any


class CheckpointManager(ABC):

    def __init__(self,
                 context: StreamFlowContext):
        self.context: StreamFlowContext = context

    @abstractmethod
    def register(self, data_location: DataLocation) -> None:
        ...


class FailureManager(ABC):

    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context

    @abstractmethod
    async def handle_exception(self,
                               job: Job,
                               step: Step,
                               exception: BaseException) -> CommandOutput:
        ...

    @abstractmethod
    async def handle_failure(self,
                             job: Job,
                             step: Step,
                             command_output: CommandOutput) -> CommandOutput:
        ...

    @abstractmethod
    def register_job(self,
                     job: Job,
                     step: Step,
                     outputs: MutableMapping[str, Token]):
        ...


class ReplayRequest(object):
    __slots__ = ('sender', 'target', 'version')

    def __init__(self, sender: str, target: str, version: int = 1):
        self.sender: str = sender
        self.target: str = target
        self.version: int = version


class ReplayResponse(object):
    __slots__ = ('job', 'outputs', 'version')

    def __init__(self, job: str, outputs: Optional[MutableMapping[str, Any]], version: int = 1):
        self.job: str = job
        self.outputs: MutableMapping[str, Any] = outputs
        self.version: int = version
