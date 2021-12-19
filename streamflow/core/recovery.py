from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.workflow import Job, CommandOutput
    from typing import Optional, MutableMapping, Any


class CheckpointManager(ABC):

    def __init__(self,
                 context: StreamFlowContext):
        self.context: StreamFlowContext = context

    @abstractmethod
    def register_path(self,
                      job: Optional[Job],
                      path: str) -> None:
        ...


class FailureManager(ABC):

    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context

    @abstractmethod
    async def handle_exception(self, job: Job, exception: BaseException) -> CommandOutput:
        ...

    @abstractmethod
    async def handle_failure(self, job: Job, command_output: CommandOutput) -> CommandOutput:
        ...

    @abstractmethod
    async def replay_job(self, replay_request: ReplayRequest) -> ReplayResponse:
        ...


class JobVersion(object):
    __slots__ = ('job', 'version')

    def __init__(self, job: str, version: int = 1):
        self.job: str = job
        self.version: int = version


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
