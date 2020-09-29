from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.workflow import Job, CommandOutput
    from typing import Optional, MutableMapping, Any
    from typing_extensions import Text


class CheckpointManager(ABC):

    def __init__(self,
                 context: StreamFlowContext):
        self.context: StreamFlowContext = context

    @abstractmethod
    def register_path(self,
                      job: Optional[Job],
                      path: Text) -> None:
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

    def __init__(self, job: Text, version: int = 1):
        self.job: Text = job
        self.version: int = version


class ReplayRequest(object):
    __slots__ = ('sender', 'target', 'version')

    def __init__(self, sender: Text, target: Text, version: int = 1):
        self.sender: Text = sender
        self.target: Text = target
        self.version: int = version


class ReplayResponse(object):
    __slots__ = ('job', 'outputs', 'version')

    def __init__(self, job: Text, outputs: Optional[MutableMapping[Text, Any]], version: int = 1):
        self.job: Text = job
        self.outputs: MutableMapping[Text, Any] = outputs
        self.version: int = version
