from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING

from streamflow.core.context import SchemaEntity
from streamflow.workflow.token import JobToken

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.workflow import Job, CommandOutput, Step, Token, Port
    from typing import MutableMapping, Any


class CheckpointManager(SchemaEntity):
    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context

    @abstractmethod
    async def close(self):
        ...

    @abstractmethod
    async def wait(self):
        ...

    @abstractmethod
    def save_data(self, token: Token):
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

    @abstractmethod
    async def get_valid_job_token(self, job_token: JobToken):
        ...

    @abstractmethod
    async def get_token(self, job_name, output_name):
        ...

    @abstractmethod
    def is_valid_tag(self, workflow_name: str, tag: str, output_port: Port):
        ...

    @abstractmethod
    async def notify_jobs(self, job_token: JobToken, out_port_name: str, token: Token):
        ...

    @abstractmethod
    async def handle_failure_transfer(self, job: Job, step: Step, port_name: str):
        ...

    @abstractmethod
    async def get_tokens(self, job_name):
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
