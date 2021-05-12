from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING, MutableSequence

if TYPE_CHECKING:
    from streamflow.deployment.deployment_manager import ModelConfig
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.deployment import Connector
    from typing import Optional, MutableMapping, Any, Set, Union


class Command(ABC):

    def __init__(self, step: Step):
        self.step: Step = step

    @abstractmethod
    async def execute(self, job: Job) -> CommandOutput:
        ...


class CommandOutput(object):
    __slots__ = ('value', 'status')

    def __init__(self,
                 value: Any,
                 status: Status):
        self.value: Any = value
        self.status: Status = status

    def update(self, value: Any):
        return CommandOutput(value=value, status=self.status)


class Token(object):
    __slots__ = ('name', 'value', 'job', 'tag', 'weight')

    def __init__(self,
                 name: str,
                 value: Any,
                 job: Optional[Union[str, MutableSequence[str]]] = None,
                 tag: str = '/',
                 weight: int = 0):
        self.name = name
        self.value: Any = value
        self.job: Optional[Union[str, MutableSequence[str]]] = job
        self.tag: str = tag
        self.weight: int = weight

    def update(self, value: Any) -> Token:
        return Token(name=self.name, job=self.job, tag=self.tag, weight=self.weight, value=value)

    def rename(self, name: str) -> Token:
        return Token(name=name, job=self.job, tag=self.tag, weight=self.weight, value=self.value)


class TerminationToken(Token):

    def __init__(self, name: str):
        super().__init__(name, None)

    def update(self, value: Any) -> Token:
        raise NotImplementedError()

    def rename(self, name: str) -> Token:
        return TerminationToken(name=name)


class Executor(ABC):

    def __init__(self, workflow: Workflow):
        self.workflow: Workflow = workflow

    @abstractmethod
    async def run(self):
        ...


class Job(ABC):
    __slots__ = ('name', 'step', 'inputs', 'input_directory', 'output_directory', 'tmp_directory')

    def __init__(self,
                 name: str,
                 step: Step,
                 inputs: MutableSequence[Token],
                 input_directory: Optional[str] = None,
                 output_directory: Optional[str] = None,
                 tmp_directory: Optional[str] = None):
        self.name: str = name
        self.step = step
        self.inputs: MutableSequence[Token] = inputs
        self.input_directory: Optional[str] = input_directory
        self.output_directory: Optional[str] = output_directory
        self.tmp_directory: Optional[str] = tmp_directory

    def get_resources(self, statuses: Optional[MutableSequence[Status]] = None) -> MutableSequence[str]:
        return self.step.context.scheduler.get_resources(self.name, statuses)

    @abstractmethod
    async def initialize(self):
        ...

    @abstractmethod
    async def run(self):
        ...


class Port(ABC):

    def __init__(self,
                 name: str,
                 step: Optional[Step] = None):
        self.name: str = name
        self.step: Optional[Step] = step
        self.token_processor: Optional[TokenProcessor] = None


class InputPort(Port, ABC):

    def __init__(self,
                 name: str,
                 step: Optional[Step] = None):
        super().__init__(name, step)
        self.dependee: Optional[OutputPort] = None

    @abstractmethod
    async def get(self) -> Union[Token, MutableSequence[Token]]:
        ...


class InputCombinator(InputPort, ABC):

    def __init__(self,
                 name: str,
                 step: Optional[Step] = None,
                 ports: Optional[MutableMapping[str, InputPort]] = None):
        super().__init__(name, step)
        self.ports: MutableMapping[str, InputPort] = ports or {}

    @abstractmethod
    async def get(self) -> MutableSequence[Token]:
        ...


class OutputPort(Port, ABC):

    @abstractmethod
    def empty(self) -> bool:
        ...

    @abstractmethod
    async def get(self, consumer: str) -> Token:
        ...

    @abstractmethod
    def put(self, token: Token):
        ...


class OutputCombinator(OutputPort, ABC):

    def __init__(self,
                 name: str,
                 step: Optional[Step] = None,
                 ports: Optional[MutableMapping[str, OutputPort]] = None):
        super().__init__(name, step)
        self.ports: MutableMapping[str, OutputPort] = ports or {}


class TokenProcessor(ABC):

    def __init__(self, port: Port):
        self.port: Port = port

    @abstractmethod
    async def collect_output(self, token: Token, output_dir: str) -> Token:
        ...

    @abstractmethod
    async def compute_token(self, job: Job, command_output: CommandOutput) -> Token:
        ...

    @abstractmethod
    def get_related_resources(self, token: Token) -> Set[str]:
        ...

    @abstractmethod
    async def recover_token(self, job: Job, resources: MutableSequence[str], token: Token) -> Token:
        ...

    @abstractmethod
    async def update_token(self, job: Job, token: Token) -> Token:
        ...

    @abstractmethod
    async def weight_token(self, job: Job, token_value: Any) -> int:
        ...


class Status(Enum):
    WAITING = 0
    FIREABLE = 1
    RUNNING = 2
    SKIPPED = 3
    COMPLETED = 4
    FAILED = 5


class Step(ABC):

    def __init__(self,
                 name: str,
                 context: StreamFlowContext,
                 command: Optional[Command] = None,
                 target: Optional[Target] = None):
        super().__init__()
        self.context: StreamFlowContext = context
        self.command: Optional[Command] = command
        self.input_combinator: Optional[InputCombinator] = None
        self.input_ports: MutableMapping[str, Union[InputPort, InputCombinator]] = {}
        self.name: str = name
        self.output_ports: MutableMapping[str, OutputPort] = {}
        self.status: Status = Status.WAITING
        self.target: Optional[Target] = target
        self.terminated: bool = False
        self.workdir: Optional[str] = None

    @abstractmethod
    def get_connector(self) -> Optional[Connector]:
        ...

    @abstractmethod
    async def run(self):
        ...

    @abstractmethod
    def terminate(self, status: Status):
        ...


class Target(object):
    __slots__ = ('model', 'resources', 'service')

    def __init__(self,
                 model: ModelConfig,
                 resources: int = 1,
                 service: Optional[str] = None):
        self.model: ModelConfig = model
        self.resources: int = resources
        self.service: Optional[str] = service


class Workflow(object):

    def __init__(self):
        super().__init__()
        self.steps: MutableMapping[str, Step] = {}
        self.output_ports: MutableMapping[str, OutputPort] = {}
