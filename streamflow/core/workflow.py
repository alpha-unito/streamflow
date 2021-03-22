from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING, MutableSequence

if TYPE_CHECKING:
    from streamflow.deployment.deployment_manager import ModelConfig
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.deployment import Connector
    from typing import Optional, MutableMapping, Any, Set, Union
    from typing_extensions import Text


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
    __slots__ = ('name', 'value', 'job', 'weight')

    def __init__(self,
                 name: Text,
                 value: Any,
                 job: Optional[Union[Text, MutableSequence[Text]]] = None,
                 weight: int = 0):
        self.name = name
        self.value: Any = value
        self.job: Optional[Union[Text, MutableSequence[Text]]] = job
        self.weight: int = weight

    def update(self, value: Any) -> Token:
        return Token(name=self.name, job=self.job, weight=self.weight, value=value)

    def rename(self, name: Text):
        return Token(name=name, job=self.job, weight=self.weight, value=self.value)


class TerminationToken(Token):

    def __init__(self, name: Text):
        super().__init__(name, None)

    def update(self, value: Any) -> Token:
        raise NotImplementedError()

    def rename(self, name: Text):
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
                 name: Text,
                 step: Step,
                 inputs: MutableSequence[Token],
                 input_directory: Optional[Text] = None,
                 output_directory: Optional[Text] = None,
                 tmp_directory: Optional[Text] = None):
        self.name: Text = name
        self.step = step
        self.inputs: MutableSequence[Token] = inputs
        self.input_directory: Optional[Text] = input_directory
        self.output_directory: Optional[Text] = output_directory
        self.tmp_directory: Optional[Text] = tmp_directory

    def get_resources(self, statuses: Optional[MutableSequence[Status]] = None) -> MutableSequence[Text]:
        return self.step.context.scheduler.get_resources(self.name, statuses)

    @abstractmethod
    async def initialize(self):
        ...

    @abstractmethod
    async def run(self):
        ...


class Port(ABC):

    def __init__(self,
                 name: Text,
                 step: Optional[Step] = None):
        self.name: Text = name
        self.step: Optional[Step] = step
        self.token_processor: Optional[TokenProcessor] = None


class InputPort(Port, ABC):

    def __init__(self,
                 name: Text,
                 step: Optional[Step] = None):
        super().__init__(name, step)
        self.dependee: Optional[OutputPort] = None

    @abstractmethod
    async def get(self) -> Union[Token, MutableSequence[Token]]:
        ...


class InputCombinator(InputPort, ABC):

    def __init__(self,
                 name: Text,
                 step: Optional[Step] = None,
                 ports: Optional[MutableMapping[Text, InputPort]] = None):
        super().__init__(name, step)
        self.ports: MutableMapping[Text, InputPort] = ports or {}

    @abstractmethod
    async def get(self) -> MutableSequence[Token]:
        ...


class OutputPort(Port, ABC):

    @abstractmethod
    def empty(self) -> bool:
        ...

    @abstractmethod
    async def get(self, consumer: Text) -> Token:
        ...

    @abstractmethod
    def put(self, token: Token):
        ...


class OutputCombinator(OutputPort, ABC):

    def __init__(self,
                 name: Text,
                 step: Optional[Step] = None,
                 ports: Optional[MutableMapping[Text, OutputPort]] = None):
        super().__init__(name, step)
        self.ports: MutableMapping[Text, OutputPort] = ports or {}


class TokenProcessor(ABC):

    def __init__(self, port: Port):
        self.port: Port = port

    @abstractmethod
    async def collect_output(self, token: Token, output_dir: Text) -> Token:
        ...

    @abstractmethod
    async def compute_token(self, job: Job, command_output: CommandOutput) -> Token:
        ...

    @abstractmethod
    def get_related_resources(self, token: Token) -> Set[Text]:
        ...

    @abstractmethod
    async def recover_token(self, job: Job, resources: MutableSequence[Text], token: Token) -> Token:
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
                 name: Text,
                 context: StreamFlowContext,
                 command: Optional[Command] = None,
                 target: Optional[Target] = None):
        super().__init__()
        self.context: StreamFlowContext = context
        self.command: Optional[Command] = command
        self.input_combinator: Optional[InputCombinator] = None
        self.input_ports: MutableMapping[Text, Union[InputPort, InputCombinator]] = {}
        self.name: Text = name
        self.output_ports: MutableMapping[Text, OutputPort] = {}
        self.status: Status = Status.WAITING
        self.target: Optional[Target] = target
        self.terminated: bool = False
        self.workdir: Optional[Text] = None

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
                 service: Optional[Text] = None):
        self.model: ModelConfig = model
        self.resources: int = resources
        self.service: Optional[Text] = service


class Workflow(object):

    def __init__(self):
        super().__init__()
        self.steps: MutableMapping[Text, Step] = {}
        self.output_ports: MutableMapping[Text, OutputPort] = {}
