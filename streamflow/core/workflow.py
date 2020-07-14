from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from streamflow.deployment.deployment_manager import ModelConfig
    from streamflow.core.context import StreamflowContext
    from streamflow.core.deployment import Connector
    from streamflow.core.scheduling import JobStatus
    from typing import Optional, MutableMapping, Any, List, Tuple
    from typing_extensions import Text


class Command(ABC):

    def __init__(self, task: Task):
        self.task: Task = task

    @abstractmethod
    async def execute(self, job: Job) -> Tuple[Any, JobStatus]:
        pass


class Condition(ABC):

    def __init__(self, task: Task):
        self.task: Task = task

    @abstractmethod
    async def evaluate(self) -> bool:
        pass


class Token(object):
    __slots__ = ('name', 'value', 'job', 'weight')

    def __init__(self,
                 name: Text,
                 value: Any,
                 job: Optional[Union[Text, List[Text]]] = None,
                 weight: Optional[int] = None):
        self.name = name
        self.value: Any = value
        self.job: Optional[Union[Text, List[Text]]] = job
        self.weight: Optional[int] = weight


class TerminationToken(Token):

    def __init__(self, name: Text):
        super().__init__(name, None)


class Executor(ABC):

    def __init__(self, workflow: Workflow):
        self.workflow: Workflow = workflow

    @abstractmethod
    async def run(self):
        pass


class Job(object):
    __slots__ = ('name', 'task', 'inputs', 'input_directory', 'output_directory')

    def __init__(self,
                 name: Text,
                 task: Task,
                 inputs: List[Token]):
        self.name: Text = name
        self.task = task
        self.inputs: List[Token] = inputs
        self.input_directory: Optional[Text] = None
        self.output_directory: Optional[Text] = None

    def get_resource(self) -> Text:
        return self.task.context.scheduler.get_resource(self.name)


class Port(ABC):

    def __init__(self, name: Text):
        self.name: Text = name
        self.task: Optional[Task] = None
        self.token_processor: Optional[TokenProcessor] = None


class InputPort(Port, ABC):

    def __init__(self, name: Text):
        super().__init__(name)
        self.dependee: Optional[OutputPort] = None

    @abstractmethod
    async def get(self) -> Token:
        pass


class InputCombinator(ABC):

    def __init__(self, name: Text):
        super().__init__()
        self.name = name
        self.ports: MutableMapping[Text, Union[InputPort, InputCombinator]] = {}

    @abstractmethod
    async def get(self) -> List[Token]:
        pass


class OutputPort(Port, ABC):

    @abstractmethod
    async def get(self, task: Task) -> Token:
        pass

    @abstractmethod
    def put(self, token: Token):
        pass


class TokenProcessor(ABC):

    def __init__(self, port: Port):
        self.port: Port = port

    @abstractmethod
    async def collect_output(self, token: Token, output_dir: Text) -> None:
        pass

    @abstractmethod
    async def compute_token(self, job: Job, result: Any, status: JobStatus) -> Token:
        pass

    @abstractmethod
    async def update_token(self, job: Job, token: Token) -> Token:
        pass

    @abstractmethod
    async def weight_token(self, job: Job, token_value: Any) -> int:
        pass


class Task(ABC):

    def __init__(self,
                 name: Text,
                 context: StreamflowContext):
        super().__init__()
        self.condition: Optional[Condition] = None
        self.context: StreamflowContext = context
        self.command: Optional[Command] = None
        self.input_combinator: Optional[InputCombinator] = None
        self.input_ports: MutableMapping[Text, Union[InputPort, InputCombinator]] = {}
        self.name: Text = name
        self.output_ports: MutableMapping[Text, OutputPort] = {}
        self.target: Optional[Target] = None

    @abstractmethod
    def get_connector(self) -> Optional[Connector]:
        pass

    @abstractmethod
    async def run(self):
        pass


class Target(object):

    def __init__(self,
                 model: ModelConfig,
                 service: Text):
        self.model: ModelConfig = model
        self.service: Text = service


class Workflow(object):

    def __init__(self):
        super().__init__()
        self.tasks: MutableMapping[Text, Task] = {}
        self.output_ports: MutableMapping[Text, OutputPort] = {}
