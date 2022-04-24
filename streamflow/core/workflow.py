from __future__ import annotations

import sys
import uuid
from abc import ABC, abstractmethod
from asyncio import Queue
from enum import Enum
from typing import MutableSequence, TYPE_CHECKING, Type, TypeVar

from streamflow.core.exception import WorkflowExecutionException

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from typing import Optional, MutableMapping, Any


class Command(ABC):

    def __init__(self, step: Step):
        super().__init__()
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


class CommandOutputProcessor(ABC):

    def __init__(self,
                 name: str,
                 workflow: Workflow):
        self.name: str = name
        self.workflow: Workflow = workflow

    @abstractmethod
    async def process(self, job: Job, command_output: CommandOutput) -> Optional[Token]:
        ...


class Executor(ABC):

    def __init__(self, workflow: Workflow):
        self.workflow: Workflow = workflow

    @abstractmethod
    async def run(self) -> MutableMapping[str, Any]:
        ...


class Job(object):
    __slots__ = ('name', 'inputs', 'input_directory', 'output_directory', 'tmp_directory')

    def __init__(self,
                 name: str,
                 inputs: MutableMapping[str, Token],
                 input_directory: str,
                 output_directory: str,
                 tmp_directory: str):
        self.name: str = name
        self.inputs: MutableMapping[str, Token] = inputs
        self.input_directory: str = input_directory
        self.output_directory: str = output_directory
        self.tmp_directory: str = tmp_directory


class Port(object):

    def __init__(self,
                 workflow: Workflow,
                 name: str):
        self.queues: MutableMapping[str, Queue] = {}
        self.name: str = name
        self.token_list: MutableSequence[Token] = []
        self.workflow: Workflow = workflow

    def _init_consumer(self, consumer: str):
        self.queues[consumer] = Queue()
        for t in self.token_list:
            self.queues[consumer].put_nowait(t)

    def close(self, consumer: str):
        if consumer in self.queues:
            self.queues[consumer].task_done()

    def empty(self) -> bool:
        return not self.token_list

    async def get(self, consumer: str) -> Token:
        if consumer not in self.queues:
            self._init_consumer(consumer)
            return await self.queues[consumer].get()
        else:
            token = await self.queues[consumer].get()
            self.queues[consumer].task_done()
            return token

    def get_input_steps(self) -> MutableSequence[Step]:
        return [s for s in self.workflow.steps.values() if self.name in s.output_ports.values()]

    def get_output_steps(self) -> MutableSequence[Step]:
        return [s for s in self.workflow.steps.values() if self.name in s.input_ports.values()]

    def put(self, token: Token):
        self.token_list.append(token)
        for q in self.queues.values():
            q.put_nowait(token)


class Status(Enum):
    WAITING = 0
    FIREABLE = 1
    RUNNING = 2
    SKIPPED = 3
    COMPLETED = 4
    FAILED = 5
    CANCELLED = 6


class Step(ABC):

    def __init__(self,
                 name: str,
                 workflow: Workflow):
        super().__init__()
        self.input_ports: MutableMapping[str, str] = {}
        self.name: str = name
        self.output_ports: MutableMapping[str, str] = {}
        self.persistent_id: Optional[int] = None
        self.status: Status = Status.WAITING
        self.terminated: bool = False
        self.workflow: Workflow = workflow

    def _set_status(self, status: Status):
        self.status = status
        if self.persistent_id is not None:
            self.workflow.context.persistence_manager.db.update_step(self.persistent_id, {"status": status.value})

    def add_input_port(self, name: str, port: Port) -> None:
        if port.name not in self.workflow.ports:
            self.workflow.ports[port.name] = port
        self.input_ports[name] = port.name

    def add_output_port(self, name: str, port: Port) -> None:
        if port.name not in self.workflow.ports:
            self.workflow.ports[port.name] = port
        self.output_ports[name] = port.name

    def get_input_port(self, name: Optional[str] = None) -> Port:
        if name is None:
            if len(self.input_ports) == 1:
                return self.workflow.ports.get(next(iter(self.input_ports.values())))
            else:
                raise WorkflowExecutionException(
                    "Cannot retrieve default input port as step {step} contains multiple input ports.".format(
                        step=self.name))
        return self.workflow.ports.get(self.input_ports[name]) if name in self.input_ports else None

    def get_input_ports(self) -> MutableMapping[str, Port]:
        return {k: self.workflow.ports[v] for k, v in self.input_ports.items()}

    def get_output_port(self, name: Optional[str] = None) -> Port:
        if name is None:
            if len(self.output_ports) == 1:
                return self.workflow.ports.get(next(iter(self.output_ports.values())))
            else:
                raise WorkflowExecutionException(
                    "Cannot retrieve default output port as step {step} contains multiple output ports.".format(
                        step=self.name))
        return self.workflow.ports.get(self.output_ports[name]) if name in self.output_ports else None

    def get_output_ports(self) -> MutableMapping[str, Port]:
        return {k: self.workflow.ports[v] for k, v in self.output_ports.items()}

    @abstractmethod
    async def run(self):
        ...

    @abstractmethod
    def terminate(self, status: Status):
        ...


class Token(object):
    __slots__ = ('value', 'tag')

    def __init__(self,
                 value: Any,
                 tag: str = '0'):
        self.value: Any = value
        self.tag: str = tag

    async def get_weight(self, context: StreamFlowContext):
        return sys.getsizeof(self.value)

    def update(self, value: Any) -> Token:
        return self.__class__(tag=self.tag, value=value)

    def retag(self, tag: str) -> Token:
        return self.__class__(tag=tag, value=self.value)


class TokenProcessor(ABC):

    def __init__(self,
                 name: str,
                 workflow: Workflow):
        self.name: str = name
        self.workflow: Workflow = workflow

    @abstractmethod
    async def process(self, inputs: MutableMapping[str, Token], token: Token) -> Token:
        ...


if TYPE_CHECKING:
    J = TypeVar('J', bound=Job)
    P = TypeVar('P', bound=Port)
    S = TypeVar('S', bound=Step)


class Workflow(object):

    def __init__(self, context: StreamFlowContext):
        super().__init__()
        self.context: StreamFlowContext = context
        self.steps: MutableMapping[str, Step] = {}
        self.ports: MutableMapping[str, Port] = {}
        self.output_ports: MutableMapping[str, str] = {}

    def create_port(self,
                    cls: Type[P] = Port,
                    name: str = None,
                    **kwargs) -> P:
        if name is None:
            name = str(uuid.uuid4())
        port = cls(workflow=self, name=name, **kwargs)
        self.ports[name] = port
        return port

    def create_step(self,
                    cls: Type[S],
                    name: str = None,
                    **kwargs) -> S:
        if name is None:
            name = str(uuid.uuid4())
        step = cls(name=name, workflow=self, **kwargs)
        self.steps[name] = step
        return step

    def get_output_port(self, name: str) -> Port:
        return self.ports[self.output_ports[name]]

    def get_output_ports(self) -> MutableMapping[str, Port]:
        return {name: self.ports[p] for name, p in self.output_ports.items()}
