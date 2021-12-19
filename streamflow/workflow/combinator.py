import asyncio
import itertools
from asyncio import Queue, Task, FIRST_COMPLETED, Lock, CancelledError
from typing import List, MutableMapping, Any, cast, MutableSequence, Optional, Union, Callable

from streamflow.core import utils
from streamflow.core.utils import flatten_list, get_tag
from streamflow.core.workflow import InputCombinator, Token, TerminationToken, InputPort, Step, OutputCombinator, \
    OutputPort
from streamflow.log_handler import logger


def _default_tag_strategy(token_list: MutableSequence[Token]) -> MutableSequence[Token]:
    return token_list


def _get_job_name(token: Union[Token, MutableSequence[Token]]) -> str:
    if isinstance(token, MutableSequence):
        return ",".join(token[0].job) if isinstance(token[0].job, MutableSequence) else token[0].job
    else:
        return ",".join(token.job) if isinstance(token.job, MutableSequence) else token.job


class DotProductInputCombinator(InputCombinator):

    def __init__(self,
                 name: str,
                 step: Optional[Step] = None,
                 ports: Optional[MutableMapping[str, InputPort]] = None):
        super().__init__(name, step, ports)
        self.lock: Lock = Lock()
        self.initialized: bool = False
        self.queue: Queue = Queue()
        self.token_values: MutableMapping[str, Any] = {}

    async def _dot_product(self):
        input_tasks, terminated = [], []
        for port_name, port in self.ports.items():
            input_tasks.append(asyncio.create_task(port.get(), name=port_name))
        while True:
            # Check if some complete input sets are available
            for tag in list(self.token_values.keys()):
                if len(self.token_values[tag]) == len(self.ports):
                    outputs = self.token_values.pop(tag)
                    self.queue.put_nowait(flatten_list([outputs[k] for k in self.ports.keys()]))
            # Wait for the next token
            finished, unfinished = await asyncio.wait(input_tasks, return_when=FIRST_COMPLETED)
            input_tasks = list(unfinished)
            for task in finished:
                task_name = cast(Task, task).get_name()
                token = task.result()
                # If a TerminationToken is received, the corresponding port terminated its outputs
                if (isinstance(token, TerminationToken) or
                        (isinstance(token, MutableSequence) and utils.check_termination(token))):
                    terminated.append(task_name)
                    # When the last port terminates, the entire combinator terminates
                    if len(terminated) == len(self.ports):
                        self.queue.put_nowait([TerminationToken(self.name)])
                        return
                # Otherwise, put the value in the token lists
                elif isinstance(token, MutableSequence):
                    for t in token:
                        if t.tag not in self.token_values:
                            self.token_values[t.tag] = {}
                        self.token_values[t.tag][task_name] = t
                elif isinstance(token, Token):
                    if token.tag not in self.token_values:
                        self.token_values[token.tag] = {}
                    self.token_values[token.tag][task_name] = token
                # Create a new task in place of the completed one if the port is not terminated
                if task_name not in terminated:
                    input_tasks.append(asyncio.create_task(self.ports[task_name].get(), name=task_name))

    def _handle_exception(self, task: Task):
        try:
            if exc := task.exception():
                logger.exception(exc)
                self.queue.put_nowait(exc)
        except CancelledError:
            pass

    async def _initialize(self):
        # Retrieve initial input tokens
        input_tasks = [asyncio.create_task(p.get()) for p in self.ports.values()]
        inputs = {k: v for (k, v) in zip(self.ports.keys(), await asyncio.gather(*input_tasks))}
        # Check for early termination and put a TerminationToken
        if utils.check_termination(list(inputs.values())):
            self.queue.put_nowait([TerminationToken(self.name)])
        # Otherwise put initial inputs in token lists and in queue and start cartesian multiplier
        else:
            for name, token in inputs.items():
                if isinstance(token, MutableSequence):
                    for t in token:
                        if t.tag not in self.token_values:
                            self.token_values[t.tag] = {}
                        self.token_values[t.tag][name] = t
                elif isinstance(token, Token):
                    if token.tag not in self.token_values:
                        self.token_values[token.tag] = {}
                    self.token_values[token.tag][name] = token
        task = asyncio.create_task(self._dot_product())
        task.add_done_callback(self._handle_exception)

    async def get(self) -> MutableSequence[Token]:
        # If this is the first call to the get() function, initialize the combinator
        async with self.lock:
            if not self.initialized:
                await self._initialize()
                self.initialized = True
        # Otherwise simply wait for new input tokens
        inputs = await self.queue.get()
        # If an exception has been thrown, raise it
        if isinstance(inputs, Exception):
            raise inputs
        # Otherwise, return inputs
        else:
            return inputs


class CartesianProductInputCombinator(InputCombinator):

    def __init__(self,
                 name: str,
                 step: Optional[Step] = None,
                 ports: Optional[MutableMapping[str, InputPort]] = None,
                 tag_strategy: Optional[Callable[[MutableSequence[Token]], MutableSequence[Token]]] = None):
        super().__init__(name, step, ports)
        self.lock: Lock = Lock()
        self.queue: Queue = Queue()
        self.token_lists: MutableMapping[str, List[Any]] = {}
        self.tag_strategy: Callable[
            [MutableSequence[Token]], MutableSequence[Token]] = tag_strategy or _default_tag_strategy

    async def _cartesian_multiplier(self):
        input_tasks, terminated = [], []
        for port_name, port in self.ports.items():
            input_tasks.append(asyncio.create_task(port.get(), name=port_name))
        while True:
            finished, unfinished = await asyncio.wait(input_tasks, return_when=FIRST_COMPLETED)
            input_tasks = list(unfinished)
            for task in finished:
                task_name = cast(Task, task).get_name()
                token = task.result()
                # If a TerminationToken is received, the corresponding port terminated its outputs
                if (isinstance(token, TerminationToken) or
                        (isinstance(token, MutableSequence) and utils.check_termination(token))):
                    terminated.append(task_name)
                    # When the last port terminates, the entire combinator terminates
                    if len(terminated) == len(self.ports):
                        self.queue.put_nowait([TerminationToken(self.name)])
                        return
                else:
                    # Get all combinations of the new element with the others
                    list_of_lists = []
                    for name, token_list in self.token_lists.items():
                        if name == task_name:
                            list_of_lists.append([token])
                        else:
                            list_of_lists.append(token_list)
                    cartesian_product = list(itertools.product(*list_of_lists))
                    # Put all combinations in the queue
                    for element in cartesian_product:
                        self.queue.put_nowait(self.tag_strategy(list(element)))
                    # Put the new token in the related list
                    self.token_lists[task_name].append(token)
                    # Create a new task in place of the completed one if the port is not terminated
                    if task_name not in terminated:
                        input_tasks.append(asyncio.create_task(self.ports[task_name].get(), name=task_name))

    def _handle_exception(self, task: Task):
        try:
            if exc := task.exception():
                self.queue.put_nowait(exc)
        except CancelledError:
            pass

    async def _initialize(self):
        # Initialize token lists
        for port in self.ports.values():
            self.token_lists[port.name] = []
        # Retrieve initial input tokens
        input_tasks = [asyncio.create_task(p.get()) for p in self.ports.values()]
        inputs = {k: v for (k, v) in zip(self.ports.keys(), await asyncio.gather(*input_tasks))}
        # Check for early termination and put a TerminationToken
        if utils.check_termination(list(inputs.values())):
            self.queue.put_nowait([TerminationToken(self.name)])
        # Otherwise put initial inputs in token lists and in queue and start cartesian multiplier
        else:
            for name, token in inputs.items():
                self.token_lists[name].append(token)
            self.queue.put_nowait(self.tag_strategy(list(inputs.values())))
            task = asyncio.create_task(self._cartesian_multiplier())
            task.add_done_callback(self._handle_exception)

    async def get(self) -> MutableSequence[Token]:
        # If lists are empty it means that this is the first call to the get() function
        async with self.lock:
            if not self.token_lists:
                await self._initialize()
        # Otherwise simply wait for new input tokens
        inputs = await self.queue.get()
        # If an exception has been thrown, raise it
        if isinstance(inputs, Exception):
            raise inputs
        # Otherwise, return inputs
        else:
            return flatten_list(inputs)


class DotProductOutputCombinator(OutputCombinator):

    def __init__(self,
                 name: str,
                 step: Optional[Step] = None,
                 ports: Optional[MutableMapping[str, OutputPort]] = None,
                 merge_strategy: Optional[Callable[[MutableSequence[Token]], MutableSequence[Token]]] = None):
        super().__init__(name, step, ports)
        self.lock: Lock = Lock()
        self.queues: MutableMapping[str, Queue] = {}
        self.merge_strategy: Optional[Callable[[MutableSequence[Token]], MutableSequence[Token]]] = merge_strategy
        self.token_values: MutableMapping[str, Any] = {}

    def _handle_exception(self, task: Task):
        try:
            if exc := task.exception():
                self.queues[task.get_name()].put_nowait(exc)
        except CancelledError:
            pass

    async def _initialize(self, consumer: str):
        # Initialise list for the consumer
        self.token_values[consumer] = {}
        self.queues[consumer] = Queue()
        # Retrieve initial input tokens
        input_tasks = [asyncio.create_task(p.get(consumer)) for p in self.ports.values()]
        inputs = {k: v for (k, v) in zip(self.ports.keys(), await asyncio.gather(*input_tasks))}
        # Check for early termination and put a TerminationToken
        if utils.check_termination(list(inputs.values())):
            self.queues[consumer].put_nowait([TerminationToken(self.name)])
        # Otherwise put initial inputs in token lists and in queue and start cartesian multiplier
        else:
            for name, token in inputs.items():
                if isinstance(token, MutableSequence):
                    for t in token:
                        if t.tag not in self.token_values[consumer]:
                            self.token_values[consumer][t.tag] = {}
                        self.token_values[consumer][t.tag][name] = t
                elif isinstance(token, Token):
                    if token.tag not in self.token_values[consumer]:
                        self.token_values[consumer][token.tag] = {}
                    self.token_values[consumer][token.tag][name] = token
            task = asyncio.create_task(self._retrieve(consumer), name=consumer)
            task.add_done_callback(self._handle_exception)

    async def _retrieve(self, consumer: str):
        input_tasks, terminated = [], []
        for port_name, port in self.ports.items():
            input_tasks.append(asyncio.create_task(port.get(consumer), name=port_name))
        while True:
            # Check if some complete input sets are available
            for tag in list(self.token_values[consumer].keys()):
                if len(self.token_values[consumer][tag]) == len(self.ports):
                    outputs = self.token_values[consumer].pop(tag)
                    outputs = flatten_list([outputs[k] for k in self.ports.keys()])
                    # Apply merge strategy
                    if self.merge_strategy is not None:
                        outputs = self.merge_strategy(outputs)
                    self.queues[consumer].put_nowait(outputs)
            # Wait for the next token
            finished, unfinished = await asyncio.wait(input_tasks, return_when=FIRST_COMPLETED)
            input_tasks = list(unfinished)
            for task in finished:
                task_name = cast(Task, task).get_name()
                token = task.result()
                # If a TerminationToken is received, the corresponding port terminated its outputs
                if (isinstance(token, TerminationToken) or
                        (isinstance(token, MutableSequence) and utils.check_termination(token))):
                    terminated.append(task_name)
                    # When the last port terminates, the entire combinator terminates
                    if len(terminated) == len(self.ports):
                        self.queues[consumer].put_nowait(TerminationToken(self.name))
                        return
                # Otherwise, put the value in the token lists
                elif isinstance(token, MutableSequence):
                    for t in token:
                        if t.tag not in self.token_values[consumer]:
                            self.token_values[consumer][t.tag] = {}
                        self.token_values[consumer][t.tag][task_name] = t
                elif isinstance(token, Token):
                    if token.tag not in self.token_values[consumer]:
                        self.token_values[consumer][token.tag] = {}
                    self.token_values[consumer][token.tag][task_name] = token
                # Create a new task in place of the completed one if the port is not terminated
                if task_name not in terminated:
                    input_tasks.append(asyncio.create_task(self.ports[task_name].get(consumer), name=task_name))

    def empty(self) -> bool:
        return all([p.empty() for p in self.ports.values()])

    async def get(self, consumer: str) -> Token:
        # If consumer list is empty it means that this is the first call to the get() function for this consumer
        async with self.lock:
            if consumer not in self.token_values:
                await self._initialize(consumer)
        # Otherwise simply wait for new outputs
        outputs = await self.queues[consumer].get()
        # If an exception has been thrown, raise it
        if isinstance(outputs, Exception):
            raise outputs
        # If must terminate, return a single termination token
        elif utils.check_termination(outputs):
            return TerminationToken(self.name)
        # Otherwise build the output token
        elif isinstance(outputs, MutableSequence):
            return Token(
                name=self.name,
                job=[t.job for t in outputs],
                value=outputs,
                tag=get_tag(outputs),
                weight=sum([t.weight for t in outputs]))
        else:
            return outputs

    def put(self, token: Token):
        raise NotImplementedError()


class NondeterminateMergeOutputCombinator(OutputCombinator):

    def __init__(self,
                 name: str,
                 step: Optional[Step] = None,
                 ports: Optional[MutableMapping[str, OutputPort]] = None):
        super().__init__(name, step, ports)
        self.queues: MutableMapping[str, Queue] = {}

    async def _get(self, consumer: str) -> None:
        tasks = []
        for port_name, port in self.ports.items():
            tasks.append(asyncio.create_task(port.get(consumer), name=port_name))
        while True:
            finished, unfinished = await asyncio.wait(tasks, return_when=FIRST_COMPLETED)
            tasks = list(unfinished)
            for task in finished:
                task_name = cast(Task, task).get_name()
                token = task.result()
                if not utils.check_termination(token):
                    self.queues[consumer].put_nowait(token)
                    tasks.append(asyncio.create_task(self.ports[task_name].get(consumer), name=task_name))
            if len(tasks) == 0:
                self.queues[consumer].put_nowait(TerminationToken(name=self.name))
                return

    def _handle_exception(self, task: Task):
        try:
            if exc := task.exception():
                self.queues[task.get_name()].put_nowait(exc)
        except CancelledError:
            pass

    def empty(self) -> bool:
        return all([p.empty() for p in self.ports.values()])

    async def get(self, consumer: str) -> Token:
        if consumer not in self.queues:
            self.queues[consumer] = Queue()
            task = asyncio.create_task(self._get(consumer), name=consumer)
            task.add_done_callback(self._handle_exception)
        outputs = await self.queues[consumer].get()
        # If an exception has been thrown, raise it
        if isinstance(outputs, Exception):
            raise outputs
        # Otherwise, return outputs
        else:
            return outputs

    def put(self, token: Token):
        raise NotImplementedError()
