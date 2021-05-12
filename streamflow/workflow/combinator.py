import asyncio
import itertools
from asyncio import Queue, Task, FIRST_COMPLETED, Lock
from typing import List, MutableMapping, Text, Any, cast, MutableSequence, Optional, Union, Callable, Set

from streamflow.core import utils
from streamflow.core.utils import flatten_list, get_tag
from streamflow.core.workflow import InputCombinator, Token, TerminationToken, InputPort, Step, OutputCombinator, \
    OutputPort, TokenProcessor, Port, Job, CommandOutput


def _get_job_name(token: Union[Token, MutableSequence[Token]]) -> Text:
    if isinstance(token, MutableSequence):
        return ",".join(token[0].job) if isinstance(token[0].job, MutableSequence) else token[0].job
    else:
        return ",".join(token.job) if isinstance(token.job, MutableSequence) else token.job


class DotProductInputCombinator(InputCombinator):

    def __init__(self,
                 name: Text,
                 step: Optional[Step] = None,
                 ports: Optional[MutableMapping[Text, InputPort]] = None):
        super().__init__(name, step, ports)
        self.token_values: MutableMapping[Text, Any] = {}

    async def get(self) -> MutableSequence[Token]:
        while True:
            # Check if some complete input sets are available
            for tag in list(self.token_values.keys()):
                if len(self.token_values[tag]) == len(self.ports):
                    return flatten_list(self.token_values.pop(tag))
            # Retrieve input tokens
            inputs = await asyncio.gather(*[asyncio.create_task(port.get()) for port in self.ports.values()])
            # Check for termination
            for token in inputs:
                # If a TerminationToken is received, the corresponding port terminated its outputs
                if utils.check_termination(token):
                    return [TerminationToken(self.name)]
                elif isinstance(token, MutableSequence):
                    for t in token:
                        if t.tag not in self.token_values:
                            self.token_values[t.tag] = []
                        self.token_values[t.tag].append(t)
                elif isinstance(token, Token):
                    if token.tag not in self.token_values:
                        self.token_values[token.tag] = []
                    self.token_values[token.tag].append(token)


class CartesianProductInputCombinator(InputCombinator):

    def __init__(self,
                 name: Text,
                 step: Optional[Step] = None,
                 ports: Optional[MutableMapping[Text, InputPort]] = None):
        super().__init__(name, step, ports)
        self.lock: Lock = Lock()
        self.queue: Queue = Queue()
        self.terminated: List[Text] = []
        self.token_lists: MutableMapping[Text, MutableMapping[Text, List[Any]]] = {}

    async def _cartesian_multiplier(self):
        input_tasks = []
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
                    self.terminated.append(task_name)
                    # When the last port terminates, the entire combinator terminates
                    if len(self.terminated) == len(self.ports):
                        self.queue.put_nowait([TerminationToken(self.name)])
                        return
                else:
                    # Get all combinations of the new element with the others
                    list_of_lists = []
                    token_job = _get_job_name(token)
                    if token_job not in self.token_lists:
                        self.token_lists[token_job] = {}
                        for port_name in self.ports:
                            self.token_lists[token_job][port_name] = []
                    for name, token_list in self.token_lists[token_job].items():
                        if name == task_name:
                            list_of_lists.append([token])
                        else:
                            list_of_lists.append(token_list)
                    cartesian_product = list(itertools.product(*list_of_lists))
                    # Put all combinations in the queue
                    for element in cartesian_product:
                        self.queue.put_nowait(list(element))
                    # Put the new token in the related list
                    self.token_lists[token_job][task_name].append(token)
                    # Create a new task in place of the completed one
                    input_tasks.append(asyncio.create_task(self.ports[task_name].get(), name=task_name))

    async def _initialize(self):
        # Retrieve initial input tokens
        input_tasks = []
        for port in self.ports.values():
            input_tasks.append(asyncio.create_task(port.get()))
        inputs = {k: v for (k, v) in zip(self.ports.keys(), await asyncio.gather(*input_tasks))}
        # Check for early termination and put a TerminationToken
        if utils.check_termination(list(inputs.values())):
            self.queue.put_nowait([TerminationToken(self.name)])
        # Otherwise put initial inputs in token lists and in queue and start cartesian multiplier
        else:
            for name, token in inputs.items():
                token_job = _get_job_name(token)
                if token_job not in self.token_lists:
                    self.token_lists[token_job] = {}
                    for port_name in self.ports:
                        self.token_lists[token_job][port_name] = []
                self.token_lists[token_job][name].append(token)
            self.queue.put_nowait(list(inputs.values()))
            asyncio.create_task(self._cartesian_multiplier())

    async def get(self) -> MutableSequence[Token]:
        # If lists are empty it means that this is the first call to the get() function
        async with self.lock:
            if not self.token_lists:
                await self._initialize()
        # Otherwise simply wait for new input tokens
        inputs = await self.queue.get()
        return flatten_list(inputs)


class DotProductOutputCombinator(OutputCombinator):

    def __init__(self,
                 name: Text,
                 step: Optional[Step] = None,
                 ports: Optional[MutableMapping[Text, OutputPort]] = None,
                 merge_strategy: Optional[Callable[[MutableSequence[Token]], MutableSequence[Token]]] = None):
        super().__init__(name, step, ports)
        self.merge_strategy: Optional[Callable[[MutableSequence[Token]], MutableSequence[Token]]] = merge_strategy
        self.token_values: MutableMapping[Text, Any] = {}

    def _merge(self, outputs: MutableSequence[Token]):
        inner_list = []
        for t in outputs:
            if isinstance(t.job, MutableSequence):
                inner_list.extend(self._merge(t.value))
            else:
                inner_list.append(t)
        return self.merge_strategy(inner_list)

    async def _retrieve(self, consumer: Text):
        while True:
            # Check if some complete input sets are available
            if consumer not in self.token_values:
                self.token_values[consumer] = {}
            for tag in list(self.token_values[consumer].keys()):
                if len(self.token_values[consumer][tag]) == len(self.ports):
                    return flatten_list(self.token_values[consumer].pop(tag))
            # Retrieve output tokens
            outputs = await asyncio.gather(*[asyncio.create_task(p.get(consumer)) for p in self.ports.values()])
            # Check for termination
            for token in outputs:
                # If a TerminationToken is received, the corresponding port terminated its outputs
                if utils.check_termination(token):
                    return [TerminationToken(self.name)]
                elif isinstance(token, MutableSequence):
                    for t in token:
                        if t.tag not in self.token_values[consumer]:
                            self.token_values[consumer][t.tag] = []
                        self.token_values[consumer][t.tag].append(t)
                elif isinstance(token, Token):
                    if token.tag not in self.token_values[consumer]:
                        self.token_values[consumer][token.tag] = []
                    self.token_values[consumer][token.tag].append(token)

    def empty(self) -> bool:
        return all([p.empty() for p in self.ports.values()])

    async def get(self, consumer: Text) -> Token:
        outputs = await self._retrieve(consumer)
        # Check for termination
        if utils.check_termination(outputs):
            return TerminationToken(self.name)
        # Return token
        outputs = flatten_list(outputs)
        if self.merge_strategy is not None:
            outputs = self._merge(outputs)
        if isinstance(outputs, MutableSequence):
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
                 name: Text,
                 step: Optional[Step] = None,
                 ports: Optional[MutableMapping[Text, OutputPort]] = None):
        super().__init__(name, step, ports)
        self.queues: MutableMapping[Text, Queue] = {}

    async def _get(self, consumer: Text) -> None:
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

    def empty(self) -> bool:
        return all([p.empty() for p in self.ports.values()])

    async def get(self, consumer: Text) -> Token:
        if consumer not in self.queues:
            self.queues[consumer] = Queue()
            asyncio.create_task(self._get(consumer))
        return await self.queues[consumer].get()

    def put(self, token: Token):
        raise NotImplementedError()
