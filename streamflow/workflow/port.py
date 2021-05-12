from __future__ import annotations

import asyncio
import copy
import posixpath
from asyncio import Event, Queue
from typing import TYPE_CHECKING, List, MutableMapping, MutableSequence, Optional, cast, Type, Callable

from typing_extensions import Text

from streamflow.core.exception import WorkflowDefinitionException
from streamflow.core.utils import get_tag
from streamflow.core.workflow import TokenProcessor, InputPort, OutputPort, Token, TerminationToken, Port, CommandOutput

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.workflow import Step, Job
    from typing import Any, Set


def _get_tag(tokens: MutableSequence[Token]):
    output_tag = '/'
    for tag in [posixpath.dirname(t.tag) for t in tokens]:
        if len(tag) > len(output_tag):
            output_tag = tag
    return output_tag


class DefaultTokenProcessor(TokenProcessor):

    async def collect_output(self, token: Token, output_dir: Text) -> Token:
        if isinstance(token.job, MutableSequence):
            return token.update(await asyncio.gather(*[asyncio.create_task(
                self.collect_output(t if isinstance(t, Token) else token.update(t), output_dir)
            ) for t in token.value]))
        else:
            return token

    async def compute_token(self, job: Job, command_output: CommandOutput) -> Token:
        return Token(
            name=self.port.name,
            value=command_output.value,
            job=job.name,
            tag=get_tag(job.inputs))

    def get_context(self) -> StreamFlowContext:
        return self.port.step.context

    def get_related_resources(self, token: Token) -> Set[Text]:
        return set()

    async def recover_token(self, job: Job, resources: MutableSequence[Text], token: Token) -> Token:
        if isinstance(token.job, MutableSequence):
            return token.update(await asyncio.gather(*[asyncio.create_task(
                self.recover_token(job, resources, t)
            ) for t in token.value]))
        return token

    async def update_token(self, job: Job, token: Token) -> Token:
        if isinstance(token.job, MutableSequence):
            return token.update(await asyncio.gather(*[asyncio.create_task(
                self.update_token(job, t)
            ) for t in token.value]))
        else:
            return token

    async def weight_token(self, job: Job, token_value: Any) -> int:
        return 0


class ListTokenProcessor(DefaultTokenProcessor):

    def __init__(self,
                 port: Port,
                 processors: MutableSequence[TokenProcessor]):
        super().__init__(port)
        self.processors: MutableSequence[TokenProcessor] = processors

    def _check_list(self, value: Any):
        if not isinstance(value, MutableSequence):
            raise WorkflowDefinitionException(
                "A {this} object can only be used to process list values".format(this=self.__class__.__name__))

    async def collect_output(self, token: Token, output_dir: Text) -> Token:
        if isinstance(token.job, MutableSequence):
            return await super().collect_output(token, output_dir)
        self._check_list(token.value)
        output_tasks = []
        for i, processor in enumerate(self.processors):
            if i < len(token.value):
                partial_token = token.update(token.value[i])
                output_tasks.append(asyncio.create_task(processor.collect_output(partial_token, output_dir)))
        return token.update([t.value for t in await asyncio.gather(*output_tasks)])

    def get_related_resources(self, token: Token) -> Set[Text]:
        self._check_list(token.value)
        related_resources = set()
        for i, processor in enumerate(self.processors):
            if i < len(token.value):
                partial_token = token.update(token.value[i])
                related_resources.update(processor.get_related_resources(partial_token))
        return related_resources

    async def recover_token(self, job: Job, resources: MutableSequence[Text], token: Token) -> Token:
        if isinstance(token.job, MutableSequence):
            return await super().recover_token(job, resources, token)
        self._check_list(token.value)
        token_tasks = []
        for i, processor in enumerate(self.processors):
            if i < len(token.value):
                partial_token = token.update(token.value[i])
                token_tasks.append(asyncio.create_task(processor.recover_token(job, resources, partial_token)))
        return token.update([t.value for t in await asyncio.gather(*token_tasks)])

    async def update_token(self, job: Job, token: Token) -> Token:
        if isinstance(token.job, MutableSequence):
            return await super().update_token(job, token)
        self._check_list(token.value)
        token_tasks = []
        for i, processor in enumerate(self.processors):
            if i < len(token.value):
                partial_token = token.update(token.value[i])
                token_tasks.append(asyncio.create_task(processor.update_token(job, partial_token)))
        return token.update([t.value for t in await asyncio.gather(*token_tasks)])

    async def weight_token(self, job: Job, token_value: Any) -> int:
        self._check_list(token_value)
        weight = 0
        for i, processor in enumerate(self.processors):
            if i < len(token_value):
                weight += processor.weight_token(job, token_value[i])
        return weight


class MapTokenProcessor(DefaultTokenProcessor):

    def __init__(self,
                 port: Port,
                 token_processor: TokenProcessor):
        super().__init__(port)
        self.processor: TokenProcessor = token_processor

    def _check_list(self, value: Any):
        if not isinstance(value, MutableSequence):
            raise WorkflowDefinitionException(
                "A {this} object can only be used to process list values".format(this=self.__class__.__name__))

    async def collect_output(self, token: Token, output_dir: Text) -> Token:
        if isinstance(token.job, MutableSequence):
            return await super().collect_output(token, output_dir)
        self._check_list(token.value)
        token_tasks = []
        for i, v in enumerate(token.value):
            token_tasks.append(asyncio.create_task(self.processor.collect_output(token.update(v), output_dir)))
        return token.update([t.value for t in await asyncio.gather(*token_tasks)])

    async def compute_token(self, job: Job, command_output: CommandOutput) -> Token:
        if isinstance(command_output.value, MutableSequence):
            token_list = await asyncio.gather(*[asyncio.create_task(
                self.processor.compute_token(job, command_output.update(value))
            ) for value in command_output.value])
            token = Token(
                name=self.port.name,
                value=[t.value for t in token_list],
                job=job.name,
                tag=get_tag(job.inputs))
        else:
            token = await self.processor.compute_token(job, command_output)
        token.value = ([] if token.value is None else
                       [token.value] if not isinstance(token.value, MutableSequence) else
                       token.value)
        return token

    def get_related_resources(self, token: Token) -> Set[Text]:
        self._check_list(token.value)
        related_resources = set()
        for v in token.value:
            related_resources.update(self.processor.get_related_resources(token.update(v)))
        return related_resources

    async def recover_token(self, job: Job, resources: MutableSequence[Text], token: Token) -> Token:
        if isinstance(token.job, MutableSequence):
            recover_tasks = []
            for t in token.value:
                recover_tasks.append(asyncio.create_task(self.processor.recover_token(job, resources, t)))
            return token.update(await asyncio.gather(*recover_tasks))
        self._check_list(token.value)
        token_tasks = []
        for i, v in enumerate(token.value):
            token_tasks.append(asyncio.create_task(self.processor.recover_token(job, resources, token.update(v))))
        return token.update([t.value for t in await asyncio.gather(*token_tasks)])

    async def update_token(self, job: Job, token: Token) -> Token:
        if isinstance(token.job, MutableSequence):
            update_tasks = []
            for t in token.value:
                update_tasks.append(asyncio.create_task(self.processor.update_token(job, t)))
            return token.update(await asyncio.gather(*update_tasks))
        self._check_list(token.value)
        token_tasks = []
        for i, v in enumerate(token.value):
            token_tasks.append(asyncio.create_task(self.processor.update_token(job, token.update(v))))
        return token.update([t.value for t in await asyncio.gather(*token_tasks)])

    async def weight_token(self, job: Job, token_value: Any) -> int:
        self._check_list(token_value)
        weight = 0
        for v in token_value:
            weight += self.processor.weight_token(job, v)
        return weight


class ObjectTokenProcessor(DefaultTokenProcessor):

    def _check_dict(self, value: Any):
        if not isinstance(value, MutableMapping):
            raise WorkflowDefinitionException(
                "A {this} object can only be used to process dict values".format(this=self.__class__.__name__))

    def __init__(self,
                 port: Port,
                 processors: MutableMapping[Text, TokenProcessor]):
        super().__init__(port)
        self.processors: MutableMapping[Text, TokenProcessor] = processors

    async def collect_output(self, token: Token, output_dir: Text) -> Token:
        if isinstance(token.job, MutableSequence):
            return await super().collect_output(token, output_dir)
        self._check_dict(token.value)
        output_tasks = []
        for key, processor in self.processors.items():
            if key in token.value:
                partial_token = token.update(token.value[key])
                output_tasks.append(asyncio.create_task(processor.collect_output(partial_token, output_dir)))
        return token.update(
            dict(zip(token.value.keys(), [t.value for t in await asyncio.gather(*output_tasks)])))

    async def compute_token(self, job: Job, command_output: CommandOutput) -> Token:
        if isinstance(command_output.value, MutableSequence):
            token_value = [t.value for t in await asyncio.gather(*[asyncio.create_task(
                self.compute_token(job, command_output.update(cv))
            ) for cv in command_output.value])]
            return Token(
                name=self.port.name,
                value=token_value,
                job=job.name,
                tag=get_tag(job.inputs))
        if isinstance(command_output.value, MutableMapping):
            if self.port.name in command_output.value:
                return await self.compute_token(job, command_output.update(command_output.value[self.port.name]))
            else:
                token_tasks = {}
                for key, processor in self.processors.items():
                    if key in command_output.value:
                        partial_command = command_output.update(command_output.value[key])
                        token_tasks[key] = asyncio.create_task(processor.compute_token(job, partial_command))
                token_value = dict(
                    zip(token_tasks.keys(), [t.value for t in await asyncio.gather(*token_tasks.values())]))
                return Token(
                    name=self.port.name,
                    value=token_value,
                    job=job.name,
                    tag=get_tag(job.inputs))
        else:
            token_tasks = {}
            for key, processor in self.processors.items():
                token_tasks[key] = asyncio.create_task(processor.compute_token(job, command_output))
            token_value = dict(
                zip(token_tasks.keys(), [t.value for t in await asyncio.gather(*token_tasks.values())]))
            return Token(
                name=self.port.name,
                value=token_value,
                job=job.name,
                tag=get_tag(job.inputs))

    def get_related_resources(self, token: Token) -> Set[Text]:
        self._check_dict(token.value)
        related_resources = set()
        for key, processor in self.processors.items():
            if key in token.value:
                partial_token = token.update(token.value[key])
                related_resources.update(processor.get_related_resources(partial_token))
        return related_resources

    async def recover_token(self, job: Job, resources: MutableSequence[Text], token: Token) -> Token:
        if isinstance(token.job, MutableSequence):
            return await super().recover_token(job, resources, token)
        self._check_dict(token.value)
        token_tasks = {}
        for key, processor in self.processors.items():
            if key in token.value:
                partial_token = token.update(token.value[key])
                token_tasks[key] = asyncio.create_task(processor.recover_token(job, resources, partial_token))
        return token.update(
            dict(zip(token_tasks.keys(), [t.value for t in await asyncio.gather(*token_tasks.values())])))

    async def update_token(self, job: Job, token: Token) -> Token:
        if isinstance(token.job, MutableSequence):
            return await super().update_token(job, token)
        self._check_dict(token.value)
        token_tasks = {}
        for key, processor in self.processors.items():
            if key in token.value:
                partial_token = token.update(token.value[key])
                token_tasks[key] = asyncio.create_task(processor.update_token(job, partial_token))
        return token.update(
            dict(zip(token_tasks.keys(), [t.value for t in await asyncio.gather(*token_tasks.values())])))

    async def weight_token(self, job: Job, token_value: Any) -> int:
        self._check_dict(token_value)
        weight = 0
        for key, processor in self.processors.items():
            if key in token_value:
                weight += processor.weight_token(job, token_value[key])
        return weight


class UnionTokenProcessor(DefaultTokenProcessor):

    def __init__(self,
                 port: Port,
                 processors: MutableSequence[TokenProcessor]):
        super().__init__(port)
        self.processors: MutableSequence[TokenProcessor] = processors
        self.check_processor: MutableMapping[Type[TokenProcessor], Callable] = {
            DefaultTokenProcessor: self._check_default_token_processor,
            MapTokenProcessor: self._check_map_processor,
            ObjectTokenProcessor: self._check_object_processor,
            UnionTokenProcessor: self._check_union_processor,
        }

    # noinspection PyMethodMayBeStatic
    def _check_default_token_processor(self, processor: DefaultTokenProcessor, token_value: Any):
        return True

    def _check_map_processor(self, processor: MapTokenProcessor, token_value: Any):
        if isinstance(token_value, MutableSequence):
            return self.check_processor[type(processor.processor)](processor.processor, token_value[0])
        else:
            return False

    def _check_object_processor(self, processor: ObjectTokenProcessor, token_value: Any):
        if isinstance(token_value, MutableMapping):
            for k, v in token_value.items():
                if not (k in processor.processors
                        and self.check_processor[type(processor.processors[k])](processor.processors[k], v)):
                    return False
            return True
        else:
            return False

    def _check_union_processor(self, processor: UnionTokenProcessor, token_value: Any):
        for p in processor.processors:
            if self.check_processor[type(p)](p, token_value):
                return True
        return False

    def get_processor(self, token_value: Any) -> TokenProcessor:
        for processor in self.processors:
            if self.check_processor[type(processor)](processor, token_value):
                return processor
        raise WorkflowDefinitionException("No suitable processors for token value " + str(token_value))

    async def compute_token(self, job: Job, command_output: CommandOutput) -> Token:
        processor = self.get_processor(command_output.value)
        return await processor.compute_token(job, command_output)

    async def collect_output(self, token: Token, output_dir: Text) -> Token:
        if isinstance(token.job, MutableSequence):
            return await super().collect_output(token, output_dir)
        processor = self.get_processor(token.value)
        return await processor.collect_output(token, output_dir)

    def get_related_resources(self, token: Token) -> Set[Text]:
        processor = self.get_processor(token.value)
        return processor.get_related_resources(token)

    async def recover_token(self, job: Job, resources: MutableSequence[Text], token: Token) -> Token:
        if isinstance(token.job, MutableSequence):
            return await super().recover_token(job, resources, token)
        processor = self.get_processor(token.value)
        return await processor.recover_token(job, resources, token)

    async def update_token(self, job: Job, token: Token) -> Token:
        if isinstance(token.job, MutableSequence):
            return await super().update_token(job, token)
        processor = self.get_processor(token.value)
        return await processor.update_token(job, token)

    async def weight_token(self, job: Job, token_value: Any) -> int:
        processor = self.get_processor(token_value)
        return await processor.weight_token(job, token_value)


class DefaultInputPort(InputPort):

    def __init__(self,
                 name: Text,
                 step: Optional[Step] = None):
        super().__init__(name, step)
        self.fireable: Event = Event()

    async def get(self) -> Token:
        token = await self.dependee.get(posixpath.join(self.step.name, self.name))
        token.name = self.name
        return token


class DefaultOutputPort(OutputPort):

    def __init__(self,
                 name: Text,
                 step: Optional[Step] = None):
        super().__init__(name, step)
        self.fireable: Event = Event()
        self.step_queues: MutableMapping[Text, Queue] = {}
        self.token: MutableSequence[Token] = []

    def _init_consumer(self, consumer_name: Text):
        self.step_queues[consumer_name] = Queue()
        for t in self.token:
            self.step_queues[consumer_name].put_nowait(copy.deepcopy(t))

    def empty(self) -> bool:
        return not self.token

    def put(self, token: Token):
        self.token.append(token)
        for q in self.step_queues.values():
            q.put_nowait(copy.deepcopy(token))
        self.fireable.set()

    async def get(self, consumer: Text) -> Token:
        await self.fireable.wait()
        if consumer not in self.step_queues:
            self._init_consumer(consumer)
        return await self.step_queues[consumer].get()


class ScatterInputPort(DefaultInputPort):

    def __init__(self,
                 name: Text,
                 step: Optional[Step] = None):
        super().__init__(name, step)
        self.queue: List = []

    async def _build_token(self, job_name: Text, token_value: Any, count: int) -> Token:
        job = self.step.context.scheduler.get_job(job_name)
        weight = await self.token_processor.weight_token(job, token_value)
        return Token(
            name=self.name,
            value=token_value,
            job=job_name,
            tag=posixpath.join(get_tag(job.inputs if job is not None else []), str(count)),
            weight=weight)

    async def get(self) -> Any:
        while not self.queue:
            token = await self.dependee.get(posixpath.join(self.step.name, self.name))
            if isinstance(token, TerminationToken) or token.value is None:
                self.queue = [token.rename(self.name)]
            elif isinstance(token.job, MutableSequence):
                self.queue = [t.rename(self.name) for t in token.value]
            elif isinstance(token.value, MutableSequence):
                self.queue = await asyncio.gather(*[asyncio.create_task(
                    self._build_token(cast(Text, token.job), t, i)
                ) for i, t in enumerate(token.value)])
            else:
                raise WorkflowDefinitionException("Scatter ports require iterable inputs")
        return self.queue.pop(0)


class GatherOutputPort(DefaultOutputPort):

    def __init__(self,
                 name: Text,
                 step: Optional[Step] = None,
                 merge_strategy: Optional[Callable[[MutableSequence[Token]], MutableSequence[Token]]] = None):
        super().__init__(name, step)
        self.merge_strategy: Optional[Callable[[MutableSequence[Token]], MutableSequence[Any]]] = merge_strategy

    def put(self, token: Token):
        if isinstance(token, TerminationToken):
            token_list = self.token
            if token_list:
                self.token = [Token(
                    name=self.name,
                    job=[t.job for t in token_list],
                    tag=_get_tag(token_list),
                    value=self.merge_strategy(token_list) if self.merge_strategy else token_list)]
                self.token.append(token)
            else:
                self.token = [token]
            self.fireable.set()
        else:
            self.token.append(token)
