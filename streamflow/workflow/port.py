from __future__ import annotations

import asyncio
import copy
import os
from asyncio import Event, Queue
from typing import TYPE_CHECKING, List, Iterable

from streamflow.core.scheduling import JobStatus
from streamflow.core.workflow import TokenProcessor, InputPort, OutputPort, Token, TerminationToken
from streamflow.workflow.exception import WorkflowDefinitionException

if TYPE_CHECKING:
    from streamflow.core.workflow import Task, Job
    from typing import Any, MutableMapping
    from typing_extensions import Text


class DefaultTokenProcessor(TokenProcessor):

    async def collect_output(self, token: Token, output_dir: Text) -> None:
        if isinstance(token.job, List):
            update_tasks = []
            for t in token.value:
                update_tasks.append(asyncio.create_task(self.collect_output(t, output_dir)))
            await asyncio.gather(*update_tasks)
        else:
            file_path = os.path.join(output_dir, token.name)
            with open(file_path, mode='w') as file:
                file.write(str(token.value))

    async def compute_token(self, job: Job, result: Any, status: JobStatus) -> Token:
        return Token(name=self.port.name, value=result, job=job.name, weight=0)

    async def update_token(self, job: Job, token: Token) -> Token:
        if isinstance(token.job, List):
            update_tasks = []
            for t in token.value:
                update_tasks.append(asyncio.create_task(self.update_token(job, t)))
            token_list = await asyncio.gather(*update_tasks)
            return Token(name=token.name, job=token.job, value=[t.value for t in token_list], weight=token.weight)
        else:
            return token

    async def weight_token(self, job: Job, token_value: Any) -> int:
        return 0


class DefaultInputPort(InputPort):

    def __init__(self, name: Text):
        super().__init__(name)
        self.fireable: Event = Event()
        self.token: List[Token] = []

    async def get(self) -> Token:
        token = await self.dependee.get(self.task)
        token.name = self.name
        return token


class DefaultOutputPort(OutputPort):

    def __init__(self, name: Text):
        super().__init__(name)
        self.fireable: Event = Event()
        self.task_queues: MutableMapping[Text, Queue] = {}
        self.token: List[Token] = []

    def _init_consumer(self, task: Task):
        if task.name not in self.task_queues:
            self.task_queues[task.name] = Queue()
            for t in self.token:
                self.task_queues[task.name].put_nowait(copy.deepcopy(t))

    def put(self, token: Token):
        self.token.append(token)
        for q in self.task_queues.values():
            q.put_nowait(copy.deepcopy(token))
        self.fireable.set()

    async def get(self, task: Task) -> Token:
        await self.fireable.wait()
        self._init_consumer(task)
        return await self.task_queues[task.name].get()


class ScatterInputPort(DefaultInputPort):

    def __init__(self, name: Text):
        super().__init__(name)
        self.queue: List = []

    async def _build_token(self, job_name: Text, token_value: Any) -> Token:
        job = self.task.context.scheduler.get_job(job_name)
        weight = await self.token_processor.weight_token(job, token_value)
        return Token(name=self.name, value=token_value, job=job_name, weight=weight)

    async def get(self) -> Any:
        if not self.queue:
            token = await self.dependee.get(self.task)
            if isinstance(token, TerminationToken):
                self.queue.append(token)
            elif isinstance(token.value, Iterable):
                token_tasks = []
                for t in token.value:
                    token_tasks.append(asyncio.create_task(self._build_token(token.job, t)))
                token_list = await asyncio.gather(*token_tasks)
                for t in token_list:
                    self.queue.append(t)
            else:
                raise WorkflowDefinitionException("Scatter ports require iterable inputs")
        return self.queue.pop(0)


class GatherOutputPort(DefaultOutputPort):

    def put(self, token: Token):
        if isinstance(token, TerminationToken):
            token_list = self.token
            self.token = [Token(name=self.name, job=[t.job for t in token_list], value=token_list, weight=0)]
            self.token.append(token)
            self.fireable.set()
        else:
            self.token.append(token)
