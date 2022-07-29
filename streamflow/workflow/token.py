import asyncio
import json
from abc import ABC, abstractmethod
from typing import Any, MutableSequence

from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Token


class IterationTerminationToken(Token):

    def __init__(self, tag: str):
        super().__init__(None, tag)

    def get_weight(self, context: StreamFlowContext):
        return 0

    def update(self, value: Any) -> Token:
        raise NotImplementedError()

    def retag(self, tag: str) -> Token:
        raise NotImplementedError()


class FileToken(Token, ABC):

    @abstractmethod
    async def get_paths(self, context: StreamFlowContext) -> MutableSequence[str]:
        ...


class JobToken(Token):

    def save(self):
        return json.dumps(self.value.name)


class ListToken(Token):

    async def get_weight(self, context: StreamFlowContext):
        return sum(await asyncio.gather(*(asyncio.create_task(t.get_weight(context)) for t in self.value)))

    def save(self):
        return json.dumps([json.loads(t.save()) for t in self.value])


class ObjectToken(Token):

    async def get_weight(self, context: StreamFlowContext):
        return sum(await asyncio.gather(*(asyncio.create_task(t.get_weight(context)) for t in self.value.values())))

    def save(self):
        return json.dumps({k: json.loads(t.save()) for k, t in self.value.items()})


class TerminationToken(Token):

    def __init__(self):
        super().__init__(None)

    def get_weight(self, context: StreamFlowContext):
        return 0

    def update(self, value: Any) -> Token:
        raise NotImplementedError()

    def retag(self, tag: str) -> Token:
        raise NotImplementedError()
