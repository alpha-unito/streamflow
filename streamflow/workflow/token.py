import asyncio
from abc import ABC, abstractmethod
from typing import MutableSequence, Any

from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Token


class FileToken(Token, ABC):

    @abstractmethod
    async def get_paths(self, context: StreamFlowContext) -> MutableSequence[str]:
        ...


class ListToken(Token):

    async def get_weight(self, context: StreamFlowContext):
        return sum(await asyncio.gather(*(asyncio.create_task(t.get_weight(context)) for t in self.value)))


class ObjectToken(Token):

    async def get_weight(self, context: StreamFlowContext):
        return sum(await asyncio.gather(*(asyncio.create_task(t.get_weight(context)) for t in self.value.values())))


class TerminationToken(Token):

    def __init__(self):
        super().__init__(None)

    def get_weight(self, context: StreamFlowContext):
        return 0

    def update(self, value: Any) -> Token:
        raise NotImplementedError()

    def retag(self, tag: str) -> Token:
        raise NotImplementedError()
