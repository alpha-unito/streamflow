from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod
from collections.abc import MutableMapping, MutableSequence
from typing import Any

from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.workflow import Job, Status, Token


class IterationTerminationToken(Token):
    __slots__ = ()

    def __init__(self, tag: str):
        super().__init__(None, tag)

    def get_weight(self, context: StreamFlowContext):
        return 0

    def update(self, value: Any) -> Token:
        return self.__class__(tag=self.tag)

    def retag(self, tag: str) -> Token:
        raise NotImplementedError

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> IterationTerminationToken:
        return cls(tag=row["tag"])


class FileToken(Token, ABC):
    __slots__ = ()

    @abstractmethod
    async def get_paths(self, context: StreamFlowContext) -> MutableSequence[str]: ...


class JobToken(Token):
    __slots__ = ()

    async def _save_value(self, context: StreamFlowContext):
        return {"job": await self.value.save(context)}

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> JobToken:
        params = json.loads(row["value"])
        return cls(
            tag=row["tag"],
            value=await Job.load(context, params["job"], loading_context),
        )


class ListToken(Token):
    __slots__ = ()

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Token:
        value = json.loads(row["value"])
        return cls(
            tag=row["tag"],
            value=await asyncio.gather(
                *(
                    asyncio.create_task(loading_context.load_token(context, t))
                    for t in value
                )
            ),
        )

    async def _save_value(self, context: StreamFlowContext):
        await asyncio.gather(
            *(asyncio.create_task(t.save(context)) for t in self.value)
        )
        return [t.persistent_id for t in self.value]

    async def get_weight(self, context: StreamFlowContext):
        return sum(
            await asyncio.gather(
                *(asyncio.create_task(t.get_weight(context)) for t in self.value)
            )
        )

    async def is_available(self, context: StreamFlowContext):
        return all(
            await asyncio.gather(
                *(asyncio.create_task(t.is_available(context)) for t in self.value)
            )
        )


class ObjectToken(Token):
    __slots__ = ()

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Token:
        value = json.loads(row["value"])
        return cls(
            tag=row["tag"],
            value={
                k: v
                for k, v in zip(
                    value.keys(),
                    await asyncio.gather(
                        *(
                            asyncio.create_task(loading_context.load_token(context, v))
                            for v in value.values()
                        )
                    ),
                )
            },
        )

    async def _save_value(self, context: StreamFlowContext):
        await asyncio.gather(
            *(asyncio.create_task(t.save(context)) for t in self.value.values())
        )
        return {k: t.persistent_id for k, t in self.value.items()}

    async def get_weight(self, context: StreamFlowContext):
        return sum(
            await asyncio.gather(
                *(
                    asyncio.create_task(t.get_weight(context))
                    for t in self.value.values()
                )
            )
        )

    async def is_available(self, context: StreamFlowContext):
        return all(
            await asyncio.gather(
                *(
                    asyncio.create_task(t.is_available(context))
                    for t in self.value.values()
                )
            )
        )


class TerminationToken(Token):
    __slots__ = ()

    def __init__(self, value: Any = Status.COMPLETED):
        if not isinstance(value, Status):
            raise WorkflowExecutionException(
                f"Termination token received an invalid value type {type(value)}: it should be of type `Status`."
            )
        super().__init__(value)

    def get_weight(self, context: StreamFlowContext):
        return 0

    def update(self, value: Any) -> Token:
        raise NotImplementedError

    def retag(self, tag: str) -> Token:
        raise NotImplementedError

    async def _save_value(self, context: StreamFlowContext):
        return {"status": self.value.value}

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> TerminationToken:
        value = json.loads(row["value"])
        return cls(Status(value["status"]))
