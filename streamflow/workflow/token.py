from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod
from typing import Any, MutableMapping, MutableSequence

from streamflow.core.context import StreamFlowContext
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.workflow import Token, Job


class IterationTerminationToken(Token):
    def __init__(self, tag: str):
        super().__init__(None, tag)

    def get_weight(self, context: StreamFlowContext):
        return 0

    def update(self, value: Any) -> Token:
        raise NotImplementedError

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
    @abstractmethod
    async def get_paths(self, context: StreamFlowContext) -> MutableSequence[str]:
        ...


class JobToken(Token):
    async def _save_value(self, context: StreamFlowContext):
        await asyncio.gather(
            *(asyncio.create_task(t.save(context)) for t in self.value.inputs.values())
        )
        job = {
            "name": self.value.name,
            "inputs": {k: v.persistent_id for k, v in self.value.inputs.items()},
            "input_directory": self.value.input_directory,
            "output_directory": self.value.output_directory,
            "tmp_directory": self.value.tmp_directory,
        }
        return job

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Token:
        value = json.loads(row["value"])
        value["inputs"] = {
            k: v
            for k, v in zip(
                value["inputs"].keys(),
                await asyncio.gather(
                    *(
                        asyncio.create_task(loading_context.load_token(context, t))
                        for t in value["inputs"].values()
                    )
                ),
            )
        }
        return cls(tag=row["tag"], value=Job(*value.values()))


class ListToken(Token):
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
            asyncio.gather(
                *(asyncio.create_task(t.is_available(context)) for t in self.value)
            )
        )


class ObjectToken(Token):
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
            asyncio.gather(
                *(
                    asyncio.create_task(t.is_available(context))
                    for t in self.value.values()
                )
            )
        )


class TerminationToken(Token):
    def __init__(self):
        super().__init__(None)

    def get_weight(self, context: StreamFlowContext):
        return 0

    def update(self, value: Any) -> Token:
        raise NotImplementedError

    def retag(self, tag: str) -> Token:
        raise NotImplementedError

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> IterationTerminationToken:
        return cls()
