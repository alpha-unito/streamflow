from __future__ import annotations

import asyncio
from typing import Any, MutableMapping, MutableSequence, TYPE_CHECKING, cast

from streamflow.core.persistence import PersistableEntity

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.deployment import Target
    from streamflow.core.persistence import DatabaseLoadingContext


class Config(PersistableEntity):
    __slots__ = ("name", "type", "config")

    def __init__(self, name: str, type: str, config: MutableMapping[str, Any]) -> None:
        super().__init__()
        self.name: str = name
        self.type: str = type
        self.config: MutableMapping[str, Any] = config or {}

    @classmethod
    async def load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Config:
        return cls(row["name"], row["type"], row["config"])

    async def save(self, context: StreamFlowContext):
        return {"name": self.name, "type": self.type, "config": self.config}


class BindingConfig:
    __slots__ = ("targets", "filters")

    def __init__(
        self, targets: MutableSequence[Target], filters: MutableSequence[Config] = None
    ):
        self.targets: MutableSequence[Target] = targets
        self.filters: MutableSequence[Config] = filters or []

    @classmethod
    async def load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> BindingConfig:
        return cls(
            targets=cast(
                MutableSequence,
                await asyncio.gather(
                    *(
                        asyncio.create_task(loading_context.load_target(context, t))
                        for t in row["targets"]
                    )
                ),
            ),
            filters=cast(
                MutableSequence,
                await asyncio.gather(
                    *(
                        asyncio.create_task(Config.load(context, f, loading_context))
                        for f in row["filters"]
                    )
                ),
            ),
        )

    async def save(self, context: StreamFlowContext):
        await asyncio.gather(
            *(asyncio.create_task(t.save(context)) for t in self.targets)
        )
        return {
            "targets": [t.persistent_id for t in self.targets],
            "filters": await asyncio.gather(
                *(asyncio.create_task(f.save(context)) for f in self.filters)
            ),
        }
