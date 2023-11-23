from __future__ import annotations

import asyncio
import json
from typing import Any, MutableMapping, MutableSequence, TYPE_CHECKING, cast, Type

from streamflow.core import utils
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
        persistent_id: int,
        loading_context: DatabaseLoadingContext,
    ) -> Config:
        row = await context.database.get_config(persistent_id)
        type = cast(Type[Config], utils.get_class_from_name(row["type"]))
        config = await type._load(context, row, loading_context)
        config.persistent_id = persistent_id
        loading_context.add_config(persistent_id, config)
        return config

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Config:
        return cls(row["name"], row["attr_type"], json.loads(row["config"]))

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return {}

    async def save(self, context: StreamFlowContext) -> None:
        async with self.persistence_lock:
            if not self.persistent_id:
                self.persistent_id = await context.database.add_config(
                    name=self.name,
                    attr_type=self.type,
                    config=self.config,
                    type=type(self),
                    params=await self._save_additional_params(context),
                )


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
                        asyncio.create_task(loading_context.load_config(context, f))
                        for f in row["filters"]
                    )
                ),
            ),
        )

    async def save(self, context: StreamFlowContext):
        await asyncio.gather(
            *(asyncio.create_task(t.save(context)) for t in self.targets)
        )
        await asyncio.gather(
            *(asyncio.create_task(f.save(context)) for f in self.filters)
        )
        return {
            "targets": [t.persistent_id for t in self.targets],
            "filters": [f.persistent_id for f in self.filters],
        }
