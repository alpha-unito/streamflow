from __future__ import annotations

import asyncio
from typing import Any, MutableMapping, MutableSequence, TYPE_CHECKING, cast

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.deployment import Target, FilterConfig
    from streamflow.core.persistence import DatabaseLoadingContext


class Config:
    __slots__ = ("name", "type", "config")

    def __init__(self, name: str, type: str, config: MutableMapping[str, Any]) -> None:
        self.name: str = name
        self.type: str = type
        self.config: MutableMapping[str, Any] = config or {}


class BindingConfig:
    __slots__ = ("targets", "filters")

    def __init__(
        self,
        targets: MutableSequence[Target],
        filters: MutableSequence[FilterConfig] = None,
    ):
        self.targets: MutableSequence[Target] = targets
        self.filters: MutableSequence[FilterConfig] = filters or []

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
                        asyncio.create_task(loading_context.load_filter(context, f))
                        for f in row["filters"]
                    )
                ),
            ),
        )

    async def save(self, context: StreamFlowContext):
        await asyncio.gather(
            *(asyncio.create_task(t.save(context)) for t in self.targets),
            *(asyncio.create_task(f.save(context)) for f in self.filters),
        )
        return {
            "targets": [t.persistent_id for t in self.targets],
            "filters": [f.persistent_id for f in self.filters],
        }
