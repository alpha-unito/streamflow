from __future__ import annotations

import asyncio
import json
import posixpath
from collections.abc import MutableMapping, MutableSequence
from typing import TYPE_CHECKING, Any

from referencing import Registry, Resource
from typing_extensions import Self

from streamflow.core.exception import WorkflowDefinitionException

if TYPE_CHECKING:
    from typing import TypeVar

    from streamflow.core.context import SchemaEntity, StreamFlowContext
    from streamflow.core.deployment import FilterConfig, Target
    from streamflow.core.persistence import DatabaseLoadingContext

    SchemaEntityType = TypeVar("SchemaEntityType", bound=SchemaEntity)


class Config:
    __slots__ = ("name", "type", "config")

    def __init__(self, name: str, type: str, config: MutableMapping[str, Any]) -> None:
        self.name: str = name
        self.type: str = type
        self.config: MutableMapping[str, Any] = config or {}

    @classmethod
    async def load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        return cls(name=row["name"], type=row["type"], config=row["config"])

    async def save(self, context: StreamFlowContext):
        return {"name": self.name, "type": self.type, "config": self.config}


class BindingConfig:
    __slots__ = ("targets", "filters")

    def __init__(
        self,
        targets: MutableSequence[Target],
        filters: MutableSequence[FilterConfig] | None = None,
    ):
        self.targets: MutableSequence[Target] = targets
        self.filters: MutableSequence[FilterConfig] = filters or []

    @classmethod
    async def load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        return cls(
            targets=await asyncio.gather(
                *(
                    asyncio.create_task(loading_context.load_target(context, t))
                    for t in row["targets"]
                )
            ),
            filters=await asyncio.gather(
                *(
                    asyncio.create_task(loading_context.load_filter(context, f))
                    for f in row["filters"]
                )
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


class Schema:
    def __init__(self, configs: MutableMapping[str, str]):
        self.configs: MutableMapping[str, str] = configs
        self.registry: Registry = Registry()

    def add_schema(self, schema: str, embed: bool = False) -> Resource:
        resource = Resource.from_contents(json.loads(schema))
        self.registry = resource @ self.registry
        entity_schema = resource.contents
        if embed:
            for config_id in self.configs.values():
                config = self.registry.contents(config_id)
                config["$defs"][entity_schema["$id"]] = entity_schema
        return resource

    def dump(self, version: str, pretty: bool = False) -> str:
        if version not in self.configs:
            raise WorkflowDefinitionException(
                f"Version {version} is unsupported. The `version` clause should be equal to `v1.0`."
            )
        output = self.registry.contents(self.configs[version])
        return json.dumps(output, indent=4) if pretty else json.dumps(output)

    def get_config(self, version: str) -> Resource:
        if version not in self.configs:
            raise WorkflowDefinitionException(
                f"Version {version} is unsupported. The `version` clause should be equal to `v1.0`."
            )
        return self.registry[self.configs[version]]

    def inject_ext(
        self,
        classes: MutableMapping[str, type[SchemaEntityType]],
        definition_name: str,
    ) -> None:
        for name, entity in classes.items():
            if entity_schema := entity.get_schema():
                entity_schema = self.add_schema(entity_schema, embed=True).contents
                for config_id in self.configs.values():
                    config = self.registry.contents(config_id)
                    definition = config["$defs"]
                    for el in definition_name.split(posixpath.sep):
                        definition = definition[el]
                    definition["properties"]["type"].setdefault("enum", []).append(name)
                    definition.setdefault("allOf", []).append(
                        {
                            "if": {"properties": {"type": {"const": name}}},
                            "then": {
                                "properties": {
                                    "config": {
                                        "type": "object",
                                        "title": "Configuration",
                                        "$ref": entity_schema["$id"],
                                    }
                                }
                            },
                        }
                    )
