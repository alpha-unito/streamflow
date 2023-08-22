from __future__ import annotations

import json
import os
import posixpath
from typing import MutableMapping

import pkg_resources
from referencing import Registry, Resource

from streamflow.core.context import SchemaEntity
from streamflow.core.exception import WorkflowDefinitionException
from streamflow.cwl.requirement.docker import (
    cwl_docker_translator_classes,
)
from streamflow.data import data_manager_classes
from streamflow.deployment import deployment_manager_classes
from streamflow.deployment.connector import connector_classes
from streamflow.deployment.filter import binding_filter_classes
from streamflow.persistence import database_classes
from streamflow.recovery import (
    checkpoint_manager_classes,
    failure_manager_classes,
)
from streamflow.scheduling import scheduler_classes
from streamflow.scheduling.policy import policy_classes

_CONFIGS = {
    "v1.0": "https://streamflow.di.unito.it/schemas/config/v1.0/config_schema.json"
}


ext_schemas = [
    pkg_resources.resource_filename(
        "streamflow.deployment.connector", os.path.join("schemas", "queue_manager.json")
    )
]


class SfSchema:
    def __init__(self):
        self.registry: Registry = Registry()
        for version in _CONFIGS.keys():
            schema = pkg_resources.resource_filename(
                __name__, os.path.join("schemas", version, "config_schema.json")
            )
            self.add_schema(schema)
        self.inject_ext(binding_filter_classes, "bindingFilter")
        self.inject_ext(checkpoint_manager_classes, "checkpointManager")
        self.inject_ext(cwl_docker_translator_classes, "cwl/docker")
        self.inject_ext(database_classes, "database")
        self.inject_ext(data_manager_classes, "dataManager")
        self.inject_ext(connector_classes, "deployment")
        self.inject_ext(deployment_manager_classes, "deploymentManager")
        self.inject_ext(failure_manager_classes, "failureManager")
        self.inject_ext(policy_classes, "policy")
        self.inject_ext(scheduler_classes, "scheduler")
        for schema in ext_schemas:
            self.add_schema(schema)
        self.registry = self.registry.crawl()

    def add_schema(self, schema: str) -> Resource:
        with open(schema) as f:
            resource = Resource.from_contents(json.load(f))
            self.registry = resource @ self.registry
            return resource

    def get_config(self, version: str) -> Resource:
        if version not in _CONFIGS:
            raise WorkflowDefinitionException(
                f"Version {version} is unsupported. The `version` clause should be equal to `v1.0`."
            )
        return self.registry.get(_CONFIGS[version])

    def inject_ext(
        self,
        classes: MutableMapping[str, type[SchemaEntity]],
        definition_name: str,
    ):
        for name, entity in classes.items():
            if entity_schema := entity.get_schema():
                entity_schema = self.add_schema(entity_schema).contents
                for config_id in _CONFIGS.values():
                    config = self.registry.contents(config_id)
                    definition = config["$defs"]
                    for el in definition_name.split(posixpath.sep):
                        definition = definition[el]
                    definition["properties"]["type"].setdefault("enum", []).append(name)
                    definition["$defs"][name] = entity_schema
                    definition.setdefault("allOf", []).append(
                        {
                            "if": {"properties": {"type": {"const": name}}},
                            "then": {"properties": {"config": entity_schema}},
                        }
                    )
