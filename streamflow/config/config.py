from __future__ import annotations

import logging
from collections.abc import MutableMapping, MutableSequence
from pathlib import PurePosixPath
from typing import Any

from streamflow.core.config import Config
from streamflow.core.exception import WorkflowDefinitionException
from streamflow.log_handler import logger


def set_targets(current_node, target):
    for node in current_node["children"].values():
        if "port" in node:
            continue
        if "step" not in node:
            node["step"] = target
        set_targets(node, node["step"])


class WorkflowConfig(Config):
    def __init__(self, name: str, config: MutableMapping[str, Any]) -> None:
        workflow_config = config["workflows"][name]
        super().__init__(
            name=name, type=workflow_config["type"], config=workflow_config["config"]
        )
        self.deployments = config.get("deployments", {})
        self.policies = {
            k: Config(name=k, type=v["type"], config=v["config"])
            for k, v in config.get("scheduling", {}).get("policies", {}).items()
        }
        self.policies["__DEFAULT__"] = Config(
            name="__DEFAULT__", type="data_locality", config={}
        )
        self.binding_filters = {
            k: Config(name=k, type=v["type"], config=v["config"])
            for k, v in config.get("bindingFilters", {}).items()
        }
        if not self.deployments:
            self.deployments = config.get("models", {})
            if self.deployments:
                if logger.isEnabledFor(logging.WARNING):
                    logger.warning(
                        "The `models` keyword is deprecated and will be removed in StreamFlow 0.3.0. "
                        "Use `deployments` instead."
                    )
        for deployment_config in self.deployments.values():
            policy = deployment_config.get("scheduling_policy", "__DEFAULT__")
            if policy not in self.policies:
                raise WorkflowDefinitionException(f"Policy {policy} is not defined")
            deployment_config["scheduling_policy"] = self.policies[policy]
        for name, deployment in self.deployments.items():
            deployment["name"] = name
        self.filesystem = {"children": {}}
        for binding in workflow_config.get("bindings", []):
            self._process_binding(binding)
        set_targets(self.filesystem, None)
        self._check_stacked_deployments()

    def _check_stacked_deployments(self) -> None:
        for deployment in self.deployments.values():
            deployments = {deployment["name"]}
            while (wraps := deployment.get("wraps")) is not None:
                # Get parent deployment
                deployment = self.deployments[
                    wraps if isinstance(wraps, str) else wraps["deployment"]
                ]
                if deployment["name"] in deployments:
                    raise WorkflowDefinitionException(
                        f"The deployment `{deployment['name']}` leads to a circular reference: "
                        f"Recursive deployment definitions are not allowed."
                    )
                else:
                    deployments.add(deployment["name"])

    def _process_binding(self, binding: MutableMapping[str, Any]):
        targets = (
            binding["target"]
            if isinstance(binding["target"], MutableSequence)
            else [binding["target"]]
        )
        target_type = "step" if "step" in binding else "port"
        if target_type == "port" and "workdir" not in binding["target"]:
            raise WorkflowDefinitionException(
                "The `workdir` option is mandatory when specifying a `port` target."
            )
        config = {"targets": targets, "filters": []}
        for f in binding.get("filters", []):
            if f in self.binding_filters:
                config["filters"].append(self.binding_filters[f])
            else:
                raise WorkflowDefinitionException(f"Binding filter {f} is not defined")
        path = PurePosixPath(binding["step"] if "step" in binding else binding["port"])
        if not path.is_absolute():
            raise WorkflowDefinitionException(
                f"Binding {path.as_posix()} is not well-defined in the StreamFlow file. "
                f"It must be an absolute POSIX path"
            )
        self.put(path, target_type, config)

    def get(
        self, path: PurePosixPath, name: str, default: Any | None = None
    ) -> Any | None:
        current_node = self.filesystem
        for part in path.parts:
            if part not in current_node["children"]:
                return default
            current_node = current_node["children"][part]
        return current_node.get(name)

    def propagate(
        self, path: PurePosixPath, name: str, default: Any | None = None
    ) -> Any | None:
        current_node = self.filesystem
        value = default
        for part in path.parts:
            if part not in current_node["children"]:
                return value
            current_node = current_node["children"][part]
            if name in current_node:
                value = current_node[name]
        return value

    def put(self, path: PurePosixPath, name: str, value: Any) -> None:
        current_node = self.filesystem
        for part in path.parts:
            if part not in current_node["children"]:
                current_node["children"][part] = {"children": {}}
            current_node = current_node["children"][part]
        current_node[name] = value
