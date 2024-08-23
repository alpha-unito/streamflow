from __future__ import annotations

import logging
import os
import posixpath
from pathlib import PurePosixPath
from types import ModuleType
from typing import Any, MutableMapping, TYPE_CHECKING

from streamflow.core.config import BindingConfig
from streamflow.core.deployment import (
    DeploymentConfig,
    FilterConfig,
    LocalTarget,
    Target,
    WrapsConfig,
)
from streamflow.deployment.connector import LocalConnector
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from streamflow.config.config import WorkflowConfig
    from streamflow.core.deployment import Connector


def get_binding_config(
    name: str, target_type: str, workflow_config: WorkflowConfig
) -> BindingConfig:
    path = PurePosixPath(name)
    config = workflow_config.propagate(path, target_type)
    if config is not None:
        targets = []
        for target in config["targets"]:
            workdir = target.get("workdir") if target is not None else None
            if "deployment" in target:
                target_deployment = workflow_config.deployments[target["deployment"]]
            else:
                target_deployment = workflow_config.deployments[target["model"]]
                if logger.isEnabledFor(logging.WARNING):
                    logger.warning(
                        "The `model` keyword is deprecated and will be removed in StreamFlow 0.3.0. "
                        "Use `deployment` instead."
                    )
            locations = target.get("locations", None)
            if locations is None:
                locations = target.get("resources")
                if locations is not None:
                    if logger.isEnabledFor(logging.WARNING):
                        logger.warning(
                            "The `resources` keyword is deprecated and will be removed in StreamFlow 0.3.0. "
                            "Use `locations` instead."
                        )
                else:
                    locations = 1
            deployment = DeploymentConfig(
                name=target_deployment["name"],
                type=target_deployment["type"],
                config=target_deployment["config"],
                external=target_deployment.get("external", False),
                lazy=target_deployment.get("lazy", True),
                scheduling_policy=target_deployment["scheduling_policy"],
                workdir=target_deployment.get("workdir")
                or (
                    workflow_config.deployments[target_deployment["wraps"]].get(
                        "workdir"
                    )
                    if "wraps" in target_deployment
                    else None
                ),
                wraps=get_wraps_config(target_deployment.get("wraps")),
            )
            targets.append(
                Target(
                    deployment=deployment,
                    locations=locations,
                    service=target.get("service"),
                    workdir=workdir,
                )
            )
        return BindingConfig(
            targets=targets,
            filters=[
                FilterConfig(name=c.name, type=c.type, config=c.config)
                for c in config.get("filters")
            ],
        )
    else:
        return BindingConfig(targets=[LocalTarget()])


def get_path_processor(connector: Connector) -> ModuleType:
    return (
        posixpath
        if connector is not None and not isinstance(connector, LocalConnector)
        else os.path
    )


def get_wraps_config(config: MutableMapping[str, Any] | None) -> WrapsConfig | None:
    if config is not None:
        if isinstance(config, str):
            return WrapsConfig(deployment=config)
        else:
            return WrapsConfig(
                deployment=config["deployment"],
                service=config.get("service"),
            )
    else:
        return None
