from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import MutableMapping
from importlib.resources import files
from typing import TYPE_CHECKING, Any, cast

from streamflow.core.deployment import (
    Connector,
    DeploymentConfig,
    DeploymentManager,
    LocalTarget,
)
from streamflow.core.exception import (
    WorkflowDefinitionException,
    WorkflowExecutionException,
)
from streamflow.deployment.connector import connector_classes
from streamflow.deployment.future import FutureConnector
from streamflow.deployment.utils import get_wraps_config
from streamflow.deployment.wrapper import ConnectorWrapper
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext


class DefaultDeploymentManager(DeploymentManager):
    def __init__(self, context: StreamFlowContext) -> None:
        super().__init__(context)
        self.config_map: MutableMapping[str, Any] = {}
        self.events_map: MutableMapping[str, asyncio.Event] = {}
        self.deployments_map: MutableMapping[str, Connector] = {}
        self.dependency_graph: MutableMapping[str, set[str]] = {}

    async def _deploy(self, deployment_config: DeploymentConfig) -> None:
        deployment_name = deployment_config.name
        while True:
            if deployment_name not in self.config_map:
                self.config_map[deployment_name] = deployment_config
                self.events_map[deployment_name] = asyncio.Event()
                self.dependency_graph[deployment_name] = set()
                connector_type = connector_classes[deployment_config.type]
                deployment_config = await self._inner_deploy(
                    connector_type=connector_type,
                    deployment_config=deployment_config,
                )
                if deployment_config.lazy:
                    connector = FutureConnector(
                        name=deployment_name,
                        config_dir=os.path.dirname(self.context.config["path"]),
                        connector_type=connector_type,
                        external=deployment_config.external,
                        **deployment_config.config,
                    )
                    self.deployments_map[deployment_name] = connector
                    self.events_map[deployment_name].set()
                else:
                    connector = connector_type(
                        deployment_name,
                        self.context.config["path"],
                        **deployment_config.config,
                    )
                    self.deployments_map[deployment_name] = connector
                    if logger.isEnabledFor(logging.INFO):
                        if not deployment_config.external:
                            logger.info(f"DEPLOYING {deployment_name}")
                    try:
                        await connector.deploy(deployment_config.external)
                    except Exception:
                        self.deployments_map.pop(deployment_name)
                        self.events_map[deployment_name].set()
                        raise
                    if logger.isEnabledFor(logging.INFO):
                        if not deployment_config.external:
                            logger.info(f"COMPLETED deployment of {deployment_name}")
                    self.events_map[deployment_name].set()
                    break
            else:
                await self.events_map[deployment_name].wait()
                if deployment_name not in self.deployments_map:
                    raise WorkflowExecutionException(
                        f"FAILED deployment of {deployment_name}"
                    )
                if deployment_name in self.config_map:
                    break

    async def _inner_deploy(
        self, connector_type: type[Connector], deployment_config: DeploymentConfig
    ) -> DeploymentConfig:
        # If it is a ConnectorWrapper
        if issubclass(connector_type, ConnectorWrapper):
            # Retrieve the inner connector's config
            if deployment_config.wraps is None:
                deployment_name = LocalTarget.deployment_name
                service = None
                if deployment_name not in self.config_map:
                    await self._deploy(LocalTarget().deployment)
            else:
                deployment_name = deployment_config.wraps.deployment
                service = deployment_config.wraps.service
            # If it has already been processed by the DeploymentManager
            if deployment_name in self.config_map:
                # If the DeploymentManager is creating the environment, wait for it to finish
                if deployment_name not in self.deployments_map:
                    await self.events_map[deployment_name].wait()
                await self._inner_deploy(
                    connector_type=type(self.deployments_map[deployment_name]),
                    deployment_config=self.config_map[deployment_name],
                )
            # Otherwise, if it exists, and it points to a new connector, deploy it
            elif deployment_name in self.context.config["deployments"]:
                inner_config = self.context.config["deployments"][deployment_name]
                inner_config = DeploymentConfig(
                    name=deployment_name,
                    type=inner_config["type"],
                    config=inner_config["config"],
                    external=inner_config.get("external", False),
                    lazy=inner_config.get("lazy", True),
                    scheduling_policy=inner_config["scheduling_policy"],
                    workdir=inner_config.get("workdir"),
                    wraps=get_wraps_config(inner_config.get("wraps")),
                )
                await self._deploy(inner_config)
            # Otherwise, the workflow is badly specified
            else:
                raise WorkflowDefinitionException(
                    f"No valid deployment configuration for {deployment_name}."
                )
            # Update dependency graph
            self.dependency_graph[deployment_name].add(deployment_config.name)
            # Then, inject the connector into the DeploymentConfig
            return DeploymentConfig(
                name=deployment_config.name,
                type=deployment_config.type,
                config=cast(dict[str, Any], deployment_config.config)
                | {
                    "connector": self.deployments_map[deployment_name],
                    "service": service,
                },
                external=deployment_config.external,
                lazy=deployment_config.lazy,
                scheduling_policy=deployment_config.scheduling_policy,
                workdir=deployment_config.workdir,
                wraps=deployment_config.wraps,
            )
        # If it is not a ConnectorWrapper, do nothing
        else:
            if deployment_config.wraps is not None:
                if logger.isEnabledFor(logging.WARNING):
                    logger.warning(
                        f"The `wraps` directive has no effect on deployment {deployment_config.name}, "
                        f"as the `{deployment_config.type}` connector does not inherit from the ConnectorWrapper class."
                    )
            return deployment_config

    async def close(self) -> None:
        await self.undeploy_all()

    async def deploy(self, deployment_config: DeploymentConfig) -> None:
        deployment_name = deployment_config.name
        await self._deploy(deployment_config)
        self.dependency_graph[deployment_name].add(deployment_name)

    def get_connector(self, deployment_name: str) -> Connector | None:
        return self.deployments_map.get(deployment_name, None)

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("deployment_manager.json")
            .read_text("utf-8")
        )

    async def undeploy(self, deployment_name: str) -> None:
        if deployment_name in dict(self.deployments_map):
            await self.events_map[deployment_name].wait()
            # Remove the deployment from the dependency graph
            self.dependency_graph[deployment_name].discard(deployment_name)
            # If there are no more inner deployments, undeploy the environment and clear the related data structures
            if len(self.dependency_graph[deployment_name]) == 0:
                self.events_map[deployment_name].clear()
                connector = self.deployments_map[deployment_name]
                config = self.config_map[deployment_name]
                if logger.isEnabledFor(logging.INFO):
                    if not config.external:
                        logger.info(f"UNDEPLOYING {deployment_name}")
                del self.deployments_map[deployment_name]
                del self.config_map[deployment_name]
                del self.dependency_graph[deployment_name]
                await connector.undeploy(config.external)
                if logger.isEnabledFor(logging.INFO):
                    if not config.external:
                        logger.info(f"COMPLETED undeployment of {deployment_name}")
                self.events_map[deployment_name].set()
            # Remove the current environment from all the other dependency graphs
            for name, deps in list(
                (k, v) for k, v in self.dependency_graph.items() if k != deployment_name
            ):
                deps.discard(deployment_name)
                # If there are no more dependencies, undeploy the environment
                if len(deps) == 0:
                    await self.undeploy(name)

    async def undeploy_all(self) -> None:
        undeployments = []
        for name in dict(self.deployments_map):
            undeployments.append(asyncio.create_task(self.undeploy(name)))
        await asyncio.gather(*undeployments)
