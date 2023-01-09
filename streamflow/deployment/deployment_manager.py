from __future__ import annotations

import asyncio
import logging
import os
from typing import TYPE_CHECKING

import pkg_resources

from streamflow.core.deployment import Connector, DeploymentManager
from streamflow.deployment.connector import connector_classes
from streamflow.deployment.future import FutureConnector
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.deployment import DeploymentConfig
    from typing import MutableMapping, Optional, Any


class DefaultDeploymentManager(DeploymentManager):
    def __init__(self, context: StreamFlowContext) -> None:
        super().__init__(context)
        self.config_map: MutableMapping[str, Any] = {}
        self.events_map: MutableMapping[str, asyncio.Event] = {}
        self.deployments_map: MutableMapping[str, Connector] = {}

    async def close(self):
        await self.undeploy_all()

    async def deploy(self, deployment_config: DeploymentConfig):
        deployment_name = deployment_config.name
        while True:
            if deployment_name not in self.events_map:
                self.events_map[deployment_name] = asyncio.Event()
            if deployment_name not in self.config_map:
                self.config_map[deployment_name] = deployment_config
                if deployment_config.lazy:
                    connector = FutureConnector(
                        name=deployment_name,
                        config_dir=self.context.config_dir,
                        type=connector_classes[deployment_config.type],
                        external=deployment_config.external,
                        **deployment_config.config,
                    )
                    self.deployments_map[deployment_name] = connector
                    self.events_map[deployment_name].set()
                else:
                    connector = connector_classes[deployment_config.type](
                        deployment_name, self.context, **deployment_config.config
                    )
                    self.deployments_map[deployment_name] = connector
                    if logger.isEnabledFor(logging.INFO):
                        if not deployment_config.external:
                            logger.info("DEPLOYING {}".format(deployment_name))
                    await connector.deploy(deployment_config.external)
                    if logger.isEnabledFor(logging.INFO):
                        if not deployment_config.external:
                            logger.info(
                                "COMPLETED Deployment of {}".format(deployment_name)
                            )
                    self.events_map[deployment_name].set()
                    break
            else:
                await self.events_map[deployment_name].wait()
                if deployment_name in self.config_map:
                    break

    def get_connector(self, deployment_name: str) -> Optional[Connector]:
        return self.deployments_map.get(deployment_name, None)

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "deployment_manager.json")
        )

    def is_deployed(self, deployment_name: str):
        return deployment_name in self.deployments_map

    async def undeploy(self, deployment_name: str):
        if deployment_name in dict(self.deployments_map):
            await self.events_map[deployment_name].wait()
            self.events_map[deployment_name].clear()
            connector = self.deployments_map[deployment_name]
            config = self.config_map[deployment_name]
            if logger.isEnabledFor(logging.INFO):
                if not config.external:
                    logger.info(
                        "UNDEPLOYING {deployment}".format(deployment=deployment_name)
                    )
            await connector.undeploy(config.external)
            if logger.isEnabledFor(logging.INFO):
                if not config.external:
                    logger.info("COMPLETED Undeployment of {}".format(deployment_name))
            del self.deployments_map[deployment_name]
            del self.config_map[deployment_name]
            self.events_map[deployment_name].set()

    async def undeploy_all(self):
        undeployments = []
        for name in dict(self.deployments_map):
            undeployments.append(asyncio.create_task(self.undeploy(name)))
        await asyncio.gather(*undeployments)
