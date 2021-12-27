from __future__ import annotations

import asyncio
from asyncio import Event
from typing import TYPE_CHECKING

from streamflow.core.deployment import DeploymentManager
from streamflow.deployment.connector.container import DockerConnector, DockerComposeConnector, SingularityConnector
from streamflow.deployment.connector.kubernetes import Helm3Connector
from streamflow.deployment.connector.local import LocalConnector
from streamflow.deployment.connector.occam import OccamConnector
from streamflow.deployment.connector.queue_manager import PBSConnector, SlurmConnector
from streamflow.deployment.connector.ssh import SSHConnector
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from streamflow.core.deployment import DeploymentConfig, Connector
    from typing import MutableMapping, Optional, Any

connector_classes = {
    'docker': DockerConnector,
    'docker-compose': DockerComposeConnector,
    'helm': Helm3Connector,
    'helm3': Helm3Connector,
    'local': LocalConnector,
    'occam': OccamConnector,
    'pbs': PBSConnector,
    'singularity': SingularityConnector,
    'slurm': SlurmConnector,
    'ssh': SSHConnector
}


class DefaultDeploymentManager(DeploymentManager):

    def __init__(self, streamflow_config_dir: str) -> None:
        super().__init__(streamflow_config_dir)
        self.config_map: MutableMapping[str, Any] = {}
        self.events_map: MutableMapping[str, Event] = {}
        self.deployments_map: MutableMapping[str, Connector] = {}

    async def deploy(self, deployment_config: DeploymentConfig):
        deployment_name = deployment_config.name
        while True:
            if deployment_name not in self.events_map:
                self.events_map[deployment_name] = Event()
            if deployment_name not in self.config_map:
                self.config_map[deployment_name] = deployment_config
                connector = connector_classes[deployment_config.connector_type](self.streamflow_config_dir,
                                                                                **deployment_config.config)
                self.deployments_map[deployment_name] = connector
                if not deployment_config.external:
                    logger.info("Deploying {deployment}".format(deployment=deployment_name))
                await connector.deploy(deployment_config.external)
                self.events_map[deployment_name].set()
                break
            else:
                await self.events_map[deployment_name].wait()
                if deployment_name in self.config_map:
                    break

    def get_connector(self, deployment_name: str) -> Optional[Connector]:
        return self.deployments_map.get(deployment_name, None)

    def is_deployed(self, deployment_name: str):
        return deployment_name in self.deployments_map

    async def undeploy(self, deployment_name: str):
        if deployment_name in dict(self.deployments_map):
            await self.events_map[deployment_name].wait()
            self.events_map[deployment_name].clear()
            connector = self.deployments_map[deployment_name]
            config = self.config_map[deployment_name]
            if not config.external:
                logger.info("Undeploying {deployment}".format(deployment=deployment_name))
            await connector.undeploy(config.external)
            del self.deployments_map[deployment_name]
            del self.config_map[deployment_name]
            self.events_map[deployment_name].set()

    async def undeploy_all(self):
        undeployments = []
        for name in dict(self.deployments_map):
            undeployments.append(asyncio.create_task(self.undeploy(name)))
        await asyncio.gather(*undeployments)
