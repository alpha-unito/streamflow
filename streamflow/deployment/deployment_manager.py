from __future__ import annotations

import asyncio
from asyncio import Event
from typing import TYPE_CHECKING

from streamflow.core.deployment import DeploymentManager
from streamflow.deployment.connector.container import DockerConnector, DockerComposeConnector, SingularityConnector
from streamflow.deployment.connector.helm import Helm2Connector, Helm3Connector
from streamflow.deployment.connector.occam import OccamConnector
from streamflow.deployment.connector.queue_manager import PBSConnector, SlurmConnector
from streamflow.deployment.connector.ssh import SSHConnector
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from streamflow.core.deployment import ModelConfig, Connector
    from typing import MutableMapping, Optional, Any
    from typing_extensions import Text

connector_classes = {
    'docker': DockerConnector,
    'docker-compose': DockerComposeConnector,
    'helm': Helm3Connector,
    'helm2': Helm2Connector,
    'helm3': Helm3Connector,
    'occam': OccamConnector,
    'pbs': PBSConnector,
    'singularity': SingularityConnector,
    'slurm': SlurmConnector,
    'ssh': SSHConnector
}


class DefaultDeploymentManager(DeploymentManager):

    def __init__(self, streamflow_config_dir: Text) -> None:
        super().__init__(streamflow_config_dir)
        self.config_map: MutableMapping[Text, Any] = {}
        self.events_map: MutableMapping[Text, Event] = {}
        self.deployments_map: MutableMapping[Text, Connector] = {}

    async def deploy(self, model_config: ModelConfig):
        model_name = model_config.name
        while True:
            if model_name not in self.events_map:
                self.events_map[model_name] = Event()
            if model_name not in self.config_map:
                self.config_map[model_name] = model_config
                connector = connector_classes[model_config.connector_type](self.streamflow_config_dir,
                                                                           **model_config.config)
                self.deployments_map[model_name] = connector
                if not model_config.external:
                    logger.info("Deploying model {model}".format(model=model_name))
                await connector.deploy(model_config.external)
                self.events_map[model_name].set()
                break
            else:
                await self.events_map[model_name].wait()
                if model_name in self.config_map:
                    break

    def get_connector(self, model_name: Text) -> Optional[Connector]:
        return self.deployments_map.get(model_name, None)

    def is_deployed(self, model_name: Text):
        return model_name in self.deployments_map

    async def undeploy(self, model_name: Text):
        if model_name in dict(self.deployments_map):
            await self.events_map[model_name].wait()
            self.events_map[model_name].clear()
            connector = self.deployments_map[model_name]
            config = self.config_map[model_name]
            if not config.external:
                logger.info("Undeploying model {model}".format(model=model_name))
            await connector.undeploy(config.external)
            del self.deployments_map[model_name]
            del self.config_map[model_name]
            self.events_map[model_name].set()

    async def undeploy_all(self):
        undeployments = []
        for name in dict(self.deployments_map):
            undeployments.append(asyncio.create_task(self.undeploy(name)))
        await asyncio.gather(*undeployments)
