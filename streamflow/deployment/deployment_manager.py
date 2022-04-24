from __future__ import annotations

import asyncio
from asyncio import Event
from typing import TYPE_CHECKING, MutableSequence, Union, Tuple, Type

from streamflow.core.deployment import Connector, DeploymentManager, ConnectorCopyKind
from streamflow.core.scheduling import Location
from streamflow.deployment.connector.container import DockerConnector, DockerComposeConnector, SingularityConnector
from streamflow.deployment.connector.kubernetes import Helm3Connector
from streamflow.deployment.connector.local import LocalConnector
from streamflow.deployment.connector.occam import OccamConnector
from streamflow.deployment.connector.queue_manager import PBSConnector, SlurmConnector
from streamflow.deployment.connector.ssh import SSHConnector
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from streamflow.core.deployment import DeploymentConfig
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

    def __init__(self,
                 streamflow_config_dir: str) -> None:
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
                if deployment_config.lazy:
                    connector = FutureConnector(
                        name=deployment_name,
                        streamflow_config_dir=self.streamflow_config_dir,
                        connector_type=connector_classes[deployment_config.connector_type],
                        external=deployment_config.external,
                        **deployment_config.config)
                    self.deployments_map[deployment_name] = connector
                    self.events_map[deployment_name].set()
                else:
                    connector = connector_classes[deployment_config.connector_type](
                        deployment_name, self.streamflow_config_dir, **deployment_config.config)
                    self.deployments_map[deployment_name] = connector
                    if not deployment_config.external:
                        logger.info("Deploying {}".format(deployment_name))
                    await connector.deploy(deployment_config.external)
                    if not deployment_config.external:
                        logger.info("Deployment of {} terminated with status COMPLETED".format(deployment_name))
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
            if not config.external:
                logger.info("Undeployment of {} terminated with status COMPLETED".format(deployment_name))
            del self.deployments_map[deployment_name]
            del self.config_map[deployment_name]
            self.events_map[deployment_name].set()

    async def undeploy_all(self):
        undeployments = []
        for name in dict(self.deployments_map):
            undeployments.append(asyncio.create_task(self.undeploy(name)))
        await asyncio.gather(*undeployments)


class FutureConnector(Connector):

    def __init__(self,
                 name: str,
                 streamflow_config_dir: str,
                 connector_type: Type[Connector],
                 external: bool,
                 **kwargs):
        super().__init__(name, streamflow_config_dir)
        self.connector_type: Type[Connector] = connector_type
        self.external: bool = external
        self.parameters: MutableMapping[str, Any] = kwargs
        self.deploying: bool = False
        self.deploy_event: Event = Event()
        self.connector: Optional[Connector] = None

    async def copy(self,
                   src: str,
                   dst: str,
                   locations: MutableSequence[str],
                   kind: ConnectorCopyKind,
                   source_location: Optional[str] = None,
                   read_only: bool = False) -> None:
        if self.connector is None:
            if not self.deploying:
                self.deploying = True
                await self.deploy(self.external)
            else:
                await self.deploy_event.wait()
        await self.connector.copy(src, dst, locations, kind, source_location, read_only)

    async def deploy(self,
                     external: bool) -> None:
        # noinspection PyArgumentList
        connector = self.connector_type(
            self.deployment_name, self.streamflow_config_dir, **self.parameters)
        if not external:
            logger.info("Deploying {}".format(self.deployment_name))
        await connector.deploy(external)
        if not external:
            logger.info("Deployment of {} terminated with status COMPLETED".format(self.deployment_name))
        self.connector = connector
        self.deploy_event.set()

    async def get_available_locations(self,
                                      service: str,
                                      input_directory: str,
                                      output_directory: str,
                                      tmp_directory: str) -> MutableMapping[str, Location]:
        if self.connector is None:
            if not self.deploying:
                self.deploying = True
                await self.deploy(self.external)
            else:
                await self.deploy_event.wait()
        return await self.connector.get_available_locations(
            service, input_directory, output_directory, tmp_directory)

    async def run(self,
                  location: str,
                  command: MutableSequence[str],
                  environment: MutableMapping[str, str] = None,
                  workdir: Optional[str] = None,
                  stdin: Optional[Union[int, str]] = None,
                  stdout: Union[int, str] = asyncio.subprocess.STDOUT,
                  stderr: Union[int, str] = asyncio.subprocess.STDOUT,
                  capture_output: bool = False,
                  job_name: Optional[str] = None) -> Optional[Tuple[Optional[Any], int]]:
        if self.connector is None:
            if not self.deploying:
                self.deploying = True
                await self.deploy(self.external)
            else:
                await self.deploy_event.wait()
        return await self.connector.run(
            location, command, environment, workdir, stdin, stdout, stderr, capture_output, job_name)

    async def undeploy(self, external: bool) -> None:
        if self.connector is not None:
            await self.connector.undeploy(external)
