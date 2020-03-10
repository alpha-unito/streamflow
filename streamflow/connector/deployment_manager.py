from threading import RLock
from typing import MutableMapping, Any

from streamflow.connector.connector import Connector
from streamflow.connector.docker_compose import DockerComposeConnector
from streamflow.connector.helm import Helm2Connector, Helm3Connector
from streamflow.connector.occam import OccamConnector

connector_classes = {
    'docker-compose': DockerComposeConnector,
    'helm': Helm3Connector,
    'helm2': Helm2Connector,
    'helm3': Helm3Connector,
    'occam': OccamConnector
}


class ConnectorConfig(object):

    def __init__(self,
                 model_name: str,
                 model_type: str,
                 model_config: MutableMapping[str, Any],
                 external: bool) -> None:
        self.model_name: str = model_name
        self.model_type: str = model_type
        self.model_config: MutableMapping[str, Any] = model_config
        self.external = external


class DeploymentManager(object):

    def __init__(self) -> None:
        super().__init__()
        self.lock = RLock()
        self.config_map = {}
        self.deployments_map = {}

    def deploy(self,
               model_name: str,
               model_type: str,
               model_config: MutableMapping[str, Any],
               external: bool
               ) -> Connector:
        with self.lock:
            if model_name not in self.config_map:
                self.config_map[model_name] = ConnectorConfig(model_name, model_type, model_config, external)
            connector = self.get_connector(model_name)
            if model_name not in self.deployments_map and not external:
                connector.deploy()
            self.deployments_map[model_name] = connector
            return connector

    def get_connector(self,
                      model_name: str):
        with self.lock:
            config = self.config_map[model_name]
            return connector_classes[config.model_type](**config.model_config)

    def is_deployed(self,
                    model_name: str):
        with self.lock:
            return model_name in self.deployments_map

    def undeploy(self, model_name):
        with self.lock:
            if model_name in dict(self.deployments_map):
                connector = self.deployments_map.pop(model_name)
                config = self.config_map.pop(model_name)
                if not config.external:
                    connector.undeploy()

    def undeploy_all(self):
        with self.lock:
            for name in dict(self.deployments_map):
                self.undeploy(name)
