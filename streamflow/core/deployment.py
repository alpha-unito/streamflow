from __future__ import annotations

import asyncio
from abc import abstractmethod, ABC
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from streamflow.core.scheduling import Resource
    from typing import MutableSequence, MutableMapping, Optional, Any, Tuple, Union
    from typing_extensions import Text


class Connector(ABC):

    def __init__(self, streamflow_config_dir: Text):
        self.streamflow_config_dir: Text = streamflow_config_dir

    @abstractmethod
    async def copy(self,
                   src: Text,
                   dst: Text,
                   resources: MutableSequence[Text],
                   kind: ConnectorCopyKind,
                   source_remote: Optional[Text] = None,
                   read_only: bool = False) -> None:
        ...

    @abstractmethod
    async def deploy(self, external: bool) -> None:
        ...

    @abstractmethod
    async def get_available_resources(self, service: Text) -> MutableMapping[Text, Resource]:
        ...

    @abstractmethod
    async def run(self,
                  resource: Text,
                  command: MutableSequence[Text],
                  environment: MutableMapping[Text, Text] = None,
                  workdir: Optional[Text] = None,
                  stdin: Optional[Union[int, Text]] = None,
                  stdout: Union[int, Text] = asyncio.subprocess.STDOUT,
                  stderr: Union[int, Text] = asyncio.subprocess.STDOUT,
                  capture_output: bool = False,
                  job_name: Optional[Text] = None) -> Optional[Tuple[Optional[Any], int]]:
        ...

    @abstractmethod
    async def undeploy(self, external: bool) -> None:
        ...


class ConnectorCopyKind(Enum):
    LOCAL_TO_REMOTE = 1
    REMOTE_TO_LOCAL = 2
    REMOTE_TO_REMOTE = 3


class DeploymentManager(ABC):

    def __init__(self,
                 streamflow_config_dir: Text) -> None:
        self.streamflow_config_dir: Text = streamflow_config_dir

    @abstractmethod
    async def deploy(self, model_config: ModelConfig):
        ...

    @abstractmethod
    def get_connector(self, model_name: Text) -> Optional[Connector]:
        ...

    @abstractmethod
    def is_deployed(self, model_name: Text):
        ...

    @abstractmethod
    async def undeploy(self, model_name: Text):
        ...

    @abstractmethod
    async def undeploy_all(self):
        ...


class ModelConfig(object):

    def __init__(self,
                 name: Text,
                 connector_type: Text,
                 config: MutableMapping[Text, Any],
                 external: bool) -> None:
        self.name: Text = name
        self.connector_type: Text = connector_type
        self.config: MutableMapping[Text, Any] = config
        self.external = external
