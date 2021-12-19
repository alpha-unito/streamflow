from __future__ import annotations

import asyncio
from abc import abstractmethod, ABC
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from streamflow.core.scheduling import Resource
    from typing import MutableSequence, MutableMapping, Optional, Any, Tuple, Union


class Connector(ABC):

    def __init__(self, streamflow_config_dir: str):
        self.streamflow_config_dir: str = streamflow_config_dir

    @abstractmethod
    async def copy(self,
                   src: str,
                   dst: str,
                   resources: MutableSequence[str],
                   kind: ConnectorCopyKind,
                   source_remote: Optional[str] = None,
                   read_only: bool = False) -> None:
        ...

    @abstractmethod
    async def deploy(self, external: bool) -> None:
        ...

    @abstractmethod
    async def get_available_resources(self, service: str) -> MutableMapping[str, Resource]:
        ...

    @abstractmethod
    async def run(self,
                  resource: str,
                  command: MutableSequence[str],
                  environment: MutableMapping[str, str] = None,
                  workdir: Optional[str] = None,
                  stdin: Optional[Union[int, str]] = None,
                  stdout: Union[int, str] = asyncio.subprocess.STDOUT,
                  stderr: Union[int, str] = asyncio.subprocess.STDOUT,
                  capture_output: bool = False,
                  job_name: Optional[str] = None) -> Optional[Tuple[Optional[Any], int]]:
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
                 streamflow_config_dir: str) -> None:
        self.streamflow_config_dir: str = streamflow_config_dir

    @abstractmethod
    async def deploy(self, model_config: ModelConfig):
        ...

    @abstractmethod
    def get_connector(self, model_name: str) -> Optional[Connector]:
        ...

    @abstractmethod
    def is_deployed(self, model_name: str):
        ...

    @abstractmethod
    async def undeploy(self, model_name: str):
        ...

    @abstractmethod
    async def undeploy_all(self):
        ...


class ModelConfig(object):

    def __init__(self,
                 name: str,
                 connector_type: str,
                 config: MutableMapping[str, Any],
                 external: bool) -> None:
        self.name: str = name
        self.connector_type: str = connector_type
        self.config: MutableMapping[str, Any] = config
        self.external = external
