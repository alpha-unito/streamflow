from __future__ import annotations

import asyncio
import json
import os
import posixpath
import tempfile
from abc import abstractmethod
from enum import Enum
from typing import TYPE_CHECKING

from streamflow.core.config import Config, SchemaEntity
from streamflow.core.data import LOCAL_LOCATION
from streamflow.core.persistence import DatabaseLoadingContext, PersistableEntity

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.scheduling import Location
    from typing import MutableSequence, MutableMapping, Optional, Any, Tuple, Union


def _init_workdir(deployment_name: str) -> str:
    if deployment_name != LOCAL_LOCATION:
        return posixpath.join('/tmp', 'streamflow')
    else:
        return os.path.join(tempfile.gettempdir(), 'streamflow')


class Connector(SchemaEntity):

    def __init__(self,
                 deployment_name: str,
                 context: StreamFlowContext):
        self.deployment_name: str = deployment_name
        self.context: StreamFlowContext = context

    @abstractmethod
    async def copy(self,
                   src: str,
                   dst: str,
                   locations: MutableSequence[str],
                   kind: ConnectorCopyKind,
                   source_connector: Optional[Connector] = None,
                   source_location: Optional[str] = None,
                   read_only: bool = False) -> None:
        ...

    @abstractmethod
    async def deploy(self, external: bool) -> None:
        ...

    @abstractmethod
    async def get_available_locations(self,
                                      service: str,
                                      input_directory: Optional[str] = None,
                                      output_directory: Optional[str] = None,
                                      tmp_directory: Optional[str] = None) -> MutableMapping[str, Location]:
        ...

    @abstractmethod
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
        ...

    @abstractmethod
    async def undeploy(self, external: bool) -> None:
        ...


class ConnectorCopyKind(Enum):
    LOCAL_TO_REMOTE = 1
    REMOTE_TO_LOCAL = 2
    REMOTE_TO_REMOTE = 3


class DeploymentManager(SchemaEntity):

    def __init__(self,
                 context: StreamFlowContext) -> None:
        self.context: StreamFlowContext = context

    @abstractmethod
    async def close(self):
        ...

    @abstractmethod
    async def deploy(self, deployment_config: DeploymentConfig):
        ...

    @abstractmethod
    def get_connector(self, deployment_name: str) -> Optional[Connector]:
        ...

    @abstractmethod
    def is_deployed(self, deployment_name: str):
        ...

    @abstractmethod
    async def undeploy(self, deployment_name: str):
        ...

    @abstractmethod
    async def undeploy_all(self):
        ...


class DeploymentConfig(Config, PersistableEntity):
    __slots__ = ('name', 'type', 'config', 'external', 'lazy', 'workdir')

    def __init__(self,
                 name: str,
                 type: str,
                 config: MutableMapping[str, Any],
                 external: bool = False,
                 lazy: bool = True,
                 workdir: Optional[str] = None) -> None:
        super().__init__(name, type, config)
        self.external = external
        self.lazy: bool = lazy
        self.workdir: Optional[str] = workdir

    @classmethod
    async def load(cls,
                   context: StreamFlowContext,
                   persistent_id: int,
                   loading_context: DatabaseLoadingContext) -> DeploymentConfig:
        row = await context.database.get_deployment(persistent_id)
        obj = cls(
            name=row['name'],
            type=row['type'],
            config=row['config'],
            external=row['external'],
            lazy=row['lazy'],
            workdir=row['workdir'])
        obj.persistent_id = persistent_id
        loading_context.add_deployment(persistent_id, obj)
        return obj

    async def save(self, context: StreamFlowContext) -> None:
        async with self.persistence_lock:
            if not self.persistent_id:
                self.persistent_id = await context.database.add_deployment(
                    name=self.name,
                    type=self.type,
                    config=json.dumps(self.config),
                    external=self.external,
                    lazy=self.lazy,
                    workdir=self.workdir)


class Target(PersistableEntity):

    def __init__(self,
                 deployment: DeploymentConfig,
                 locations: int = 1,
                 service: Optional[str] = None,
                 scheduling_group: Optional[str] = None,
                 scheduling_policy: Optional[Config] = None,
                 workdir: Optional[str] = None):
        super().__init__()
        self.deployment: DeploymentConfig = deployment
        self.locations: int = locations
        self.service: Optional[str] = service
        self.scheduling_group: Optional[str] = scheduling_group
        self.scheduling_policy: Optional[Config] = (
                scheduling_policy or Config(name='__DEFAULT__', type='data_locality', config={}))
        self.workdir: str = workdir or self.deployment.workdir or _init_workdir(deployment.name)

    async def _save_additional_params(self, context: StreamFlowContext) -> MutableMapping[str, Any]:
        return {}

    @classmethod
    async def load(cls,
                   context: StreamFlowContext,
                   persistent_id: int,
                   loading_context: DatabaseLoadingContext) -> Target:
        row = await context.database.get_target(persistent_id)
        obj = cls(
            deployment=await DeploymentConfig.load(context, row['deployment'], loading_context),
            locations=row['locations'],
            service=row['service'],
            workdir=row['workdir'])
        obj.persistent_id = persistent_id
        loading_context.add_target(persistent_id, obj)
        return obj

    async def save(self, context: StreamFlowContext) -> None:
        await self.deployment.save(context)
        async with self.persistence_lock:
            if not self.persistent_id:
                self.persistent_id = await context.database.add_target(
                    deployment=self.deployment.persistent_id,
                    type=type(self),
                    params=json.dumps(await self._save_additional_params(context)),
                    locations=self.locations,
                    service=self.service,
                    workdir=self.workdir)


class LocalTarget(Target):
    __deployment_config = None

    def __init__(self, workdir: Optional[str] = None):
        super().__init__(
            deployment=self._get_deployment_config(),
            locations=1,
            workdir=workdir)

    @classmethod
    def _get_deployment_config(cls):
        if not cls.__deployment_config:
            cls.__deployment_config = DeploymentConfig(
                name=LOCAL_LOCATION,
                type='local',
                config={},
                external=True,
                lazy=False)
        return cls.__deployment_config
