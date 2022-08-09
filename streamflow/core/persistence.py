from __future__ import annotations

from abc import ABC, abstractmethod
from asyncio import Lock
from enum import Enum
from typing import Any, MutableMapping, MutableSequence, Optional, TYPE_CHECKING, Type

from streamflow.core.config import SchemaEntity
from streamflow.core.context import StreamFlowContext

if TYPE_CHECKING:
    from streamflow.core.deployment import DeploymentConfig, Target
    from streamflow.core.workflow import Port, Step, Token, Workflow


class DatabaseLoadingContext(ABC):

    @abstractmethod
    def add_deployment(self,
                       persistent_id: int,
                       deployment: DeploymentConfig):
        ...

    @abstractmethod
    def add_port(self,
                 persistent_id: int,
                 port: Port):
        ...

    @abstractmethod
    def add_step(self,
                 persistent_id: int,
                 step: Step):
        ...

    @abstractmethod
    def add_target(self,
                   persistent_id: int,
                   target: Target):
        ...

    @abstractmethod
    def add_token(self,
                  persistent_id: int,
                  token: Token):
        ...

    @abstractmethod
    def add_workflow(self,
                     persistent_id: int,
                     workflow: Workflow):
        ...

    @abstractmethod
    async def load_deployment(self,
                              context: StreamFlowContext,
                              persistent_id: int):
        ...

    @abstractmethod
    async def load_port(self,
                        context: StreamFlowContext,
                        persistent_id: int):
        ...

    @abstractmethod
    async def load_step(self,
                        context: StreamFlowContext,
                        persistent_id: int):
        ...

    @abstractmethod
    async def load_target(self,
                          context: StreamFlowContext,
                          persistent_id: int):
        ...

    @abstractmethod
    async def load_token(self,
                         context: StreamFlowContext,
                         persistent_id: int):
        ...

    @abstractmethod
    async def load_workflow(self,
                            context: StreamFlowContext,
                            persistent_id: int):
        ...


class PersistableEntity(object):

    def __init__(self):
        self.persistent_id: Optional[int] = None
        self.persistence_lock: Lock = Lock()

    @classmethod
    @abstractmethod
    async def load(cls,
                   context: StreamFlowContext,
                   persistent_id: int,
                   loading_context: DatabaseLoadingContext) -> PersistableEntity:
        ...

    @abstractmethod
    async def save(self, context: StreamFlowContext) -> None:
        ...


class DependencyType(Enum):
    INPUT = 0
    OUTPUT = 1


class Database(SchemaEntity):

    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context

    @abstractmethod
    async def add_command(self,
                          step_id: int,
                          cmd: str) -> int:
        ...

    @abstractmethod
    async def add_dependency(self,
                             step: int,
                             port: int,
                             type: DependencyType, name: str) -> None:
        ...

    @abstractmethod
    async def add_deployment(self,
                             name: str,
                             type: str,
                             config: str,
                             external: bool,
                             lazy: bool,
                             workdir: Optional[str]) -> int:
        ...

    @abstractmethod
    async def add_port(self,
                       name: str,
                       workflow_id: int,
                       type: Type[Port],
                       params: str) -> int:
        ...

    @abstractmethod
    async def add_provenance(self,
                             inputs: MutableSequence[int],
                             token: int):
        ...

    @abstractmethod
    async def add_step(self,
                       name: str,
                       workflow_id: int,
                       status: int,
                       type: Type[Step],
                       params: str) -> int:
        ...

    @abstractmethod
    async def add_target(self,
                         deployment: int,
                         type: Type[Target],
                         params: str,
                         locations: int = 1,
                         service: Optional[str] = None,
                         workdir: Optional[str] = None) -> int:
        ...

    @abstractmethod
    async def add_token(self,
                        tag: str,
                        type: Type[Token],
                        value: Any,
                        port: Optional[int] = None) -> int:
        ...

    @abstractmethod
    async def add_workflow(self,
                           name: str,
                           params: str,
                           status: int,
                           type: str) -> int:
        ...

    @abstractmethod
    async def close(self):
        ...

    @abstractmethod
    async def get_deployment(self, deplyoment_id: int) -> MutableMapping[str, Any]:
        ...

    @abstractmethod
    async def get_input_ports(self, step_id: int) -> MutableSequence[MutableMapping[str, Any]]:
        ...

    @abstractmethod
    async def get_output_ports(self, step_id: int) -> MutableSequence[MutableMapping[str, Any]]:
        ...

    @abstractmethod
    async def get_port(self, port_id: int) -> MutableMapping[str, Any]:
        ...

    @abstractmethod
    async def get_port_tokens(self, port_id: int) -> MutableSequence[int]:
        ...

    @abstractmethod
    async def get_report(self, workflow: str) -> MutableSequence[MutableMapping[str, Any]]:
        ...

    @abstractmethod
    async def get_step(self, step_id: int) -> MutableMapping[str, Any]:
        ...

    @abstractmethod
    async def get_target(self, target_id: int) -> MutableMapping[str, Any]:
        ...

    @abstractmethod
    async def get_token(self, token_id: int) -> MutableMapping[str, Any]:
        ...

    @abstractmethod
    async def get_workflow(self, workflow_id: int) -> MutableMapping[str, Any]:
        ...

    @abstractmethod
    async def get_workflow_ports(self, workflow_id: int) -> MutableSequence[MutableMapping[str, Any]]:
        ...

    @abstractmethod
    async def get_workflow_steps(self, workflow_id: int) -> MutableSequence[MutableMapping[str, Any]]:
        ...

    @abstractmethod
    async def list_workflows(self) -> MutableSequence[MutableMapping[str, Any]]:
        ...

    @abstractmethod
    async def update_command(self,
                             command_id: int,
                             updates: MutableMapping[str, Any]) -> int:
        ...

    @abstractmethod
    async def update_deployment(self,
                                deployment_id: int,
                                updates: MutableMapping[str, Any]) -> int:
        ...

    @abstractmethod
    async def update_port(self,
                          port_id: int,
                          updates: MutableMapping[str, Any]) -> int:
        ...

    @abstractmethod
    async def update_step(self,
                          step_id: int,
                          updates: MutableMapping[str, Any]) -> int:
        ...

    @abstractmethod
    async def update_target(self,
                            target_id: str,
                            updates: MutableMapping[str, Any]) -> int:
        ...

    @abstractmethod
    async def update_workflow(self,
                              workflow_id: int,
                              updates: MutableMapping[str, Any]) -> int:
        ...
