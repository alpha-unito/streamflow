from __future__ import annotations

from abc import ABC, abstractmethod
from asyncio import Lock
from collections.abc import MutableMapping, MutableSequence
from enum import Enum
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from streamflow.core.context import SchemaEntity, StreamFlowContext

if TYPE_CHECKING:
    from streamflow.core.deployment import DeploymentConfig, FilterConfig, Target
    from streamflow.core.workflow import Port, Step, Token, Workflow


class DatabaseLoadingContext(ABC):
    @abstractmethod
    def add_deployment(
        self, persistent_id: int, deployment: DeploymentConfig
    ) -> None: ...

    @abstractmethod
    def add_filter(self, persistent_id: int, filter_config: FilterConfig) -> None: ...

    @abstractmethod
    def add_port(self, persistent_id: int, port: Port) -> None: ...

    @abstractmethod
    def add_step(self, persistent_id: int, step: Step) -> None: ...

    @abstractmethod
    def add_target(self, persistent_id: int, target: Target) -> None: ...

    @abstractmethod
    def add_token(self, persistent_id: int, token: Token) -> None: ...

    @abstractmethod
    def add_workflow(self, persistent_id: int, workflow: Workflow) -> None: ...

    @abstractmethod
    async def load_deployment(
        self, context: StreamFlowContext, persistent_id: int
    ) -> DeploymentConfig: ...

    @abstractmethod
    async def load_filter(
        self, context: StreamFlowContext, persistent_id: int
    ) -> FilterConfig: ...

    @abstractmethod
    async def load_port(
        self, context: StreamFlowContext, persistent_id: int
    ) -> Port: ...

    @abstractmethod
    async def load_step(
        self, context: StreamFlowContext, persistent_id: int
    ) -> Step: ...

    @abstractmethod
    async def load_target(
        self, context: StreamFlowContext, persistent_id: int
    ) -> Target: ...

    @abstractmethod
    async def load_token(
        self, context: StreamFlowContext, persistent_id: int
    ) -> Token: ...

    @abstractmethod
    async def load_workflow(
        self, context: StreamFlowContext, persistent_id: int
    ) -> Workflow: ...


class PersistableEntity:
    __slots__ = ("persistent_id", "persistence_lock")

    def __init__(self) -> None:
        self.persistent_id: int | None = None
        self.persistence_lock: Lock = Lock()

    @classmethod
    @abstractmethod
    async def load(
        cls,
        context: StreamFlowContext,
        persistent_id: int,
        loading_context: DatabaseLoadingContext,
    ) -> Self: ...

    @abstractmethod
    async def save(self, context: StreamFlowContext) -> None: ...


class DependencyType(Enum):
    INPUT = 0
    OUTPUT = 1


class Database(SchemaEntity):
    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context

    @abstractmethod
    async def add_dependency(
        self, step: int, port: int, type: DependencyType, name: str
    ) -> None: ...

    @abstractmethod
    async def add_deployment(
        self,
        name: str,
        type: str,
        config: MutableMapping[str, Any],
        external: bool,
        lazy: bool,
        scheduling_policy: MutableMapping[str, Any],
        workdir: str | None,
        wraps: MutableMapping[str, Any] | None,
    ) -> int: ...

    @abstractmethod
    async def add_execution(self, step_id: int, job_token_id: int, cmd: str) -> int: ...

    @abstractmethod
    async def add_filter(
        self,
        name: str,
        type: str,
        config: MutableMapping[str, Any],
    ) -> int: ...

    @abstractmethod
    async def add_port(
        self,
        name: str,
        workflow_id: int,
        type: type[Port],
        params: MutableMapping[str, Any],
    ) -> int: ...

    @abstractmethod
    async def add_provenance(
        self, inputs: MutableSequence[int], token: int
    ) -> None: ...

    @abstractmethod
    async def add_step(
        self,
        name: str,
        workflow_id: int,
        status: int,
        type: type[Step],
        params: MutableMapping[str, Any],
    ) -> int: ...

    @abstractmethod
    async def add_target(
        self,
        deployment: int,
        type: type[Target],
        params: MutableMapping[str, Any],
        locations: int = 1,
        service: str | None = None,
        workdir: str | None = None,
    ) -> int: ...

    @abstractmethod
    async def add_token(
        self,
        tag: str,
        type: type[Token],
        value: Any,
        port: int | None = None,
        recoverable: bool = False,
    ) -> int: ...

    @abstractmethod
    async def add_workflow(
        self,
        name: str,
        params: MutableMapping[str, Any],
        status: int,
        type: type[Workflow],
    ) -> int: ...

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    async def get_dependees(
        self, token_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]: ...

    @abstractmethod
    async def get_dependers(
        self, token_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]: ...

    @abstractmethod
    async def get_deployment(self, deployment_id: int) -> MutableMapping[str, Any]: ...

    @abstractmethod
    async def get_execution(self, execution_id: int) -> MutableMapping[str, Any]: ...

    @abstractmethod
    async def get_executions_by_step(
        self, step_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]: ...

    @abstractmethod
    async def get_filter(self, filter_id: int) -> MutableMapping[str, Any]: ...

    @abstractmethod
    async def get_input_ports(
        self, step_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]: ...

    @abstractmethod
    async def get_input_steps(
        self, port_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]: ...

    @abstractmethod
    async def get_output_ports(
        self, step_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]: ...

    @abstractmethod
    async def get_output_steps(
        self, port_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]: ...

    @abstractmethod
    async def get_port(self, port_id: int) -> MutableMapping[str, Any]: ...

    @abstractmethod
    async def get_port_from_token(self, token_id: int) -> MutableMapping[str, Any]: ...

    @abstractmethod
    async def get_port_tokens(self, port_id: int) -> MutableSequence[int]: ...

    @abstractmethod
    async def get_reports(
        self, workflow: str, last_only: bool = False
    ) -> MutableSequence[MutableSequence[MutableMapping[str, Any]]]: ...

    @abstractmethod
    async def get_step(self, step_id: int) -> MutableMapping[str, Any]: ...

    @abstractmethod
    async def get_target(self, target_id: int) -> MutableMapping[str, Any]: ...

    @abstractmethod
    async def get_token(self, token_id: int) -> MutableMapping[str, Any]: ...

    @abstractmethod
    async def get_workflow(self, workflow_id: int) -> MutableMapping[str, Any]: ...

    @abstractmethod
    async def get_workflow_ports(
        self, workflow_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]: ...

    @abstractmethod
    async def get_workflow_steps(
        self, workflow_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]: ...

    @abstractmethod
    async def get_workflows_by_name(
        self, workflow_name: str, last_only: bool = False
    ) -> MutableSequence[MutableMapping[str, Any]]: ...

    @abstractmethod
    async def get_workflows_list(
        self, name: str | None
    ) -> MutableSequence[MutableMapping[str, Any]]: ...

    @abstractmethod
    async def update_deployment(
        self, deployment_id: int, updates: MutableMapping[str, Any]
    ) -> int: ...

    @abstractmethod
    async def update_execution(
        self, execution_id: int, updates: MutableMapping[str, Any]
    ) -> int: ...

    @abstractmethod
    async def update_filter(
        self, filter_id: int, updates: MutableMapping[str, Any]
    ) -> int: ...

    @abstractmethod
    async def update_port(
        self, port_id: int, updates: MutableMapping[str, Any]
    ) -> int: ...

    @abstractmethod
    async def update_step(
        self, step_id: int, updates: MutableMapping[str, Any]
    ) -> int: ...

    @abstractmethod
    async def update_target(
        self, target_id: str, updates: MutableMapping[str, Any]
    ) -> int: ...

    @abstractmethod
    async def update_workflow(
        self, workflow_id: int, updates: MutableMapping[str, Any]
    ) -> int: ...
