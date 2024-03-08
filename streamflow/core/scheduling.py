from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, TYPE_CHECKING, Type, cast

from streamflow.core import utils
from streamflow.core.config import BindingConfig, Config
from streamflow.core.context import SchemaEntity, StreamFlowContext
from streamflow.core.deployment import Connector, ExecutionLocation, Target
from streamflow.core.persistence import DatabaseLoadingContext

if TYPE_CHECKING:
    from streamflow.core.workflow import Job, Status, Token
    from typing import MutableSequence, MutableMapping


class Hardware:
    def __init__(
        self,
        cores: float = 0.0,
        memory: float = 0.0,
        input_directory: float = 0.0,
        output_directory: float = 0.0,
        tmp_directory: float = 0.0,
    ):
        self.cores: float = cores
        self.memory: float = memory
        self.input_directory: float = input_directory
        self.output_directory: float = output_directory
        self.tmp_directory: float = tmp_directory

    def __add__(self, other):
        if not isinstance(other, Hardware):
            return NotImplementedError
        return self.__class__(
            **{
                k: vars(self).get(k, 0.0) + vars(other).get(k, 0.0)
                for k in vars(self).keys()
            }
        )

    def __sub__(self, other):
        if not isinstance(other, Hardware):
            return NotImplementedError
        return self.__class__(
            **{
                k: vars(self).get(k, 0.0) - vars(other).get(k, 0.0)
                for k in vars(self).keys()
            }
        )

    def __ge__(self, other):
        if not isinstance(other, Hardware):
            return NotImplementedError
        return all(
            vars(self).get(k, 0.0) >= vars(other).get(k, 0.0)
            for k in set().union(vars(self).keys(), vars(other).keys())
        )

    def __gt__(self, other):
        if not isinstance(other, Hardware):
            return NotImplementedError
        return all(
            vars(self).get(k, 0.0) > vars(other).get(k, 0.0)
            for k in set().union(vars(self).keys(), vars(other).keys())
        )

    def __le__(self, other):
        if not isinstance(other, Hardware):
            return NotImplementedError
        return all(
            vars(self).get(k, 0.0) <= vars(other).get(k, 0.0)
            for k in set().union(vars(self).keys(), vars(other).keys())
        )

    def __lt__(self, other):
        if not isinstance(other, Hardware):
            return NotImplementedError
        return all(
            vars(self).get(k, 0.0) < vars(other).get(k, 0.0)
            for k in set().union(vars(self).keys(), vars(other).keys())
        )


class HardwareRequirement(ABC):
    @classmethod
    @abstractmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ): ...

    @abstractmethod
    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]: ...

    @abstractmethod
    def eval(self, inputs: MutableMapping[str, Token]) -> Hardware: ...

    @classmethod
    async def load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ):
        type = utils.get_class_from_name(row["type"])
        return await cast(Type[HardwareRequirement], type)._load(
            context, row["params"], loading_context
        )

    async def save(self, context: StreamFlowContext):
        return {
            "type": utils.get_class_fullname(type(self)),
            "params": await self._save_additional_params(context),
        }


class JobAllocation:
    __slots__ = ("job", "target", "locations", "status", "hardware")

    def __init__(
        self,
        job: str,
        target: Target,
        locations: MutableSequence[ExecutionLocation],
        status: Status,
        hardware: Hardware,
    ):
        self.job: str = job
        self.target: Target = target
        self.locations: MutableSequence[ExecutionLocation] = locations
        self.status: Status = status
        self.hardware: Hardware = hardware


class AvailableLocation:
    __slots__ = (
        "hardware",
        "location",
        "slots",
        "wraps",
    )

    def __init__(
        self,
        name: str,
        deployment: str,
        hostname: str,
        service: str | None = None,
        slots: int = 1,
        hardware: Hardware | None = None,
        wraps: AvailableLocation | None = None,
    ):
        self.hardware: Hardware | None = hardware
        self.location: ExecutionLocation = ExecutionLocation(
            deployment=deployment,
            hostname=hostname,
            name=name,
            service=service,
            wraps=wraps.location if wraps else None,
        )
        self.slots: int = slots
        self.wraps: AvailableLocation | None = wraps

    @property
    def deployment(self) -> str:
        return self.location.deployment

    @property
    def hostname(self) -> str:
        return self.location.hostname

    @property
    def name(self) -> str:
        return self.location.name

    @property
    def service(self) -> str | None:
        return self.location.service


class LocationAllocation:
    __slots__ = ("name", "deployment", "jobs")

    def __init__(self, name: str, deployment: str):
        self.name: str = name
        self.deployment: str = deployment
        self.jobs: MutableSequence[str] = []


class PolicyConfig(Config):
    async def save(self, context: StreamFlowContext):
        # TODO
        return self.config


class Policy(SchemaEntity):
    @abstractmethod
    async def get_location(
        self,
        context: StreamFlowContext,
        job: Job,
        hardware_requirement: Hardware,
        available_locations: MutableMapping[str, AvailableLocation],
        jobs: MutableMapping[str, JobAllocation],
        locations: MutableMapping[str, MutableMapping[str, LocationAllocation]],
    ) -> AvailableLocation | None: ...


class Scheduler(SchemaEntity):
    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context
        self.job_allocations: MutableMapping[str, JobAllocation] = {}
        self.location_allocations: MutableMapping[
            str, MutableMapping[str, LocationAllocation]
        ] = {}

    @abstractmethod
    async def close(self) -> None: ...

    def get_allocation(self, job_name: str) -> JobAllocation | None:
        return self.job_allocations.get(job_name)

    def get_hardware(self, job_name: str) -> Hardware | None:
        allocation = self.get_allocation(job_name)
        return allocation.hardware if allocation else None

    def get_connector(self, job_name: str) -> Connector | None:
        allocation = self.get_allocation(job_name)
        return (
            self.context.deployment_manager.get_connector(
                allocation.target.deployment.name
            )
            if allocation
            else None
        )

    def get_locations(
        self, job_name: str, statuses: MutableSequence[Status] | None = None
    ) -> MutableSequence[ExecutionLocation]:
        allocation = self.get_allocation(job_name)
        return (
            allocation.locations
            if allocation is not None
            and (statuses is None or allocation.status in statuses)
            else []
        )

    def get_service(self, job_name: str) -> str | None:
        allocation = self.get_allocation(job_name)
        return allocation.target.service if allocation else None

    @abstractmethod
    async def notify_status(self, job_name: str, status: Status) -> None: ...

    @abstractmethod
    async def schedule(
        self, job: Job, binding_config: BindingConfig, hardware_requirement: Hardware
    ) -> None: ...
