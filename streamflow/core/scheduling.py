from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, TYPE_CHECKING, Type, cast

from streamflow.core import utils
from streamflow.core.config import Config, SchemaEntity
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import Connector, Target
from streamflow.core.persistence import DatabaseLoadingContext, PersistableEntity

if TYPE_CHECKING:
    from streamflow.core.workflow import Job, Status, Token
    from typing import MutableSequence, MutableMapping, Optional


class Hardware(object):
    def __init__(self,
                 cores: float = 0.0,
                 memory: float = 0.0,
                 input_directory: float = 0.0,
                 output_directory: float = 0.0,
                 tmp_directory: float = 0.0):
        self.cores: float = cores
        self.memory: float = memory
        self.input_directory: float = input_directory
        self.output_directory: float = output_directory
        self.tmp_directory: float = tmp_directory

    def __add__(self, other):
        if not isinstance(other, Hardware):
            return NotImplemented
        return self.__class__(**{k: vars(self).get(k, 0.0) + vars(other).get(k, 0.0)
                                 for k in vars(self).keys()})

    def __sub__(self, other):
        if not isinstance(other, Hardware):
            return NotImplemented
        return self.__class__(**{k: vars(self).get(k, 0.0) - vars(other).get(k, 0.0)
                                 for k in vars(self).keys()})

    def __ge__(self, other):
        if not isinstance(other, Hardware):
            return NotImplemented
        return all((vars(self).get(k, 0.0) >= vars(other).get(k, 0.0)
                    for k in set().union(vars(self).keys(), vars(other).keys())))

    def __le__(self, other):
        if not isinstance(other, Hardware):
            return NotImplemented
        return all((vars(self).get(k, 0.0) <= vars(other).get(k, 0.0)
                    for k in set().union(vars(self).keys(), vars(other).keys())))


class HardwareRequirement(ABC):

    @classmethod
    @abstractmethod
    async def _load(cls,
                    context: StreamFlowContext,
                    row: MutableMapping[str, Any],
                    loading_context: DatabaseLoadingContext):
        ...

    @abstractmethod
    async def _save_additional_params(self, context: StreamFlowContext) -> MutableMapping[str, Any]:
        ...

    @abstractmethod
    def eval(self, inputs: MutableMapping[str, Token]) -> Hardware:
        ...

    @classmethod
    async def load(cls,
                   context: StreamFlowContext,
                   row: MutableMapping[str, Any],
                   loading_context: DatabaseLoadingContext):
        type = utils.get_class_from_name(row['type'])
        return await cast(Type[HardwareRequirement], type)._load(context, row['params'], loading_context)

    async def save(self, context: StreamFlowContext):
        return {'type': utils.get_class_fullname(type(self)),
                'params': await self._save_additional_params(context)}


class JobAllocation(object):
    __slots__ = ('job', 'target', 'locations', 'status', 'hardware')

    def __init__(self,
                 job: str,
                 target: Target,
                 locations: MutableSequence[str],
                 status: Status,
                 hardware: Hardware):
        self.job: str = job
        self.target: Target = target
        self.locations: MutableSequence[str] = locations
        self.status: Status = status
        self.hardware: Hardware = hardware


class Location(object):
    __slots__ = ('name', 'hostname', 'hardware', 'slots')

    def __init__(self,
                 name: str,
                 hostname: str,
                 slots: int = 1,
                 hardware: Optional[Hardware] = None):
        self.name: str = name
        self.hostname: str = hostname
        self.slots: int = slots
        self.hardware: Optional[Hardware] = hardware


class LocationAllocation(object):
    __slots__ = ('name', 'deployment', 'jobs')

    def __init__(self,
                 name: str,
                 deployment: str):
        self.name: str = name
        self.deployment: str = deployment
        self.jobs: MutableSequence[str] = []


class PolicyConfig(Config, PersistableEntity):

    async def save(self, context: StreamFlowContext):
        # TODO
        return self.config


class Policy(SchemaEntity):

    @abstractmethod
    async def get_location(self,
                           context: StreamFlowContext,
                           job: Job,
                           deployment: str,
                           hardware_requirement: Hardware,
                           available_locations: MutableMapping[str, Location],
                           jobs: MutableMapping[str, JobAllocation],
                           locations: MutableMapping[str, LocationAllocation]) -> Optional[str]: ...


class Scheduler(SchemaEntity):

    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context
        self.job_allocations: MutableMapping[str, JobAllocation] = {}
        self.location_allocations: MutableMapping[str, LocationAllocation] = {}

    @abstractmethod
    async def close(self):
        ...

    def get_allocation(self, job_name: str) -> Optional[JobAllocation]:
        return self.job_allocations.get(job_name)

    def get_hardware(self, job_name: str) -> Optional[Hardware]:
        allocation = self.get_allocation(job_name)
        return allocation.hardware if allocation else None

    def get_connector(self, job_name: str) -> Optional[Connector]:
        allocation = self.get_allocation(job_name)
        return self.context.deployment_manager.get_connector(allocation.target.deployment.name) if allocation else None

    def get_locations(self,
                      job_name: str,
                      statuses: Optional[MutableSequence[Status]] = None) -> MutableSequence[str]:
        allocation = self.get_allocation(job_name)
        return allocation.locations if allocation is not None and (
                statuses is None or allocation.status in statuses) else []

    def get_service(self, job_name: str) -> Optional[str]:
        allocation = self.get_allocation(job_name)
        return allocation.target.service if allocation else None

    @abstractmethod
    async def notify_status(self,
                            job_name: str,
                            status: Status):
        ...

    @abstractmethod
    async def schedule(self,
                       job: Job,
                       target: Target,
                       hardware_requirement: Hardware) -> None:
        ...
