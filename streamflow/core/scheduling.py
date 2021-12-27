from __future__ import annotations

from abc import abstractmethod, ABC
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from streamflow.core.workflow import Job, Status
    from typing import MutableSequence, MutableMapping, Optional


class JobAllocation(object):
    __slots__ = ('job', 'locations', 'status', 'hardware')

    def __init__(self,
                 job: Job,
                 locations: MutableSequence[str],
                 status: Status,
                 hardware: Hardware):
        self.job: Job = job
        self.locations: MutableSequence[str] = locations
        self.status: Status = status
        self.hardware: Hardware = hardware


class Policy(ABC):

    @abstractmethod
    def get_location(self,
                     job: Job,
                     available_locations: MutableMapping[str, Location],
                     jobs: MutableMapping[str, JobAllocation],
                     locations: MutableMapping[str, LocationAllocation]) -> Optional[str]: ...


class Hardware(object):
    __slots__ = ('cores', 'memory', 'disk')

    def __init__(self,
                 cores: float = 0.0,
                 memory: float = 0.0,
                 disk: float = 0.0):
        self.cores: float = cores
        self.memory: float = memory
        self.disk: float = disk

    def __add__(self, other):
        if not isinstance(other, Hardware):
            return NotImplemented
        return Hardware(
            cores=self.cores + other.cores,
            memory=self.memory + other.memory,
            disk=self.disk + other.disk)

    def __sub__(self, other):
        if not isinstance(other, Hardware):
            return NotImplemented
        return Hardware(
            cores=self.cores - other.cores,
            memory=self.memory - other.memory,
            disk=self.disk - other.disk)

    def __ge__(self, other):
        if not isinstance(other, Hardware):
            return NotImplemented
        return (self.cores >= other.cores and
                self.memory >= other.memory and
                self.disk >= other.disk)

    def __gt__(self, other):
        if not isinstance(other, Hardware):
            return NotImplemented
        return (self.cores > other.cores and
                self.memory > other.memory and
                self.disk > other.disk)

    def __le__(self, other):
        if not isinstance(other, Hardware):
            return NotImplemented
        return (self.cores <= other.cores and
                self.memory <= other.memory and
                self.disk <= other.disk)

    def __lt__(self, other):
        if not isinstance(other, Hardware):
            return NotImplemented
        return (self.cores < other.cores and
                self.memory < other.memory and
                self.disk < other.disk)


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


class Scheduler(ABC):

    def __init__(self):
        self.job_allocations: MutableMapping[str, JobAllocation] = {}
        self.location_allocations: MutableMapping[str, LocationAllocation] = {}

    def get_job(self, job_name: str) -> Optional[Job]:
        job = self.job_allocations.get(job_name, None)
        return job.job if job is not None else None

    def get_locations(self,
                      job_name: str,
                      statuses: Optional[MutableSequence[Status]] = None) -> MutableSequence[str]:
        job = self.job_allocations.get(job_name, None)
        return job.locations if job is not None and (statuses is None or job.status in statuses) else []

    @abstractmethod
    async def notify_status(self,
                            job_name: str,
                            status: Status):
        ...

    @abstractmethod
    async def schedule(self,
                       job: Job,
                       scheduling_policy: Policy = None):
        ...
