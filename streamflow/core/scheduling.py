from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, TYPE_CHECKING, Type, cast, MutableSet, Iterable

from streamflow.core import utils
from streamflow.core.config import BindingConfig, Config
from streamflow.core.context import SchemaEntity, StreamFlowContext
from streamflow.core.deployment import Connector, ExecutionLocation, Target
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.persistence import DatabaseLoadingContext

if TYPE_CHECKING:
    from streamflow.core.workflow import Job, Status
    from typing import MutableSequence, MutableMapping


def get_mapping_hardware(
    job_hardware: Hardware, available_locations: MutableSequence[AvailableLocation]
) -> MutableMapping[str, Hardware]:
    hardware = {}
    for location in available_locations:
        if location.hardware and job_hardware:
            storage = {}
            for path, disk in job_hardware.storage.items():
                # Set the mount_point in the job storage requirement using the current location storage information
                mount_point = get_mount_point(
                    path, list(location.hardware.storage.values())
                )
                if mount_point is None:
                    raise WorkflowExecutionException(
                        f"Impossible find the mount point of path {path}"
                    )
                storage[mount_point] = Storage(mount_point, disk.size, {path})
            hardware[location.name] = Hardware(
                job_hardware.cores, job_hardware.memory, storage
            )
        else:
            hardware[location.name] = None
    return hardware


def get_mount_point(path: str, disks: MutableSequence[Storage]) -> str | None:
    for disk in disks:
        if path in disk.paths:
            return disk.mount_point
    return None


class Hardware:
    def __init__(
        self, cores: float, memory: float, storage: MutableMapping[str, Storage]
    ):
        self.cores: float = cores
        self.memory: float = memory
        self.storage: MutableMapping[str, Storage] = storage

    def __ge__(self, other):
        if not isinstance(other, Hardware):
            return NotImplementedError
        if self.cores >= other.cores and self.memory >= other.memory:
            for disk in other.storage.values():
                if (
                    disk.mount_point not in self.storage.keys()
                    or self.storage[disk.mount_point] < disk
                ):
                    return False
            return True
        return False

    def __add__(self, other):
        if not isinstance(other, Hardware):
            return NotImplementedError
        storage = {path: disk for path, disk in self.storage.items()}
        for path, disk in other.storage.items():
            if path in storage.keys():
                storage[path] += disk
            else:
                storage[path] = disk
        return Hardware(self.cores + other.cores, self.memory + other.memory, storage)

    def __sub__(self, other):
        if not isinstance(other, Hardware):
            return NotImplementedError
        storage = {path: disk for path, disk in self.storage.items()}
        for path, disk in other.storage.items():
            if path in storage.keys():
                storage[path] -= disk
            # else do nothing
        return Hardware(self.cores - other.cores, self.memory - other.memory, storage)

    def __gt__(self, other):
        if not isinstance(other, Hardware):
            return NotImplementedError
        if self.cores > other.cores and self.memory > other.memory:
            for disk in other.storage.values():
                if (
                    disk.mount_point not in self.storage.keys()
                    or self.storage[disk.mount_point] <= disk
                ):
                    return False
            return True
        return False

    def __le__(self, other):
        if not isinstance(other, Hardware):
            return NotImplementedError
        if self.cores <= other.cores and self.memory <= other.memory:
            for disk in other.storage.values():
                if (
                    disk.mount_point not in self.storage.keys()
                    or self.storage[disk.mount_point] > disk
                ):
                    return False
            return True
        return False

    def __lt__(self, other):
        if not isinstance(other, Hardware):
            return NotImplementedError
        if self.cores < other.cores and self.memory < other.memory:
            for disk in other.storage.values():
                if (
                    disk.mount_point not in self.storage.keys()
                    or self.storage[disk.mount_point] >= disk
                ):
                    return False
            return True
        return False


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
    def eval(self, job: Job) -> Hardware: ...

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
        hardware: HardwareRequirement | None,
    ):
        self.job: str = job
        self.target: Target = target
        self.locations: MutableSequence[ExecutionLocation] = locations
        self.status: Status = status
        self.hardware: HardwareRequirement | None = hardware


class AvailableLocation:
    __slots__ = (
        "hardware",
        "location",
        "slots",
        "stacked",
        "wraps",
    )

    def __init__(
        self,
        name: str,
        deployment: str,
        hostname: str,
        service: str | None = None,
        slots: int | None = None,
        stacked: bool = False,
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
        self.slots: int | None = slots
        self.stacked: bool = stacked
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

    def get_hardware(self, job_name: str) -> HardwareRequirement | None:
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
    async def notify_status(self, job: Job, status: Status) -> None: ...

    @abstractmethod
    async def schedule(
        self,
        job: Job,
        binding_config: BindingConfig,
        hardware_requirement: HardwareRequirement | None,
    ) -> None: ...


class Storage:
    def __init__(
        self, mount_point: str | None, size: float, paths: MutableSet[str] | None = None
    ):
        self.mount_point: str | None = mount_point
        self.paths: MutableSet[str] = paths or set()
        self.size: float = size

    def add_paths(self, paths: Iterable[str]):
        for path in paths:
            self.paths.add(path)

    def __add__(self, other: Storage):
        if not isinstance(other, Storage):
            return NotImplementedError
        storage = Storage(self.mount_point or other.mount_point, self.size + other.size)
        self.add_paths(self.paths)
        self.add_paths(other.paths)
        return storage

    def __sub__(self, other: Storage):
        if not isinstance(other, Storage):
            return NotImplementedError
        storage = Storage(self.mount_point or other.mount_point, self.size - other.size)
        self.add_paths(self.paths)
        self.add_paths(other.paths)
        return storage

    def __ge__(self, other: Storage):
        if not isinstance(other, Storage):
            return NotImplementedError
        return self.size >= other.size

    def __gt__(self, other: Storage):
        if not isinstance(other, Storage):
            return NotImplementedError
        return self.size > other.size

    def __le__(self, other: Storage):
        if not isinstance(other, Storage):
            return NotImplementedError
        return self.size <= other.size

    def __lt__(self, other: Storage):
        if not isinstance(other, Storage):
            return NotImplementedError
        return self.size < other.size
