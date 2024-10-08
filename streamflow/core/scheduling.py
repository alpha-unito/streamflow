from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Type, cast


from streamflow.core import utils
from streamflow.core.config import BindingConfig, Config
from streamflow.core.context import SchemaEntity, StreamFlowContext
from streamflow.core.deployment import Connector, ExecutionLocation, Target
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.persistence import DatabaseLoadingContext

if TYPE_CHECKING:
    from streamflow.core.workflow import Job, Status
    from typing import (
        Any,
        Callable,
        Iterable,
        MutableMapping,
        MutableSequence,
        MutableSet,
    )


def _reduce_storages(
    storages: Iterable[Storage], operator: Callable[[Storage, Any], Storage]
) -> MutableMapping[str, Storage]:
    storage = {}
    for disk in storages:
        if disk.mount_point in storage.keys():
            storage[disk.mount_point] = operator(storage[disk.mount_point], disk)
        else:
            storage[disk.mount_point] = Storage(disk.mount_point, disk.size)
    return storage


class Hardware:
    def __init__(
        self,
        cores: float = 0.0,
        memory: float = 0.0,
        storage: MutableMapping[str, Storage] | None = None,
    ):
        """
        The `Hardware` class serves as a representation of the system's resources.

        The `storage` attribute is a map with `key : storage`. The `key` meaning is left to the implementation of
        the `HardwareRequirement`. No assumption should be made about any specific behaviour of the `key` field
        outside this class. Only other classes of the same `HardwareRequirement` domain should be allowed to use
        the `key` as a fast entry point to the `Storage`. When an operation (arithmetic or comparison) is done,
        a new `Hardware` instance is created following a normalized form. In the normalized form, the `key` is equal
        to the storage's mount point, and there is only one (aggregated) `Storage` object for each mount point.

        :param cores: total number of cores
        :param memory: total number of memory
        :param storage: a map with string keys and `Storage` values
        """
        self.cores: float = cores
        self.memory: float = memory
        self.storage: MutableMapping[str, Storage] = storage or {
            os.sep: Storage(os.sep, 0.0)
        }

    def _normalize_storage(self) -> MutableMapping[str, Storage]:
        return _reduce_storages(self.storage.values(), Storage.__add__.__call__)

    def get_mount_point(self, path: str) -> str:
        return self.get_storage(path).mount_point

    def get_mount_points(self) -> MutableSequence[str]:
        return [storage.mount_point for storage in self.storage.values()]

    def get_size(self, path: str) -> float:
        return self.get_storage(path).size

    def get_storage(self, path: str) -> Storage:
        for disk in self.storage.values():
            if path == disk.mount_point or path in disk.paths:
                return disk
        raise KeyError(path)

    def __add__(self, other: Any) -> Hardware:
        if not isinstance(other, Hardware):
            raise NotImplementedError
        return Hardware(
            self.cores + other.cores,
            self.memory + other.memory,
            _reduce_storages(
                (
                    *self._normalize_storage().values(),
                    *other._normalize_storage().values(),
                ),
                Storage.__add__.__call__,
            ),
        )

    def __sub__(self, other: Any) -> Hardware:
        if not isinstance(other, Hardware):
            raise NotImplementedError
        return Hardware(
            self.cores - other.cores,
            self.memory - other.memory,
            _reduce_storages(
                (
                    *self._normalize_storage().values(),
                    *other._normalize_storage().values(),
                ),
                Storage.__sub__.__call__,
            ),
        )

    def __ge__(self, other: Any) -> bool:
        if not isinstance(other, Hardware):
            raise NotImplementedError
        if self.cores >= other.cores and self.memory >= other.memory:
            normalized_storages = self._normalize_storage()
            return all(
                normalized_storages[disk.mount_point] >= disk
                for disk in other._normalize_storage().values()
            )
        else:
            return False

    def __gt__(self, other: Any) -> bool:
        if not isinstance(other, Hardware):
            raise NotImplementedError
        if self.cores > other.cores and self.memory > other.memory:
            normalized_storages = self._normalize_storage()
            return all(
                normalized_storages[disk.mount_point] > disk
                for disk in other._normalize_storage().values()
            )
        else:
            return False

    def __le__(self, other: Any) -> bool:
        if not isinstance(other, Hardware):
            raise NotImplementedError
        if self.cores <= other.cores and self.memory <= other.memory:
            normalized_storages = self._normalize_storage()
            return all(
                normalized_storages[disk.mount_point] <= disk
                for disk in other._normalize_storage().values()
            )
        else:
            return False

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, Hardware):
            raise NotImplementedError
        if self.cores < other.cores and self.memory < other.memory:
            normalized_storages = self._normalize_storage()
            return all(
                normalized_storages[disk.mount_point] < disk
                for disk in other._normalize_storage().values()
            )
        else:
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

    def __str__(self):
        return self.location.__str__()


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
        self,
        job: Job,
        binding_config: BindingConfig,
        hardware_requirement: HardwareRequirement | None,
    ) -> None: ...


class Storage:
    def __init__(
        self, mount_point: str, size: float, paths: MutableSet[str] | None = None
    ):
        """
        The `Storage` class represents a persistent volume

        :param mount_point: the path of the volume's mount point
        :param size: the total size of the volume, expressed in Kilobyte
        :param paths: a list of paths inside the volume
        """
        self.mount_point: str = mount_point
        self.paths: MutableSet[str] = paths or set()
        if size < 0:
            raise WorkflowExecutionException(
                f"Storage {mount_point} with {paths} paths cannot have negative size: {size}"
            )
        self.size: float = size

    def add_path(self, path: str):
        self.paths.add(path)

    def __add__(self, other: Any) -> Storage:
        if not isinstance(other, Storage):
            raise NotImplementedError
        if self.mount_point != other.mount_point:
            raise WorkflowExecutionException(
                f"Cannot sum two storages with different mount points: {self.mount_point} and {other.mount_point}"
            )
        return Storage(
            mount_point=self.mount_point,
            size=self.size + other.size,
            paths=self.paths | other.paths,
        )

    def __sub__(self, other: Any) -> Storage:
        if not isinstance(other, Storage):
            raise NotImplementedError
        if self.mount_point != other.mount_point:
            raise WorkflowExecutionException(
                f"Cannot subtract two storages with different mount points: {self.mount_point} and {other.mount_point}"
            )
        return Storage(
            mount_point=self.mount_point,
            size=self.size - other.size,
            paths=self.paths | other.paths,
        )

    def __ge__(self, other: Any) -> bool:
        if not isinstance(other, Storage):
            raise NotImplementedError
        if self.mount_point != other.mount_point:
            raise KeyError(
                f"Cannot compare two storages with different mount points: {self.mount_point} and {other.mount_point}"
            )
        return self.size >= other.size

    def __gt__(self, other: Any) -> bool:
        if not isinstance(other, Storage):
            raise NotImplementedError
        if self.mount_point != other.mount_point:
            raise KeyError(
                f"Cannot compare two storages with different mount points: {self.mount_point} and {other.mount_point}"
            )
        return self.size > other.size

    def __le__(self, other: Any) -> bool:
        if not isinstance(other, Storage):
            raise NotImplementedError
        if self.mount_point != other.mount_point:
            raise KeyError(
                f"Cannot compare two storages with different mount points: {self.mount_point} and {other.mount_point}"
            )
        return self.size <= other.size

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, Storage):
            raise NotImplementedError
        if self.mount_point != other.mount_point:
            raise KeyError(
                f"Cannot compare two storages with different mount points: {self.mount_point} and {other.mount_point}"
            )
        return self.size < other.size
