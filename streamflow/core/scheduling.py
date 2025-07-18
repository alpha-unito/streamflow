from __future__ import annotations

import copy
import os
from abc import ABC, abstractmethod
from collections.abc import (
    Callable,
    Iterable,
    MutableMapping,
    MutableSequence,
    MutableSet,
)
from typing import TYPE_CHECKING, AbstractSet, cast

from typing_extensions import Self

from streamflow.core import utils
from streamflow.core.config import BindingConfig, Config
from streamflow.core.context import SchemaEntity, StreamFlowContext
from streamflow.core.deployment import ExecutionLocation
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.persistence import DatabaseLoadingContext

if TYPE_CHECKING:
    from typing import Any

    from streamflow.core.deployment import Connector, Target
    from streamflow.core.workflow import Job, Status


def _reduce_storages(
    storages: Iterable[Storage], operator: Callable[[Storage, Any], Storage]
) -> MutableMapping[str, Storage]:
    storage: MutableMapping[str, Storage] = {}
    for disk in storages:
        if disk.mount_point in storage.keys():
            storage[disk.mount_point] = operator(storage[disk.mount_point], disk)
        else:
            storage[disk.mount_point] = Storage(
                mount_point=disk.mount_point,
                size=disk.size,
                bind=disk.bind,
            )
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
        Instead, the merge operators preserve the existing storage keys, but pretend that all `Storage` objects
        under the same key expose the same mount point. Conversely, the operation throws an exception.

        :param cores: total number of cores
        :param memory: total amount of memory (in MB)
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
        return list({storage.mount_point for storage in self.storage.values()})

    def get_size(self, path: str) -> float:
        return self.get_storage(path).size

    def get_storage(self, path: str) -> Storage:
        for disk in self.storage.values():
            if path == disk.mount_point or path in disk.paths:
                return disk
        raise KeyError(path)

    def __repr__(self) -> str:
        return f"Hardware(cores={self.cores}, memory={self.memory}, storage={self.storage})"

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

    def __ior__(self, other: Hardware) -> None:
        if not isinstance(other, Hardware):
            raise NotImplementedError
        self.cores += other.cores
        self.memory += other.memory
        for key, disk in other.storage.items():
            if key not in self.storage.keys():
                self.storage[key] = disk
            else:
                self.storage[key] |= disk

    def __or__(self, other: Hardware) -> Hardware:
        if not isinstance(other, Hardware):
            raise NotImplementedError
        hardware = copy.deepcopy(self)
        hardware |= other
        return hardware

    def __sub__(self, other: Hardware) -> Hardware:
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

    def satisfies(self, other: Any) -> bool:
        """Check if this hardware has enough resources to satisfy the requirement."""
        if not isinstance(other, Hardware):
            raise NotImplementedError
        if self.cores >= other.cores and self.memory >= other.memory:
            if set((other_norm := other._normalize_storage()).keys()) - set(
                (self_norm := self._normalize_storage()).keys()
            ):
                raise WorkflowExecutionException(
                    f"Invalid `Hardware` comparison: {self} should contain all the storage included in {other}."
                )
            return all(
                self_norm[other_disk.mount_point].size >= other_disk.size
                for other_disk in other_norm.values()
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
    ) -> Self: ...

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
    ) -> Self:
        type_ = cast(Self, utils.get_class_from_name(row["type"]))
        return await type_._load(context, row["params"], loading_context)

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
        local: bool = False,
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
            local=local,
            mounts=(
                {
                    storage.mount_point: storage.bind
                    for storage in self.hardware.storage.values()
                    if storage.bind is not None
                }
                if hardware is not None
                else {}
            ),
            name=name,
            service=service,
            stacked=stacked,
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
    def local(self) -> bool:
        return self.location.local

    @property
    def name(self) -> str:
        return self.location.name

    @property
    def service(self) -> str | None:
        return self.location.service

    def __str__(self) -> str:
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

    def get_allocation(self, job_name: str) -> JobAllocation:
        if (allocation := self.job_allocations.get(job_name)) is None:
            raise WorkflowExecutionException(
                f"Could not retrieve allocation for job {job_name}"
            )
        return allocation

    def get_hardware(self, job_name: str) -> Hardware:
        return self.get_allocation(job_name).hardware

    def get_connector(self, job_name: str) -> Connector:
        allocation = self.get_allocation(job_name)
        if (
            connector := self.context.deployment_manager.get_connector(
                allocation.target.deployment.name
            )
        ) is None:
            raise WorkflowExecutionException(
                f"Could not retrieve connector for job {job_name}"
            )
        return connector

    def get_locations(
        self, job_name: str, statuses: MutableSequence[Status] | None = None
    ) -> MutableSequence[ExecutionLocation]:
        allocation = self.get_allocation(job_name)
        return (
            allocation.locations
            if statuses is None or allocation.status in statuses
            else []
        )

    def get_service(self, job_name: str) -> str | None:
        return self.get_allocation(job_name).target.service

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
    __slots__ = ("mount_point", "size", "paths", "bind")

    def __init__(
        self,
        mount_point: str,
        size: float,
        paths: AbstractSet[str] | None = None,
        bind: str | None = None,
    ):
        """
        The `Storage` class represents a persistent volume

        :param mount_point: the path of the volume's mount point
        :param size: the total size of the volume, expressed in Kilobyte
        :param paths: a list of paths inside the volume
        :param bind: the path bound by the volume, if any
        """
        self.mount_point: str = mount_point
        self.paths: MutableSet[str] = paths or set()
        if size < 0:
            raise WorkflowExecutionException(
                f"Storage {mount_point} with {paths} paths cannot have negative size: {size}"
            )
        self.size: float = size
        self.bind: str | None = bind

    def add_path(self, path: str) -> None:
        self.paths.add(path)

    def __repr__(self) -> str:
        return f"Storage(mount_point={self.mount_point}, size={self.size}, bind={self.bind}, paths={self.paths})"

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
            bind=self.bind,
        )

    def __ior__(self, other: Storage) -> None:
        if not isinstance(other, Storage):
            raise NotImplementedError
        if self.mount_point != other.mount_point:
            raise WorkflowExecutionException(
                f"Cannot merge two storages with different mount points: {self.mount_point} and {other.mount_point}"
            )
        self.size = max(self.size, other.size)
        self.paths |= other.paths

    def __or__(self, other: Storage) -> Storage:
        if not isinstance(other, Storage):
            raise NotImplementedError
        if self.mount_point != other.mount_point:
            raise WorkflowExecutionException(
                f"Cannot merge two storages with different mount points: {self.mount_point} and {other.mount_point}"
            )
        return Storage(
            mount_point=self.mount_point,
            size=max(self.size, other.size),
            paths=self.paths | other.paths,
            bind=self.bind,
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
            bind=self.bind,
        )
