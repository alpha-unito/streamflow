from __future__ import annotations

import copy
import os
from abc import ABC, abstractmethod
from collections.abc import Iterable, MutableMapping, MutableSequence, MutableSet
from typing import TYPE_CHECKING, Any, Callable, cast

from streamflow.core import utils
from streamflow.core.exception import WorkflowExecutionException

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.persistence import DatabaseLoadingContext
    from streamflow.core.workflow import Job


def _reduce_storages(
    storages: Iterable[Storage], operator: Callable[[Storage, Any], Storage]
) -> MutableMapping[str, Storage]:
    storage = {}
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


class Device:
    __slots__ = ("capabilities", "vendor")

    def __init__(self, capabilities: MutableMapping[str, Any], vendor: str):
        self.vendor = vendor
        self.capabilities = capabilities

    def _check_default(self, name: str, value: Any):
        return name in self.capabilities and self.capabilities[name] == value

    def check_capability(self, name: str, value: Any) -> bool:
        return (
            getattr(self, f"check_{name}")(value)
            if hasattr(self, f"check_{name}")
            else self._check_default(name, value)
        )

    def satisfies(self, device: Device) -> bool:
        return self.vendor == device.vendor and all(
            self.check_capability(k, v) for k, v in device.capabilities.items()
        )


class Hardware:
    def __init__(
        self,
        cores: float = 0.0,
        memory: float = 0.0,
        storage: MutableMapping[str, Storage] | None = None,
        devices: MutableSequence[Device] | None = None,
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
        :param: devices: a list of devices and their characteristics
        """
        self.cores: float = cores
        self.memory: float = memory
        self.storage: MutableMapping[str, Storage] = storage or {
            os.sep: Storage(os.sep, 0.0)
        }
        self.devices: MutableSequence[Device] = devices or []

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

    def __repr__(self):
        return f"Hardware(cores={self.cores}, memory={self.memory}, storage={self.storage})"

    def __add__(self, other: Any) -> Hardware:
        if not isinstance(other, Hardware):
            raise NotImplementedError
        return Hardware(
            cores=self.cores + other.cores,
            memory=self.memory + other.memory,
            storage=_reduce_storages(
                (
                    *self._normalize_storage().values(),
                    *other._normalize_storage().values(),
                ),
                Storage.__add__.__call__,
            ),
            devices=[*self.devices, *other.devices],
        )

    def __ior__(self, other) -> None:
        if not isinstance(other, Hardware):
            raise NotImplementedError
        self.cores += other.cores
        self.memory += other.memory
        for key, disk in other.storage.items():
            if key not in self.storage.keys():
                self.storage[key] = disk
            else:
                self.storage[key] |= disk
        self.devices.extend(other.devices)

    def __or__(self, other) -> Hardware:
        if not isinstance(other, Hardware):
            raise NotImplementedError
        hardware = copy.deepcopy(self)
        hardware |= other
        return hardware

    def __sub__(self, other: Any) -> Hardware:
        if not isinstance(other, Hardware):
            raise NotImplementedError
        devices = list(self.devices)
        for device in other.devices:
            if dev := next((dev for dev in devices if dev.satisfies(device)), None):
                devices.remove(dev)
        return Hardware(
            cores=self.cores - other.cores,
            memory=self.memory - other.memory,
            storage=_reduce_storages(
                (
                    *self._normalize_storage().values(),
                    *other._normalize_storage().values(),
                ),
                Storage.__sub__.__call__,
            ),
            devices=devices,
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
            if all(
                self_norm[other_disk.mount_point].size >= other_disk.size
                for other_disk in other_norm.values()
            ):
                devices = list(self.devices)
                for device in other.devices:
                    if dev := next(
                        (dev for dev in devices if dev.satisfies(device)), None
                    ):
                        devices.remove(dev)
                    else:
                        return False
                return True
            else:
                return False
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
        type_ = cast(type[HardwareRequirement], utils.get_class_from_name(row["type"]))
        return await type_._load(context, row["params"], loading_context)

    async def save(self, context: StreamFlowContext):
        return {
            "type": utils.get_class_fullname(type(self)),
            "params": await self._save_additional_params(context),
        }


class Storage:
    __slots__ = ("mount_point", "size", "paths", "bind")

    def __init__(
        self,
        mount_point: str,
        size: float,
        paths: MutableSet[str] | None = None,
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

    def add_path(self, path: str):
        self.paths.add(path)

    def __repr__(self):
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

    def __ior__(self, other) -> None:
        if not isinstance(other, Storage):
            raise NotImplementedError
        if self.mount_point != other.mount_point:
            raise WorkflowExecutionException(
                f"Cannot merge two storages with different mount points: {self.mount_point} and {other.mount_point}"
            )
        self.size = max(self.size, other.size)
        self.paths |= other.paths

    def __or__(self, other) -> Storage:
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
