from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from enum import Enum
from typing import MutableSequence, TYPE_CHECKING

from streamflow.core.context import SchemaEntity

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.deployment import ExecutionLocation
    from typing import Any


class DataType(Enum):
    PRIMARY = 0
    SYMBOLIC_LINK = 1
    INVALID = 2


class DataLocation:
    __slots__ = (
        "available",
        "data_type",
        "location",
        "path",
        "relpath",
    )

    def __init__(
        self,
        location: ExecutionLocation,
        path: str,
        relpath: str,
        data_type: DataType,
        available: bool = False,
    ):
        self.available: asyncio.Event = asyncio.Event()
        self.data_type: DataType = data_type
        self.location: ExecutionLocation = location
        self.path: str = path
        self.relpath: str = relpath
        if available:
            self.available.set()

    @property
    def deployment(self) -> str:
        return self.location.deployment

    @property
    def name(self) -> str:
        return self.location.name

    @property
    def service(self) -> str | None:
        return self.location.service

    @property
    def wraps(self) -> ExecutionLocation | None:
        return self.location.wraps


class DataManager(SchemaEntity):
    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    def get_data_locations(
        self,
        path: str,
        deployment: str | None = None,
        location_name: str | None = None,
        data_type: DataType | None = None,
    ) -> MutableSequence[DataLocation]: ...

    @abstractmethod
    def get_source_location(
        self, path: str, dst_deployment: str
    ) -> DataLocation | None: ...

    @abstractmethod
    def invalidate_location(self, location: ExecutionLocation, path: str) -> None: ...

    @abstractmethod
    def register_path(
        self,
        location: ExecutionLocation,
        path: str,
        relpath: str | None = None,
        data_type: DataType = DataType.PRIMARY,
    ) -> DataLocation: ...

    @abstractmethod
    def register_relation(
        self, src_location: DataLocation, dst_location: DataLocation
    ) -> None: ...

    @abstractmethod
    async def transfer_data(
        self,
        src_location: ExecutionLocation,
        src_path: str,
        dst_locations: MutableSequence[ExecutionLocation],
        dst_path: str,
        writable: bool = False,
    ) -> None: ...


class FileType(Enum):
    FILE = 1
    DIRECTORY = 2


class StreamWrapper(ABC):
    def __init__(self, stream: Any):
        self.stream: Any = stream

    @abstractmethod
    async def close(self): ...

    @abstractmethod
    async def read(self, size: int | None = None): ...

    @abstractmethod
    async def write(self, data: Any): ...


class StreamWrapperContextManager(ABC):
    @abstractmethod
    async def __aenter__(self) -> StreamWrapper: ...

    @abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None: ...
