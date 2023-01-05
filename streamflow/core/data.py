from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from enum import Enum
from typing import MutableSequence, TYPE_CHECKING

from streamflow.core.config import SchemaEntity
from streamflow.core.deployment import Location

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from typing import Any, Optional, Set


class DataType(Enum):
    PRIMARY = 0
    SYMBOLIC_LINK = 1
    INVALID = 2


class DataLocation(Location):
    __slots__ = ("path", "relpath", "data_type", "available")

    def __init__(
        self,
        name: str,
        path: str,
        relpath: str,
        deployment: str,
        data_type: DataType,
        service: Optional[str] = None,
        available: bool = False,
    ):
        super().__init__(name, deployment, service)
        self.path: str = path
        self.relpath: str = relpath
        self.deployment: str = deployment
        self.name: str = name
        self.data_type: DataType = data_type
        self.available: asyncio.Event = asyncio.Event()
        if available:
            self.available.set()


class DataManager(SchemaEntity):
    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context

    @abstractmethod
    async def close(self):
        ...

    @abstractmethod
    def get_data_locations(
        self,
        path: str,
        deployment: Optional[str] = None,
        location: Optional[str] = None,
        location_type: Optional[DataType] = None,
    ) -> MutableSequence[DataLocation]:
        ...

    @abstractmethod
    def get_source_location(
        self, path: str, dst_deployment: str
    ) -> Optional[DataLocation]:
        ...

    @abstractmethod
    def invalidate_location(self, location: Location, path: str) -> None:
        ...

    @abstractmethod
    def register_path(
        self,
        location: Location,
        path: str,
        relpath: str,
        data_type: DataType = DataType.PRIMARY,
    ) -> DataLocation:
        ...

    @abstractmethod
    def register_relation(
        self, src_location: DataLocation, dst_location: DataLocation
    ) -> None:
        ...

    @abstractmethod
    async def transfer_data(
        self,
        src_locations: MutableSequence[Location],
        src_path: str,
        dst_locations: MutableSequence[Location],
        dst_path: str,
        writable: bool = False,
    ):
        ...


class FileType(Enum):
    FILE = 1
    DIRECTORY = 2


class StreamWrapper(ABC):
    def __init__(self, stream):
        self.stream = stream
        self.closed = False

    @abstractmethod
    async def close(self):
        ...

    @abstractmethod
    async def read(self, size: Optional[int] = None):
        ...

    @abstractmethod
    async def write(self, data: Any):
        ...


class StreamWrapperContext(ABC):
    @abstractmethod
    async def __aenter__(self) -> StreamWrapper:
        ...

    @abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        ...
