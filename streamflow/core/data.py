from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from enum import Enum
from typing import MutableSequence, TYPE_CHECKING

from streamflow.core.config import SchemaEntity

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from typing import Any, Optional, Set

LOCAL_LOCATION = '__LOCAL__'


class DataType(Enum):
    PRIMARY = 0
    SYMBOLIC_LINK = 1
    INVALID = 2


class DataLocation(object):
    __slots__ = ('path', 'relpath', 'deployment', 'data_type', 'location', 'available')

    def __init__(self,
                 path: str,
                 relpath: str,
                 deployment: str,
                 data_type: DataType,
                 location: str,
                 available: bool = False):
        self.path: str = path
        self.relpath: str = relpath
        self.deployment: str = deployment
        self.location: str = location
        self.data_type: DataType = data_type
        self.available: asyncio.Event = asyncio.Event()
        if available:
            self.available.set()

    def __eq__(self, other):
        if not isinstance(other, DataLocation):
            return False
        else:
            return (self.path == other.path and
                    self.deployment == other.deployment and
                    self.location == other.location)

    def __hash__(self):
        return hash((self.path, self.deployment, self.location))


class DataManager(SchemaEntity):

    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context

    @abstractmethod
    async def close(self):
        ...

    @abstractmethod
    def get_data_locations(self,
                           path: str,
                           deployment: Optional[str] = None,
                           location: Optional[str] = None,
                           location_type: Optional[DataType] = None) -> Set[DataLocation]:
        ...

    @abstractmethod
    def get_source_location(self,
                            path: str,
                            dst_deployment: str) -> Optional[DataLocation]:
        ...

    @abstractmethod
    def invalidate_location(self,
                            location: str,
                            path: str) -> None:
        ...

    @abstractmethod
    def register_path(self,
                      deployment: str,
                      location: str,
                      path: str,
                      relpath: str,
                      data_type: DataType = DataType.PRIMARY) -> DataLocation:
        ...

    @abstractmethod
    def register_relation(self,
                          src_location: DataLocation,
                          dst_location: DataLocation) -> None:
        ...

    @abstractmethod
    async def transfer_data(self,
                            src_deployment: str,
                            src_locations: MutableSequence[str],
                            src_path: str,
                            dst_deployment: str,
                            dst_locations: MutableSequence[str],
                            dst_path: str,
                            writable: bool = False):
        ...


class FileType(Enum):
    FILE = 1
    DIRECTORY = 2


class StreamWrapper(ABC):

    def __init__(self, stream):
        self.stream = stream
        self.closed = False

    @abstractmethod
    async def close(self): ...

    @abstractmethod
    async def read(self, size: Optional[int] = None): ...

    @abstractmethod
    async def write(self, data: Any): ...


class StreamWrapperContext(ABC):

    @abstractmethod
    async def __aenter__(self) -> StreamWrapper:
        ...

    @abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        ...
