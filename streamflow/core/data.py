from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.workflow import Job
    from typing import Optional, Set

LOCAL_LOCATION = '__LOCAL__'


class DataManager(ABC):

    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context

    @abstractmethod
    def get_data_locations(self,
                           location: str,
                           path: str,
                           location_type: Optional[DataType] = None) -> Set[DataLocation]:
        ...

    @abstractmethod
    def invalidate_location(self,
                            location: str,
                            path: str) -> None:
        ...

    @abstractmethod
    def register_path(self,
                      job: Optional[Job],
                      location: Optional[str],
                      path: str):
        ...

    @abstractmethod
    async def transfer_data(self,
                            src: str,
                            src_job: Optional[Job],
                            dst: str,
                            dst_job: Optional[Job],
                            writable: bool = False):
        ...


class FileType(Enum):
    FILE = 1
    DIRECTORY = 2


class DataLocation(object):
    __slots__ = ('path', 'job', 'data_type', 'location', 'available')

    def __init__(self,
                 path: str,
                 job: Optional[str],
                 data_type: DataType,
                 location: Optional[str] = None,
                 available: bool = False):
        self.path: str = path
        self.job: Optional[str] = job
        self.location: Optional[str] = location
        self.data_type: DataType = data_type
        self.available: asyncio.Event = asyncio.Event()
        if available:
            self.available.set()

    def __eq__(self, other):
        if not isinstance(other, DataLocation):
            return False
        else:
            return (self.path == other.path and
                    self.location == other.location)

    def __hash__(self):
        return hash((self.path, self.location))


class DataType(Enum):
    PRIMARY = 0
    SYMBOLIC_LINK = 1
    WRITABLE_COPY = 3
    INVALID = 4
