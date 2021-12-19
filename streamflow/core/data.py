from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.workflow import Job
    from typing import Optional, Set

LOCAL_RESOURCE = '__LOCAL__'


class DataManager(ABC):

    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context

    @abstractmethod
    def get_data_locations(self,
                           resource: str,
                           path: str,
                           location_type: Optional[DataLocationType] = None) -> Set[DataLocation]:
        ...

    @abstractmethod
    def invalidate_location(self,
                            resource: str,
                            path: str) -> None:
        ...

    @abstractmethod
    def register_path(self,
                      job: Optional[Job],
                      resource: Optional[str],
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
    __slots__ = ('path', 'job', 'location_type', 'resource', 'available')

    def __init__(self,
                 path: str,
                 job: Optional[str],
                 location_type: DataLocationType,
                 resource: Optional[str] = None,
                 available: bool = False):
        self.path: str = path
        self.job: Optional[str] = job
        self.resource: Optional[str] = resource
        self.location_type: DataLocationType = location_type
        self.available: asyncio.Event = asyncio.Event()
        if available:
            self.available.set()

    def __eq__(self, other):
        if not isinstance(other, DataLocation):
            return False
        else:
            return (self.path == other.path and
                    self.resource == other.resource)

    def __hash__(self):
        return hash((self.path, self.resource))


class DataLocationType(Enum):
    PRIMARY = 0
    SYMBOLIC_LINK = 1
    WRITABLE_COPY = 3
    INVALID = 4
