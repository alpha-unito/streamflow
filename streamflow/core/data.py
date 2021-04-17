from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.workflow import Job
    from typing import Optional, Set
    from typing_extensions import Text

LOCAL_RESOURCE = '__LOCAL__'


class DataManager(ABC):

    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context

    @abstractmethod
    def get_data_locations(self,
                           resource: Text,
                           path: Text,
                           location_type: Optional[DataLocationType] = None) -> Set[DataLocation]:
        ...

    @abstractmethod
    def invalidate_location(self,
                            resource: Text,
                            path: Text) -> None:
        ...

    @abstractmethod
    def register_path(self,
                      job: Optional[Job],
                      resource: Optional[Text],
                      path: Text):
        ...

    @abstractmethod
    async def transfer_data(self,
                            src: Text,
                            src_job: Optional[Job],
                            dst: Text,
                            dst_job: Optional[Job],
                            writable: bool = False):
        ...


class FileType(Enum):
    FILE = 1
    DIRECTORY = 2


class DataLocation(object):
    __slots__ = ('path', 'job', 'location_type', 'resource', 'available')

    def __init__(self,
                 path: Text,
                 job: Optional[Text],
                 location_type: DataLocationType,
                 resource: Optional[Text] = None,
                 available: bool = False):
        self.path: Text = path
        self.job: Optional[Text] = job
        self.resource: Optional[Text] = resource
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
