from __future__ import annotations

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
    def get_data_locations(self, resource: Text, path: Text) -> Set[DataLocation]:
        ...

    @abstractmethod
    def invalidate_location(self, resource: Text, path: Text) -> None:
        ...

    @abstractmethod
    async def register_path(self,
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
    __slots__ = ('path', 'job', 'resource', 'valid')

    def __init__(self,
                 path: Text,
                 job: Optional[Text],
                 resource: Optional[Text] = None,
                 valid: bool = True):
        self.path: Text = path
        self.job: Optional[Text] = job
        self.resource: Optional[Text] = resource
        self.valid: bool = valid

    def __eq__(self, other):
        if not isinstance(other, DataLocation):
            return False
        else:
            return (self.path == other.path and
                    self.job == other.job and
                    self.resource == other.resource)

    def __hash__(self):
        return hash((self.path, self.job, self.resource))
