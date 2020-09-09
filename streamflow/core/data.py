from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from streamflow.core.deployment import Connector
    from streamflow.core.workflow import Job
    from typing import Optional
    from typing_extensions import Text


class DataManager(ABC):

    @abstractmethod
    def get_related_resources(self, resource: Text, path: Text):
        ...

    @abstractmethod
    async def register_path(self,
                            connector: Optional[Connector],
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
    __slots__ = ('path', 'resource')

    def __init__(self,
                 path: Text,
                 resource: Optional[Text] = None):
        self.path: Text = path
        self.resource: Optional[Text] = resource

    def __eq__(self, other):
        if not isinstance(other, DataLocation):
            return False
        else:
            return self.path == other.path and self.resource == other.resource

    def __hash__(self):
        return hash((self.path, self.resource))
