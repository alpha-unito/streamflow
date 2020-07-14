from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from streamflow.core.workflow import Job
    from typing_extensions import Text


class DataManager(ABC):

    @abstractmethod
    async def transfer_data(self,
                            src: Text,
                            src_job: Optional[Job],
                            dst: Text,
                            dst_job: Optional[Job],
                            symlink_if_possible: bool = False):
        pass


class FileType(Enum):
    FILE = 1
    DIRECTORY = 2
