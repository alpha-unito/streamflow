from __future__ import annotations

import asyncio
import os
import tempfile
from typing import TYPE_CHECKING

from streamflow.core import utils
from streamflow.core.recovery import CheckpointManager
from streamflow.core.workflow import Job

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from typing import Optional, MutableSequence
    from typing_extensions import Text


class DummyCheckpointManager(CheckpointManager):

    def register_path(self, job: Optional[Job], path: Text) -> None:
        pass


class DefaultCheckpointManager(CheckpointManager):

    def __init__(self,
                 context: StreamFlowContext,
                 checkpoint_dir: Optional[Text] = None):
        super().__init__(context)
        self.checkpoint_dir = checkpoint_dir or os.path.join(
            tempfile.gettempdir(), 'streamflow', 'checkpoint', utils.random_name())
        self.copy_tasks: MutableSequence = []

    async def _async_local_copy(self, job: Job, remote_path: Text):
        parent_directory = os.path.join(self.checkpoint_dir, *job.name.split("/"))
        local_path = os.path.join(parent_directory, os.path.basename(remote_path))
        await self.context.data_manager.transfer_data(remote_path, job, local_path, None, False)

    def register_path(self, job: Optional[Job], path: Text) -> None:
        self.copy_tasks.append(asyncio.create_task(self._async_local_copy(job, path)))
