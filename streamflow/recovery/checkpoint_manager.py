from __future__ import annotations

import asyncio
import os
import tempfile
from typing import TYPE_CHECKING

from importlib_resources import files

from streamflow.core import utils
from streamflow.core.data import DataLocation
from streamflow.core.deployment import ExecutionLocation, LOCAL_LOCATION
from streamflow.core.recovery import CheckpointManager
from streamflow.core.utils import random_name

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from typing import MutableSequence


class DefaultCheckpointManager(CheckpointManager):
    def __init__(self, context: StreamFlowContext, checkpoint_dir: str | None = None):
        super().__init__(context)
        self.checkpoint_dir = checkpoint_dir or os.path.join(
            os.path.realpath(tempfile.gettempdir()),
            "streamflow",
            "checkpoint",
            utils.random_name(),
        )
        self.copy_tasks: MutableSequence = []

    async def _async_local_copy(self, data_location: DataLocation):
        parent_directory = os.path.join(self.checkpoint_dir, random_name())
        local_path = os.path.join(parent_directory, data_location.relpath)
        await self.context.data_manager.transfer_data(
            src_location=data_location.location,
            src_path=data_location.path,
            dst_locations=[
                ExecutionLocation(deployment=LOCAL_LOCATION, name=LOCAL_LOCATION)
            ],
            dst_path=local_path,
        )

    async def close(self):
        pass

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("default_checkpoint_manager.json")
            .read_text("utf-8")
        )

    def register(self, data_location: DataLocation) -> None:
        self.copy_tasks.append(
            asyncio.create_task(self._async_local_copy(data_location))
        )


class DummyCheckpointManager(CheckpointManager):
    async def close(self):
        pass

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("dummy_checkpoint_manager.json")
            .read_text("utf-8")
        )

    def register(self, data_location: DataLocation) -> None:
        pass
