from __future__ import annotations

import asyncio
import logging
import os
import tempfile
from typing import TYPE_CHECKING

import pkg_resources

from streamflow.core import utils
from streamflow.core.data import DataLocation
from streamflow.core.deployment import LOCAL_LOCATION
from streamflow.core.exception import WorkflowDefinitionException
from streamflow.core.recovery import CheckpointManager
from streamflow.core.utils import random_name
from streamflow.core.workflow import Token
from streamflow.log_handler import logger
from streamflow.recovery.utils import get_files_from_token

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from typing import MutableSequence


class DefaultCheckpointManager(CheckpointManager):
    def __init__(
        self,
        context: StreamFlowContext,
        checkpoint_dir: str | None = None,
        deployment: str = None,
        service: str = None,
    ):
        super().__init__(context)
        self.checkpoint_dir = checkpoint_dir or os.path.join(
            os.path.realpath(tempfile.gettempdir()),
            "streamflow",
            "checkpoint",
            utils.random_name(),
        )
        if deployment is None:
            self.deployment_name = LOCAL_LOCATION
        elif deployment in context.config.get("deployments"):
            self.deployment_name = deployment
        else:
            raise WorkflowDefinitionException(
                f"Checkpoint cannot use deployment {deployment} because it is not defined"
            )
        self.service = service
        self.copy_tasks: MutableSequence[asyncio.Task] = []

    async def _async_local_copy(self, data_location: DataLocation):
        parent_directory = os.path.join(self.checkpoint_dir, random_name())
        local_path = os.path.join(parent_directory, data_location.relpath)
        dst_location = list(
            (
                await self.context.deployment_manager.get_connector(
                    self.deployment_name
                ).get_available_locations(self.service)
            ).values()
        )[0]
        # writable is setted to True, in this way it is not created a symbolic link if file is available in the same location
        await self.context.data_manager.transfer_data(
            src_locations=[data_location],
            src_path=data_location.path,
            dst_locations=[dst_location],
            dst_path=local_path,
            writable=True,
        )
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"CHECKPOINT file {data_location.path} in location {data_location.name} to {local_path} in location {dst_location.name}"
            )

    async def close(self):
        for alive_task in (t for t in self.copy_tasks if not t.done()):
            alive_task.cancel()

    async def wait(self):
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                f"CHECKPOINT active copy tasks: {len([t for t in self.copy_tasks if not t.done()])}"
            )
        await asyncio.gather(*(t for t in self.copy_tasks if not t.done()))

    def save_data(self, token: Token):
        for file in get_files_from_token(token):
            data_location = self.context.data_manager.get_data_locations(file)[0]
            self.copy_tasks.append(
                asyncio.create_task(self._async_local_copy(data_location))
            )

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "default_checkpoint_manager.json")
        )


class DummyCheckpointManager(CheckpointManager):
    async def close(self):
        pass

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "dummy_checkpoint_manager.json")
        )

    async def wait(self):
        pass

    def save_data(self, token: Token):
        pass
