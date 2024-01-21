from __future__ import annotations

import asyncio
import logging
import os
import tempfile
from collections.abc import MutableSequence
from importlib.resources import files
from typing import TYPE_CHECKING

from streamflow.core.data import DataLocation
from streamflow.core.deployment import Connector, LocalTarget
from streamflow.core.exception import (
    WorkflowDefinitionException,
    WorkflowExecutionException,
)
from streamflow.core.recovery import CheckpointManager
from streamflow.core.utils import get_token_paths, random_name
from streamflow.core.workflow import Job, Token
from streamflow.data.remotepath import StreamFlowPath
from streamflow.data.utils import get_mount_point
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext


class DefaultCheckpointManager(CheckpointManager):
    def __init__(
        self,
        context: StreamFlowContext,
        checkpoint_dir: str | None = None,
        deployment: str = None,
        service: str = None,
    ):
        super().__init__(context)
        if deployment is None:
            deployment = LocalTarget.deployment_name
        elif deployment not in context.config.get("deployments"):
            raise WorkflowDefinitionException(
                f"Checkpoint cannot use deployment {deployment} because it is not defined"
            )

        self.checkpoint_dir: str = checkpoint_dir or os.path.join(
            os.path.realpath(tempfile.gettempdir()),
            "streamflow",
            "checkpoint",
            random_name(),
        )
        self.connector: Connector | None = None
        self.deployment: str = deployment
        self.service: str = service
        self._copy_tasks: MutableSequence[asyncio.Task] = []

    async def _async_copy(self, data_locations: MutableSequence[DataLocation]) -> None:
        times = 0
        while self.connector is None:
            # todo: implement a smarter deployment wait strategy
            # fixme: the `deployment` is not initialized if no step is not mapped into it
            self.connector = self.context.deployment_manager.get_connector(
                self.deployment
            )
            await asyncio.sleep(0.30)
            times += 1
            if times == 30:
                raise WorkflowExecutionException(
                    f"FAILED checkpointing of {next(iter(data_locations)).path} of {next(iter(data_locations)).location} "
                    f"because the deployment {self.deployment} has timed out"
                )
        # todo: improve the copy optimization in case of stacked locations
        dst_locations = await self.connector.get_available_locations(self.service)

        if (
            info_location := next(
                iter(
                    (data_loc, dst_loc)
                    for data_loc in data_locations
                    for dst_loc in dst_locations.values()
                    if dst_loc.name == data_loc.name
                )
            )
        ) is not None:
            # data has already a copy in the checkpoint location
            if (
                len(
                    set(
                        await asyncio.gather(
                            *(
                                asyncio.create_task(
                                    get_mount_point(
                                        context=self.context,
                                        connector=self.connector,
                                        location=info_location[1],
                                        path=path,
                                    )
                                )
                                for path in (self.checkpoint_dir, info_location[0].path)
                            )
                        )
                    )
                )
                == 1
            ):
                # data are in same location and same mount point: copy not necessary
                return
            else:
                # data are in the same location but on different mount points
                writable = True
        else:
            data_location = next(iter(data_locations))
            info_location = data_location, next(
                loc for loc in dst_locations.values() if data_location.name == loc.name
            )
            writable = False
        workdir = StreamFlowPath(
            self.checkpoint_dir,
            random_name(),
            context=self.context,
            location=next(iter(dst_locations.values())).location,
        )
        await workdir.mkdir(parents=True)
        dst_path = os.path.join(str(workdir), info_location[0].relpath)
        await self.context.data_manager.transfer_data(
            src_location=info_location[1].location,
            src_path=info_location[0].path,
            dst_locations=[loc.location for loc in dst_locations.values()],
            dst_path=dst_path,
            writable=writable,
        )
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"COMPLETED checkpointing of {info_location[0].path} from location {info_location[1].name}"
            )

    async def close(self) -> None:
        # todo: add an attribute in the manager configuration to choose whether to cancel the copy or
        #  termite safely. In this last case, it is necessary that DeploymentManager does not close the
        #  deployments involved
        for alive_task in (t for t in self._copy_tasks if not t.done()):
            alive_task.cancel()

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("default_checkpoint_manager.json")
            .read_text("utf-8")
        )

    def save_data(self, job: Job, token: Token) -> None:
        self._copy_tasks.extend(
            asyncio.create_task(
                self._async_copy(self.context.data_manager.get_data_locations(path))
            )
            for path in get_token_paths(self.context, token)
        )


class DummyCheckpointManager(CheckpointManager):
    async def close(self) -> None:
        pass

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("dummy_checkpoint_manager.json")
            .read_text("utf-8")
        )

    def save_data(self, job: Job, token: Token) -> None:
        pass
