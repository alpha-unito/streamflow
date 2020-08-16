from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from streamflow.core.data import DataManager
from streamflow.data import remotepath
from streamflow.deployment.base import ConnectorCopyKind

if TYPE_CHECKING:
    from streamflow.core.context import StreamflowContext
    from streamflow.core.workflow import Job
    from typing import Optional
    from typing_extensions import Text


class DefaultDataManager(DataManager):

    def __init__(self, context: StreamflowContext) -> None:
        super().__init__()
        self.context = context

    async def transfer_data(self,
                            src: Text,
                            src_job: Optional[Job],
                            dst: Text,
                            dst_job: Optional[Job],
                            symlink_if_possible: bool = False):
        # Get connectors and resources from tasks
        src_connector = src_job.task.get_connector() if src_job is not None else None
        src_resource = src_job.get_resource() if src_job is not None else None
        dst_connector = dst_job.task.get_connector() if dst_job is not None else None
        dst_resources = dst_job.get_resources() if dst_job is not None else []
        # Create destination folder
        await remotepath.mkdir(dst_connector, dst_resources, str(Path(dst).parent))
        # Follow symlink for source path
        src = await remotepath.follow_symlink(src_connector, src_resource, src)
        # If tasks are both local
        if src_connector is None and dst_connector is None:
            if src != dst:
                if symlink_if_possible:
                    os.symlink(os.path.abspath(src), dst, target_is_directory=os.path.isdir(dst))
                else:
                    if os.path.isdir(src):
                        os.makedirs(dst, exist_ok=True)
                        shutil.copytree(src, dst, dirs_exist_ok=True)
                    else:
                        shutil.copy(src, dst)
        # If tasks are scheduled on the same model
        elif src_connector == dst_connector:
            remote_resources = []
            for dst_resource in dst_resources:
                # If tasks are scheduled on the same resource and it is possible to link, only create a symlink
                if len(dst_resources) == 1 and src_resource == dst_resource and src != dst and symlink_if_possible:
                    await remotepath.symlink(dst_connector, dst_resource, src, dst)
                # Otherwise perform a remote copy managed by the connector
                else:
                    remote_resources.append(dst_resource)
            if remote_resources:
                await dst_connector.copy(src, dst, remote_resources, ConnectorCopyKind.REMOTE_TO_REMOTE, src_resource)
        # If source task is local, copy files to the remote resources
        elif src_connector is None:
            await dst_connector.copy(src, dst, dst_resources, ConnectorCopyKind.LOCAL_TO_REMOTE)
        # If destination task is local, copy files from the remote resource
        elif dst_connector is None:
            await src_connector.copy(src, dst, [src_resource], ConnectorCopyKind.REMOTE_TO_LOCAL)
        # If tasks are both remote and scheduled on different models, perform an intermediate local copy
        else:
            temp_dir = tempfile.mkdtemp()
            await src_connector.copy(src, temp_dir, [src_resource], ConnectorCopyKind.REMOTE_TO_LOCAL)
            copy_tasks = []
            for element in os.listdir(temp_dir):
                copy_tasks.append(asyncio.create_task(dst_connector.copy(
                    os.path.join(temp_dir, element), dst, dst_resources, ConnectorCopyKind.LOCAL_TO_REMOTE)))
            await asyncio.gather(*copy_tasks)
            shutil.rmtree(temp_dir)
