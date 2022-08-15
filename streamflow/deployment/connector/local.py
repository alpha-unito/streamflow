import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import MutableMapping, MutableSequence, Optional

import pkg_resources
import psutil

from streamflow.core.context import StreamFlowContext
from streamflow.core.data import LOCAL_LOCATION
from streamflow.core.deployment import Connector
from streamflow.core.scheduling import Hardware, Location
from streamflow.deployment.connector.base import BaseConnector


def _get_disk_usage(path: Path):
    while not os.path.exists(path):
        path = path.parent
    return float(getattr(shutil.disk_usage(path), 'free') / 2 ** 20)


class LocalConnector(BaseConnector):

    def __init__(self,
                 deployment_name: str,
                 context: StreamFlowContext,
                 transferBufferSize: int = 2 ** 16):
        super().__init__(deployment_name, context, transferBufferSize)
        self.cores = float(psutil.cpu_count())
        self.memory = float(psutil.virtual_memory().available / 2 ** 20)

    def _get_run_command(self,
                         command: str,
                         location: str,
                         interactive: bool = False):
        if sys.platform == 'win32':
            return "cmd /C '{command}'".format(command=command)
        else:
            return "sh -c '{command}'".format(command=command)

    async def _copy_remote_to_remote(self,
                                     src: str,
                                     dst: str,
                                     locations: MutableSequence[str],
                                     source_location: str,
                                     source_connector: Optional[Connector] = None,
                                     read_only: bool = False) -> None:
        source_connector = source_connector or self
        if source_connector == self:
            if os.path.isdir(src):
                os.makedirs(dst, exist_ok=True)
                shutil.copytree(src, dst, dirs_exist_ok=True)
            else:
                shutil.copy(src, dst)
        else:
            await super()._copy_remote_to_remote(
                src=src,
                dst=dst,
                locations=locations,
                source_connector=source_connector,
                source_location=source_location,
                read_only=read_only)

    async def deploy(self, external: bool) -> None:
        os.makedirs(os.path.join(tempfile.gettempdir(), 'streamflow'), exist_ok=True)

    async def get_available_locations(self,
                                      service: str,
                                      input_directory: Optional[str] = None,
                                      output_directory: Optional[str] = None,
                                      tmp_directory: Optional[str] = None) -> MutableMapping[str, Location]:
        return {LOCAL_LOCATION: Location(
            name=LOCAL_LOCATION,
            hostname='localhost',
            slots=1,
            hardware=Hardware(
                cores=self.cores,
                memory=self.memory,
                input_directory=_get_disk_usage(Path(input_directory)) if input_directory else float('inf'),
                output_directory=_get_disk_usage(Path(output_directory)) if output_directory else float('inf'),
                tmp_directory=_get_disk_usage(Path(tmp_directory)) if tmp_directory else float('inf')))}

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join('schemas', 'local.json'))

    async def undeploy(self, external: bool) -> None:
        pass
