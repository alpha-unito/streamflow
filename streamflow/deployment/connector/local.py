import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import MutableMapping, MutableSequence

import psutil

from streamflow.core.data import LOCAL_LOCATION
from streamflow.core.scheduling import Location, Hardware
from streamflow.deployment.connector.base import BaseConnector


def _get_disk_usage(path: Path):
    while not os.path.exists(path):
        path = path.parent
    return float(getattr(shutil.disk_usage(path), 'free') / 2 ** 20)


class LocalConnector(BaseConnector):

    def __init__(self,
                 deployment_name: str,
                 streamflow_config_dir: str,
                 transferBufferSize: int = 2 ** 16):
        super().__init__(deployment_name, streamflow_config_dir, transferBufferSize)
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
                                     read_only: bool = False) -> None:
        if os.path.isdir(src):
            os.makedirs(dst, exist_ok=True)
            shutil.copytree(src, dst, dirs_exist_ok=True)
        else:
            shutil.copy(src, dst)

    async def deploy(self, external: bool) -> None:
        os.makedirs(os.path.join(tempfile.gettempdir(), 'streamflow'), exist_ok=True)

    async def get_available_locations(self,
                                      service: str,
                                      input_directory: str,
                                      output_directory: str,
                                      tmp_directory: str) -> MutableMapping[str, Location]:
        return {LOCAL_LOCATION: Location(
            name=LOCAL_LOCATION,
            hostname='localhost',
            slots=1,
            hardware=Hardware(
                cores=self.cores,
                memory=self.memory,
                input_directory=_get_disk_usage(Path(input_directory)),
                output_directory=_get_disk_usage(Path(output_directory)),
                tmp_directory=_get_disk_usage(Path(tmp_directory))))}

    async def undeploy(self, external: bool) -> None:
        pass
