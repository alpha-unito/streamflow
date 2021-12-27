import asyncio
import os
import shutil
import sys
import tempfile
from typing import MutableMapping, MutableSequence, Optional, Union, Tuple, Any

import psutil

from streamflow.core.data import LOCAL_RESOURCE
from streamflow.core.scheduling import Resource, Hardware
from streamflow.deployment.connector.base import BaseConnector


class LocalConnector(BaseConnector):

    def __init__(self,
                 streamflow_config_dir: str,
                 transferBufferSize: int = 2 ** 16):
        super().__init__(streamflow_config_dir, transferBufferSize)
        self.cores = float(psutil.cpu_count())
        self.memory = float(psutil.virtual_memory().available / 2 ** 20)

    def _get_run_command(self,
                         command: str,
                         resource: str,
                         interactive: bool = False):
        if sys.platform == 'win32':
            return "cmd /C '{command}'".format(command=command)
        else:
            return "sh -c '{command}'".format(command=command)

    async def _copy_remote_to_remote(self,
                                     src: str,
                                     dst: str,
                                     resources: MutableSequence[str],
                                     source_remote: str,
                                     read_only: bool = False) -> None:
        if os.path.isdir(src):
            os.makedirs(dst, exist_ok=True)
            shutil.copytree(src, dst, dirs_exist_ok=True)
        else:
            shutil.copy(src, dst)

    async def deploy(self, external: bool) -> None:
        os.makedirs(os.path.join(tempfile.gettempdir(), 'streamflow'), exist_ok=True)

    async def get_available_resources(self, service: str) -> MutableMapping[str, Resource]:
        if service:
            os.makedirs(service, exist_ok=True)
        return {LOCAL_RESOURCE: Resource(
            name=LOCAL_RESOURCE,
            hostname='localhost',
            slots=1,
            hardware=Hardware(
                cores=self.cores,
                memory=self.memory,
                disk=float(getattr(shutil.disk_usage(service), 'free') / 2 ** 30)))}

    async def undeploy(self, external: bool) -> None:
        pass
