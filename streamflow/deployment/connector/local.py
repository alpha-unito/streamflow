from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import MutableMapping, MutableSequence

import psutil
from importlib_resources import files

from streamflow.core.deployment import (
    Connector,
    ExecutionLocation,
    LOCAL_LOCATION,
)
from streamflow.core.utils import local_copy
from streamflow.core.scheduling import AvailableLocation, Hardware, Storage
from streamflow.deployment.connector.base import BaseConnector
from streamflow.log_handler import logger


class LocalConnector(BaseConnector):
    def __init__(
        self, deployment_name: str, config_dir: str, transferBufferSize: int = 2**16
    ):
        super().__init__(deployment_name, config_dir, transferBufferSize)
        self.hardware: Hardware = Hardware(
            cores=float(psutil.cpu_count()),
            memory=float(psutil.virtual_memory().total / 2**20),
            storage={
                disk.mountpoint: Storage(
                    disk.mountpoint, shutil.disk_usage(disk.mountpoint).free / 2**20
                )
                for disk in psutil.disk_partitions()
            },
        )

    def _get_run_command(
        self, command: str, location: ExecutionLocation, interactive: bool = False
    ) -> MutableSequence[str]:
        if sys.platform == "win32":
            return [self._get_shell(), "/C", f"'{command}'"]
        else:
            return [self._get_shell(), "-c", f"'{command}'"]

    def _get_shell(self) -> str:
        if sys.platform == "win32":
            return "cmd"
        elif sys.platform == "darwin":
            return "bash"
        else:
            return "sh"

    async def _copy_local_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[ExecutionLocation],
        read_only: bool = False,
    ) -> None:
        local_copy(src, dst, read_only)

    async def _copy_remote_to_local(
        self, src: str, dst: str, location: ExecutionLocation, read_only: bool = False
    ) -> None:
        local_copy(src, dst, read_only)

    async def _copy_remote_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[ExecutionLocation],
        source_location: ExecutionLocation,
        source_connector: Connector | None = None,
        read_only: bool = False,
    ) -> None:
        source_connector = source_connector or self
        if source_connector == self:
            local_copy(src, dst, read_only)
        else:
            await super()._copy_remote_to_remote(
                src=src,
                dst=dst,
                locations=locations,
                source_connector=source_connector,
                source_location=source_location,
                read_only=read_only,
            )

    async def deploy(self, external: bool) -> None:
        os.makedirs(
            os.path.join(os.path.realpath(tempfile.gettempdir()), "streamflow"),
            exist_ok=True,
        )

    async def get_available_locations(
        self,
        service: str | None = None,
        directories: MutableSequence[str] | None = None,
    ) -> MutableMapping[str, AvailableLocation]:
        for directory in directories or []:
            try:
                self.hardware.get_mount_point(directory)
            except KeyError:
                path = Path(directory)
                while not path.exists():
                    path = path.parent

                # Get mount point of the path
                mount_point = Path(os.path.realpath(path))
                while not os.path.ismount(mount_point):
                    mount_point = mount_point.parent
                logger.info(self.hardware.storage.keys())
                self.hardware.storage[str(mount_point)].add_path(directory)
        return {
            LOCAL_LOCATION: AvailableLocation(
                name=LOCAL_LOCATION,
                deployment=self.deployment_name,
                service=service,
                hostname="localhost",
                slots=1,
                hardware=self.hardware,
            )
        }

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("local.json")
            .read_text("utf-8")
        )

    async def undeploy(self, external: bool) -> None:
        pass
