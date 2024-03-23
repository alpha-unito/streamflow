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
from streamflow.core.scheduling import AvailableLocation, Hardware
from streamflow.deployment.connector.base import BaseConnector


def _get_disk_usage(path: Path):
    while not os.path.exists(path):
        path = path.parent
    return float(shutil.disk_usage(path).free / 2**20)


class LocalConnector(BaseConnector):
    def __init__(
        self, deployment_name: str, config_dir: str, transferBufferSize: int = 2**16
    ):
        super().__init__(deployment_name, config_dir, transferBufferSize)
        self.cores = float(psutil.cpu_count())
        self.memory = float(psutil.virtual_memory().available / 2**20)

    def _get_run_command(
        self, command: str, location: ExecutionLocation, interactive: bool = False
    ):
        if sys.platform == "win32":
            return f"{self._get_shell()} /C '{command}'"
        else:
            return f"{self._get_shell()} -c '{command}'"

    def _get_shell(self) -> str:
        if sys.platform == "win32":
            return "cmd"
        elif sys.platform == "darwin":
            return "bash"
        else:
            return "sh"

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
        return {
            LOCAL_LOCATION: AvailableLocation(
                name=LOCAL_LOCATION,
                deployment=self.deployment_name,
                service=service,
                hostname="localhost",
                slots=1,
                hardware=Hardware(
                    cores=self.cores,
                    memory=self.memory,
                    storage={path: _get_disk_usage(Path(path)) for path in directories},
                ),
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
