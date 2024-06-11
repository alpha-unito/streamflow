from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import PurePath
from typing import MutableMapping, MutableSequence

import psutil
from importlib_resources import files

from streamflow.core.deployment import (
    Connector,
    ExecutionLocation,
    LOCAL_LOCATION,
)
from streamflow.core.scheduling import AvailableLocation, Hardware
from streamflow.core.utils import local_copy
from streamflow.deployment.connector.base import BaseConnector


def _get_disk_usage(path: PurePath):
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
        input_directory: str | None = None,
        output_directory: str | None = None,
        tmp_directory: str | None = None,
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
                    input_directory=(
                        _get_disk_usage(PurePath(input_directory))
                        if input_directory
                        else float("inf")
                    ),
                    output_directory=(
                        _get_disk_usage(PurePath(output_directory))
                        if output_directory
                        else float("inf")
                    ),
                    tmp_directory=(
                        _get_disk_usage(PurePath(tmp_directory))
                        if tmp_directory
                        else float("inf")
                    ),
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
