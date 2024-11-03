from __future__ import annotations

import asyncio
import logging
import os
import shutil
import sys
import tempfile
from typing import Any, MutableMapping, MutableSequence

import psutil
from importlib_resources import files

from streamflow.core import utils
from streamflow.core.deployment import (
    Connector,
    ExecutionLocation,
    LOCAL_LOCATION,
)
from streamflow.core.scheduling import AvailableLocation, Hardware, Storage
from streamflow.deployment.connector.base import BaseConnector
from streamflow.log_handler import logger


def _local_copy(src: str, dst: str, read_only: bool):
    if read_only:
        if os.path.isdir(dst):
            dst = os.path.join(dst, os.path.basename(src))
        os.symlink(src, dst, target_is_directory=os.path.isdir(src))
    else:
        if os.path.isdir(src):
            os.makedirs(dst, exist_ok=True)
            shutil.copytree(src, dst, dirs_exist_ok=True)
        else:
            shutil.copy(src, dst)


class LocalConnector(BaseConnector):
    def __init__(
        self, deployment_name: str, config_dir: str, transferBufferSize: int = 2**16
    ):
        super().__init__(deployment_name, config_dir, transferBufferSize)
        self._hardware: Hardware = Hardware(
            cores=float(psutil.cpu_count()),
            memory=float(psutil.virtual_memory().total / 2**20),
            storage={
                disk.mountpoint: Storage(
                    disk.mountpoint, shutil.disk_usage(disk.mountpoint).free / 2**20
                )
                for disk in psutil.disk_partitions()
                if os.access(disk.mountpoint, os.R_OK)
            },
        )

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
        _local_copy(src, dst, read_only)

    async def _copy_remote_to_local(
        self, src: str, dst: str, location: ExecutionLocation, read_only: bool = False
    ) -> None:
        _local_copy(src, dst, read_only)

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
        if isinstance(source_connector, LocalConnector):
            _local_copy(src, dst, read_only)
        else:
            await source_connector.copy_remote_to_local(
                src=src,
                dst=dst,
                location=source_location,
                read_only=read_only,
            )

    async def deploy(self, external: bool) -> None:
        os.makedirs(
            os.path.join(os.path.realpath(tempfile.gettempdir()), "streamflow"),
            exist_ok=True,
        )

    async def get_available_locations(
        self, service: str | None = None
    ) -> MutableMapping[str, AvailableLocation]:
        return {
            LOCAL_LOCATION: AvailableLocation(
                name=LOCAL_LOCATION,
                deployment=self.deployment_name,
                service=service,
                hostname="localhost",
                slots=1,
                hardware=self._hardware,
            )
        }

    async def run(
        self,
        location: ExecutionLocation,
        command: MutableSequence[str],
        environment: MutableMapping[str, str] = None,
        workdir: str | None = None,
        stdin: int | str | None = None,
        stdout: int | str = asyncio.subprocess.STDOUT,
        stderr: int | str = asyncio.subprocess.STDOUT,
        capture_output: bool = False,
        timeout: int | None = None,
        job_name: str | None = None,
    ) -> tuple[Any | None, int] | None:
        command = utils.create_command(
            self.__class__.__name__,
            command,
            environment,
            workdir,
            stdin,
            stdout,
            stderr,
        )
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "EXECUTING command {command} on {location} {job}".format(
                    command=command,
                    location=location,
                    job=f"for job {job_name}" if job_name else "",
                )
            )
        command = utils.encode_command(command, self._get_shell())
        return await utils.run_in_subprocess(
            location=location,
            command=[
                self._get_shell(),
                "/C" if sys.platform == "win32" else "-c",
                f"'{command}'",
            ],
            capture_output=capture_output,
            timeout=timeout,
        )

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
