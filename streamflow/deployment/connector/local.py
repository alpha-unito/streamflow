from __future__ import annotations

import asyncio
import base64
import errno
import json
import logging
import os
import shutil
import sys
import tempfile
from collections.abc import MutableMapping, MutableSequence
from importlib.resources import files

import psutil

from streamflow.core import utils
from streamflow.core.deployment import Connector, ExecutionLocation
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.scheduling import AvailableLocation, Hardware, Storage
from streamflow.deployment.connector.base import FS_TYPES_TO_SKIP, BaseConnector
from streamflow.log_handler import logger


def _local_copy(src: str, dst: str, read_only: bool) -> None:
    if read_only:
        if os.path.isdir(dst):
            dst = os.path.join(dst, os.path.basename(src))
        try:
            os.symlink(src, dst, target_is_directory=os.path.isdir(src))
        except OSError as e:
            if e.errno != errno.EEXIST:
                if sys.platform == "win32" and e.errno == errno.EINVAL:
                    if logger.isEnabledFor(logging.WARNING):
                        logger.warning(
                            f"Unable to create a symbolic link from {src} to {dst}: {e.strerror}"
                        )
                    shutil.copy(src, dst)
                else:
                    raise
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
        storage = {}
        for disk in psutil.disk_partitions(all=True):
            if disk.fstype not in FS_TYPES_TO_SKIP and os.access(
                disk.mountpoint, os.R_OK
            ):
                try:
                    storage[disk.mountpoint] = Storage(
                        mount_point=disk.mountpoint,
                        size=shutil.disk_usage(disk.mountpoint).free / 2**20,
                    )
                except (PermissionError, TimeoutError) as e:
                    logger.warning(
                        f"Skipping Storage on partition {disk.device} on {disk.mountpoint} "
                        f"for deployment {self.deployment_name}: {e}"
                    )
        self._hardware: Hardware = Hardware(
            cores=float(psutil.cpu_count()),
            memory=float(psutil.virtual_memory().total / 2**20),
            storage=storage,
        )

    def _get_shell(self) -> str:
        if sys.platform == "win32":
            return "cmd"
        elif sys.platform == "darwin":
            return "bash"
        else:
            return "sh"

    async def copy_local_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[ExecutionLocation],
        read_only: bool = False,
    ) -> None:
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"COPYING from {src} to {dst} on local file-system")
        _local_copy(src, dst, read_only)
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"COMPLETED copy from {src} to {dst} on local file-system")

    async def copy_remote_to_local(
        self, src: str, dst: str, location: ExecutionLocation, read_only: bool = False
    ) -> None:
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"COPYING from {src} to {dst} on local file-system")
        _local_copy(src, dst, read_only)
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"COMPLETED copy from {src} to {dst} on local file-system")

    async def copy_remote_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[ExecutionLocation],
        source_location: ExecutionLocation,
        source_connector: Connector | None = None,
        read_only: bool = False,
    ) -> None:
        source_connector = source_connector or self
        if source_location.local:
            if logger.isEnabledFor(logging.INFO):
                logger.info(f"COPYING from {src} to {dst} on local file-system")
            _local_copy(src, dst, read_only)
            if logger.isEnabledFor(logging.INFO):
                logger.info(f"COMPLETED copy from {src} to {dst} on local file-system")
        else:
            await source_connector.copy_remote_to_local(
                src=src,
                dst=dst,
                location=source_location,
                read_only=read_only,
            )

    async def deploy(self, external: bool) -> None:
        if sys.platform == "win32":
            # Check if PowerShell is installed
            if not shutil.which("pwsh"):
                raise WorkflowExecutionException(
                    "StreamFlow requires Windows PowerShell version 7.1 or higher"
                )
            # Check PowerShell version
            stdout, returncode = await utils.run_in_subprocess(
                location=ExecutionLocation(name="pscheck", deployment=self.deployment_name, local=True),
                command=[
                    "pwsh",
                    "-nologo",
                    "-Command",
                    "'$PSVersionTable.PSVersion | ConvertTo-Json'"
                ],
                capture_output=True,
                timeout=None,
            )
            if returncode != 0:
                raise WorkflowExecutionException(
                    f"Failed to retrieve PowerShell version: [{returncode}]: {stdout}"
                )
            result = json.loads(stdout)
            if result["Major"] < 7 or (result["Major"] == 7 and result["Minor"] < 1):
                raise WorkflowExecutionException(
                    "StreamFlow requires Windows PowerShell version 7.1 or higher"
                )
        os.makedirs(
            os.path.join(os.path.realpath(tempfile.gettempdir()), "streamflow"),
            exist_ok=True,
        )

    async def get_available_locations(
        self, service: str | None = None
    ) -> MutableMapping[str, AvailableLocation]:
        return {
            "__LOCAL__": AvailableLocation(
                name="__LOCAL__",
                deployment=self.deployment_name,
                service=service,
                hostname="localhost",
                local=True,
                slots=1,
                hardware=self._hardware,
            )
        }

    async def run(
        self,
        location: ExecutionLocation,
        command: MutableSequence[str],
        environment: MutableMapping[str, str] | None = None,
        workdir: str | None = None,
        stdin: int | str | None = None,
        stdout: int | str = asyncio.subprocess.STDOUT,
        stderr: int | str = asyncio.subprocess.STDOUT,
        capture_output: bool = False,
        timeout: int | None = None,
        job_name: str | None = None,
    ) -> tuple[str, int] | None:
        # Create command
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
        # Run command
        if sys.platform == "win32":
            command = base64.b64encode(command.encode("utf-16-le")).decode("utf-8")
            return await utils.run_in_subprocess(
                location=location,
                command=["pwsh", "-nologo", "-encodedCommand", command,
                         ],
                capture_output=capture_output,
                timeout=timeout,
            )
        else:
            return await utils.run_in_subprocess(
                location=location,
                command=["sh", "-c", f"'{utils.encode_command(command)}'",
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
