from __future__ import annotations

import asyncio
import logging
import os
import posixpath
import shlex
import tarfile
from abc import abstractmethod
from typing import MutableSequence, TYPE_CHECKING

from streamflow.core import utils
from streamflow.core.data import StreamWrapperContext
from streamflow.core.deployment import (
    Connector,
    LOCAL_LOCATION,
    Location,
)
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.utils import get_local_to_remote_destination
from streamflow.deployment import aiotarstream
from streamflow.deployment.future import FutureAware
from streamflow.deployment.stream import (
    StreamReaderWrapper,
    StreamWriterWrapper,
    SubprocessStreamReaderWrapperContext,
)
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from typing import Any, MutableMapping


async def extract_tar_stream(
    tar: aiotarstream.AioTarStream,
    src: str,
    dst: str,
    transferBufferSize: int | None = None,
) -> None:
    async for member in tar:
        if os.path.isdir(dst) and member.path == posixpath.basename(src):
            await tar.extract(member, dst)
        elif member.isfile():
            async with await tar.extractfile(member) as inputfile:
                path = os.path.normpath(
                    os.path.join(
                        dst, posixpath.relpath(member.path, posixpath.basename(src))
                    )
                )
                with open(path, "wb") as outputfile:
                    while content := await inputfile.read(transferBufferSize):
                        outputfile.write(content)
        else:
            member.path = posixpath.relpath(member.path, posixpath.basename(src))
            await tar.extract(member, os.path.normpath(os.path.join(dst, member.path)))


class BaseConnector(Connector, FutureAware):
    def __init__(self, deployment_name: str, config_dir: str, transferBufferSize: int):
        super().__init__(deployment_name, config_dir)
        self.transferBufferSize: int = transferBufferSize

    async def _copy_local_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[Location],
        read_only: bool = False,
    ) -> None:
        dst = await get_local_to_remote_destination(self, locations[0], src, dst)
        await asyncio.gather(
            *(
                asyncio.create_task(
                    self._copy_local_to_remote_single(
                        src=src, dst=dst, location=location, read_only=read_only
                    )
                )
                for location in locations
            )
        )

    async def _copy_local_to_remote_single(
        self, src: str, dst: str, location: Location, read_only: bool = False
    ) -> None:
        proc = await asyncio.create_subprocess_exec(
            *shlex.split(
                self._get_run_command(
                    command="tar xf - -C /", location=location, interactive=True
                )
            ),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            async with aiotarstream.open(
                stream=StreamWriterWrapper(proc.stdin),
                format=tarfile.GNU_FORMAT,
                mode="w",
                dereference=True,
                copybufsize=self.transferBufferSize,
            ) as tar:
                await tar.add(src, arcname=dst)
        except tarfile.TarError as e:
            raise WorkflowExecutionException(
                f"Error copying {src} to {dst} on location {location}: {e}"
            ) from e
        finally:
            proc.stdin.close()
            await proc.wait()

    async def _copy_remote_to_local(
        self, src: str, dst: str, location: Location, read_only: bool = False
    ) -> None:
        dirname, basename = posixpath.split(src)
        proc = await asyncio.create_subprocess_exec(
            *shlex.split(
                self._get_run_command(
                    command=f"tar chf - -C {dirname} {basename}",
                    location=location,
                )
            ),
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            async with aiotarstream.open(
                stream=StreamReaderWrapper(proc.stdout),
                mode="r",
                copybufsize=self.transferBufferSize,
            ) as tar:
                await extract_tar_stream(tar, src, dst, self.transferBufferSize)
        except tarfile.TarError as e:
            raise WorkflowExecutionException(
                f"Error copying {src} from location {location} to {dst}: {e}"
            ) from e
        finally:
            await proc.wait()

    async def _copy_remote_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[Location],
        source_location: Location,
        source_connector: str | None = None,
        read_only: bool = False,
    ) -> None:
        source_connector = source_connector or self
        if source_connector == self and source_location in locations:
            if src != dst:
                command = ["/bin/cp", "-rf", src, dst]
                await self.run(source_location, command)
                locations.remove(source_location)
        if locations:
            # Get write command
            write_command = " ".join(
                await utils.get_remote_to_remote_write_command(
                    src_connector=source_connector,
                    src_location=source_location,
                    src=src,
                    dst_connector=self,
                    dst_locations=locations,
                    dst=dst,
                )
            )
            # Open source StreamReader
            async with source_connector._get_stream_reader(
                source_location, src
            ) as reader:
                # Open a target StreamWriter for each location
                writers = await asyncio.gather(
                    *(
                        asyncio.create_task(
                            asyncio.create_subprocess_exec(
                                *shlex.split(
                                    self._get_run_command(
                                        command=write_command,
                                        location=location,
                                        interactive=True,
                                    )
                                ),
                                stdin=asyncio.subprocess.PIPE,
                                stdout=asyncio.subprocess.DEVNULL,
                                stderr=asyncio.subprocess.DEVNULL,
                            )
                        )
                        for location in locations
                    )
                )
                try:
                    # Multiplex the reader output to all the writers
                    while content := await reader.read(
                        source_connector.transferBufferSize
                    ):
                        for writer in writers:
                            writer.stdin.write(content)
                        await asyncio.gather(
                            *(
                                asyncio.create_task(writer.stdin.drain())
                                for writer in writers
                            )
                        )
                finally:
                    # Close all writers
                    for writer in writers:
                        writer.stdin.close()
                    await asyncio.gather(
                        *(asyncio.create_task(writer.wait()) for writer in writers)
                    )

    @abstractmethod
    def _get_run_command(
        self, command: str, location: Location, interactive: bool = False
    ) -> str:
        ...

    def _get_shell(self) -> str:
        return "sh"

    def _get_stream_reader(self, location: Location, src: str) -> StreamWrapperContext:
        dirname, basename = posixpath.split(src)
        return SubprocessStreamReaderWrapperContext(
            coro=asyncio.create_subprocess_exec(
                *shlex.split(
                    self._get_run_command(
                        command=f"tar chf - -C {dirname} {basename}",
                        location=location,
                    )
                ),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        )

    async def copy_local_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[Location],
        read_only: bool = False,
    ) -> None:
        if logger.isEnabledFor(logging.INFO):
            if len(locations) > 1:
                logger.info(
                    "COPYING {src} on local file-system to {dst} on locations:\n\t{locations}".format(
                        src=src,
                        dst=dst,
                        locations="\n\t".join([str(loc) for loc in locations]),
                    )
                )
            else:
                logger.info(
                    "COPYING {src} on local file-system to {dst} {location}".format(
                        src=src,
                        dst=dst,
                        location=(
                            "on local file-system"
                            if locations[0].name == LOCAL_LOCATION
                            else f"on location {locations[0]}"
                        ),
                    )
                )
        await self._copy_local_to_remote(
            src=src, dst=dst, locations=locations, read_only=read_only
        )

    async def copy_remote_to_local(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[Location],
        read_only: bool = False,
    ) -> None:
        if len(locations) > 1:
            raise Exception("Copy from multiple locations is not supported")
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"COPYING {src} on location {locations[0]} to {dst} on local file-system"
            )
        await self._copy_remote_to_local(
            src=src, dst=dst, location=locations[0], read_only=read_only
        )

    async def copy_remote_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[Location],
        source_location: Location,
        source_connector: Connector | None = None,
        read_only: bool = False,
    ):
        if logger.isEnabledFor(logging.INFO):
            if len(locations) > 1:
                logger.info(
                    "COPYING {src} on location {src_loc} to {dst} on locations:\n\t{locations}".format(
                        src_loc=source_location,
                        src=src,
                        dst=dst,
                        locations="\n\t".join([str(loc) for loc in locations]),
                    )
                )
            else:
                logger.info(
                    "COPYING {src} on location {src_loc} to {dst} on location {location}".format(
                        src_loc=source_location,
                        src=src,
                        dst=dst,
                        location=locations[0],
                    )
                )
        await self._copy_remote_to_remote(
            src=src,
            dst=dst,
            locations=locations,
            source_location=source_location,
            source_connector=source_connector,
            read_only=read_only,
        )

    async def run(
        self,
        location: Location,
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
        run_command = self._get_run_command(command, location)
        proc = await asyncio.create_subprocess_exec(
            *shlex.split(run_command),
            stdin=None,
            stdout=(
                asyncio.subprocess.PIPE
                if capture_output
                else asyncio.subprocess.DEVNULL
            ),
            stderr=(
                asyncio.subprocess.PIPE
                if capture_output
                else asyncio.subprocess.DEVNULL
            ),
        )
        if capture_output:
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            return stdout.decode().strip(), proc.returncode
        else:
            await asyncio.wait_for(proc.wait(), timeout=timeout)
            return None
