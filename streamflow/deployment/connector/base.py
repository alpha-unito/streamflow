from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import posixpath
import shlex
import tarfile
from abc import ABC
from collections.abc import MutableSequence, MutableMapping
from typing import Any

from streamflow.core import utils
from streamflow.core.data import StreamWrapperContextManager
from streamflow.core.deployment import (
    Connector,
    ExecutionLocation,
    LOCAL_LOCATION,
)
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.utils import get_local_to_remote_destination
from streamflow.deployment import aiotarstream
from streamflow.deployment.future import FutureAware
from streamflow.deployment.stream import (
    SubprocessStreamReaderWrapperContextManager,
    SubprocessStreamWriterWrapperContextManager,
)
from streamflow.log_handler import logger


async def extract_tar_stream(
    tar: aiotarstream.AioTarStream,
    src: str,
    dst: str,
    transferBufferSize: int | None = None,
) -> None:
    async for member in tar:
        # If `dst` is a directory, copy the content of `src` inside `dst`
        if os.path.isdir(dst) and member.path == posixpath.basename(src):
            await tar.extract(member, dst)

        # Otherwise, if copying a file, simply move it inside `dst`
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

        # Otherwise, if copying a directory, modify the member path to
        # move all the file hierarchy inside `dst`
        else:
            member.path = posixpath.relpath(member.path, posixpath.basename(src))
            await tar.extract(
                member, os.path.normpath(os.path.join(dst, os.path.curdir))
            )


async def copy_local_to_remote(
    connector: Connector,
    location: ExecutionLocation,
    src: str,
    dst: str,
    writer_command: MutableSequence[str],
) -> None:
    async with await connector.get_stream_writer(
        command=writer_command, location=location
    ) as writer:
        try:
            async with aiotarstream.open(
                stream=writer,
                format=tarfile.GNU_FORMAT,
                mode="w",
                dereference=True,
                copybufsize=connector.transferBufferSize,
            ) as tar:
                await tar.add(src, arcname=dst)
        except tarfile.TarError as e:
            raise WorkflowExecutionException(
                f"Error copying {src} to {dst} on location {location}: {e}"
            ) from e


async def copy_remote_to_local(
    connector: Connector,
    location: ExecutionLocation,
    src: str,
    dst: str,
    reader_command: MutableSequence[str],
) -> None:
    async with await connector.get_stream_reader(
        command=reader_command, location=location
    ) as reader:
        try:
            async with aiotarstream.open(
                stream=reader,
                mode="r",
                copybufsize=connector.transferBufferSize,
            ) as tar:
                await extract_tar_stream(tar, src, dst, connector.transferBufferSize)
        except tarfile.TarError as e:
            raise WorkflowExecutionException(
                f"Error copying {src} from location {location} to {dst}: {e}"
            ) from e


async def copy_remote_to_remote(
    connector: Connector,
    locations: MutableSequence[ExecutionLocation],
    source_connector: Connector,
    source_location: ExecutionLocation,
    reader_command: MutableSequence[str],
    writer_command: MutableSequence[str],
) -> None:
    # Open source StreamReader
    async with await source_connector.get_stream_reader(
        command=reader_command,
        location=source_location,
    ) as reader:
        # Open a target StreamWriter for each location
        write_contexts = await asyncio.gather(
            *(
                asyncio.create_task(
                    connector.get_stream_writer(writer_command, location)
                )
                for location in locations
            )
        )
        async with contextlib.AsyncExitStack() as exit_stack:
            writers = await asyncio.gather(
                *(
                    asyncio.create_task(exit_stack.enter_async_context(context))
                    for context in write_contexts
                )
            )
            # Multiplex the reader output to all the writers
            while content := await reader.read(source_connector.transferBufferSize):
                await asyncio.gather(
                    *(asyncio.create_task(writer.write(content)) for writer in writers)
                )


class BaseConnector(Connector, FutureAware, ABC):
    def __init__(self, deployment_name: str, config_dir: str, transferBufferSize: int):
        super().__init__(
            deployment_name=deployment_name,
            config_dir=config_dir,
            transferBufferSize=transferBufferSize,
        )

    async def _copy_local_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[ExecutionLocation],
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
        self, src: str, dst: str, location: ExecutionLocation, read_only: bool = False
    ) -> None:
        await copy_local_to_remote(
            connector=self,
            location=location,
            src=src,
            dst=dst,
            writer_command=["tar", "xf", "-", "-C", "/"],
        )

    async def _copy_remote_to_local(
        self, src: str, dst: str, location: ExecutionLocation, read_only: bool = False
    ) -> None:
        await copy_remote_to_local(
            connector=self,
            location=location,
            src=src,
            dst=dst,
            reader_command=["tar", "chf", "-", "-C", *posixpath.split(src)],
        )

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
        if source_connector == self and source_location.name in (
            loc.name for loc in locations
        ):
            if src != dst:
                command = ["/bin/cp", "-rf", src, dst]
                await self.run(source_location, command)
                locations.remove(source_location)
        if locations:
            # Perform remote to remote copy
            await copy_remote_to_remote(
                connector=self,
                locations=locations,
                source_connector=source_connector,
                source_location=source_location,
                reader_command=["tar", "chf", "-", "-C", *posixpath.split(src)],
                writer_command=await utils.get_remote_to_remote_write_command(
                    src_connector=source_connector,
                    src_location=source_location,
                    src=src,
                    dst_connector=self,
                    dst_locations=locations,
                    dst=dst,
                ),
            )

    async def copy_local_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[ExecutionLocation],
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
        if logger.isEnabledFor(logging.INFO):
            if len(locations) > 1:
                logger.info(
                    "COMPLETED copy {src} on local file-system to {dst} on locations:\n\t{locations}".format(
                        src=src,
                        dst=dst,
                        locations="\n\t".join([str(loc) for loc in locations]),
                    )
                )
            else:
                logger.info(
                    "COMPLETED copy {src} on local file-system to {dst} {location}".format(
                        src=src,
                        dst=dst,
                        location=(
                            "on local file-system"
                            if locations[0].name == LOCAL_LOCATION
                            else f"on location {locations[0]}"
                        ),
                    )
                )

    async def copy_remote_to_local(
        self,
        src: str,
        dst: str,
        location: ExecutionLocation,
        read_only: bool = False,
    ) -> None:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"COPYING {src} on location {location} to {dst} on local file-system"
            )
        await self._copy_remote_to_local(
            src=src, dst=dst, location=location, read_only=read_only
        )
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"COMPLETED copy {src} on location {location} to {dst} on local file-system"
            )

    async def copy_remote_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[ExecutionLocation],
        source_location: ExecutionLocation,
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
        if logger.isEnabledFor(logging.INFO):
            if len(locations) > 1:
                logger.info(
                    "COMPLETED copy {src} on location {src_loc} to {dst} on locations:\n\t{locations}".format(
                        src_loc=source_location,
                        src=src,
                        dst=dst,
                        locations="\n\t".join([str(loc) for loc in locations]),
                    )
                )
            else:
                logger.info(
                    "COMPLETED copy {src} on location {src_loc} to {dst} on location {location}".format(
                        src_loc=source_location,
                        src=src,
                        dst=dst,
                        location=locations[0],
                    )
                )

    async def get_stream_reader(
        self,
        command: MutableSequence[str],
        location: ExecutionLocation,
    ) -> StreamWrapperContextManager:
        return SubprocessStreamReaderWrapperContextManager(
            coro=asyncio.create_subprocess_exec(
                *shlex.split(" ".join(command)),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        )

    async def get_stream_writer(
        self, command: MutableSequence[str], location: ExecutionLocation
    ) -> StreamWrapperContextManager:
        return SubprocessStreamWriterWrapperContextManager(
            coro=asyncio.create_subprocess_exec(
                *shlex.split(" ".join(command)),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
        )

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
        return await utils.run_in_subprocess(
            location=location,
            command=[utils.encode_command(command, "sh")],
            capture_output=capture_output,
            timeout=timeout,
        )


class BatchConnector(Connector, ABC):
    pass
