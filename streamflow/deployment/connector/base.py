from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import posixpath
import shlex
import tarfile
from abc import ABC
from collections.abc import MutableMapping, MutableSequence
from types import TracebackType
from typing import AsyncContextManager

from streamflow.core import utils
from streamflow.core.data import StreamWrapper
from streamflow.core.deployment import Connector, ExecutionLocation, Shell
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.utils import get_local_to_remote_destination
from streamflow.deployment import aiotarstream
from streamflow.deployment.future import FutureAware
from streamflow.deployment.shell import BaseShell
from streamflow.deployment.stream import StreamReaderWrapper, StreamWriterWrapper
from streamflow.log_handler import logger

FS_TYPES_TO_SKIP = {
    "-",
    "bpf",
    "cgroup",
    "cgroup2",
    "configfs",
    "debugfs",
    "devpts",
    "devtmpfs",
    "fusectl",
    "hugetlbfs",
    "mqueue",
    "proc",
    "pstore",
    "securityfs",
    "selinuxfs",
    "sysfs",
    "tmpfs",
    "tracefs",
}


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
            # `member.path` is an alias of `member.name` and updates automatically.
            member.name = posixpath.relpath(member.name, posixpath.basename(src))
            if member.linkname:
                # `member.linkpath` is an alias of `member.linkname` and updates automatically.
                member.linkname = posixpath.relpath(
                    member.linkname, posixpath.basename(src)
                )
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
    if logger.isEnabledFor(logging.INFO):
        logger.info(
            f"COPYING from {src} on local file-system to {dst} on location {location}"
        )
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
                f"FAILED copy from {src} to {dst} on location {location}: {e}"
            ) from e
    if logger.isEnabledFor(logging.INFO):
        logger.info(
            f"COMPLETED copy from {src} on local file-system to {dst} on location {location}"
        )


async def copy_remote_to_local(
    connector: Connector,
    location: ExecutionLocation,
    src: str,
    dst: str,
    reader_command: MutableSequence[str],
) -> None:
    if logger.isEnabledFor(logging.INFO):
        logger.info(
            f"COPYING from {src} on location {location} to {dst} on local file-system"
        )
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
                f"FAILED copy from {src} from location {location} to {dst}: {e}"
            ) from e
    if logger.isEnabledFor(logging.INFO):
        logger.info(
            f"COMPLETED copy from {src} on location {location} to {dst} on local file-system"
        )


async def copy_remote_to_remote(
    connector: Connector,
    locations: MutableSequence[ExecutionLocation],
    src: str,
    dst: str,
    source_connector: Connector,
    source_location: ExecutionLocation,
    reader_command: MutableSequence[str] | None = None,
    writer_command: MutableSequence[str] | None = None,
) -> None:
    if logger.isEnabledFor(logging.INFO):
        if len(locations) > 1:
            logger.info(
                "COPYING from {src} on location {src_loc} to {dst} on locations:\n\t{locations}".format(
                    src_loc=source_location,
                    src=src,
                    dst=dst,
                    locations="\n\t".join([str(loc) for loc in locations]),
                )
            )
        else:
            logger.info(
                f"COPYING from {src} on location {source_location} to {dst} on location {locations[0]}"
            )
    # Build reader and writer commands
    if reader_command is None:
        reader_command = ["tar", "chf", "-", "-C", *posixpath.split(src)]
    if writer_command is None:
        writer_command = await utils.get_remote_to_remote_write_command(
            src_connector=source_connector,
            src_location=source_location,
            src=src,
            dst_connector=connector,
            dst_locations=locations,
            dst=dst,
        )
    # Open source StreamReader
    async with await source_connector.get_stream_reader(
        command=reader_command,
        location=source_location,
    ) as reader:
        # Open a target StreamWriter for each location
        write_contexts = await asyncio.gather(
            *(
                asyncio.create_task(
                    connector.get_stream_writer(
                        command=writer_command,
                        location=location,
                    )
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
    if logger.isEnabledFor(logging.INFO):
        if len(locations) > 1:
            logger.info(
                "COMPLETED copy from {src} on location {src_loc} to {dst} on locations:\n\t{locations}".format(
                    src_loc=source_location,
                    src=src,
                    dst=dst,
                    locations="\n\t".join([str(loc) for loc in locations]),
                )
            )
        else:
            logger.info(
                f"COMPLETED copy from {src} on location {source_location} "
                f"to {dst} on location {locations[0]}"
            )


async def copy_same_connector(
    connector: Connector,
    locations: MutableSequence[ExecutionLocation],
    source_location: ExecutionLocation,
    src: str,
    dst: str,
    read_only: bool,
) -> MutableSequence[ExecutionLocation]:
    if src != dst:
        for location in locations:
            if (
                location.name == source_location.name
                and location.deployment == source_location.deployment
            ):
                if logger.isEnabledFor(logging.INFO):
                    logger.info(f"COPYING from {src} to {dst} on location {location}")
                await connector.run(
                    location=location,
                    command=(["ln", "-snf"] if read_only else ["/bin/cp", "-rf"])
                    + [
                        src,
                        dst,
                    ],
                )
                if logger.isEnabledFor(logging.INFO):
                    logger.info(
                        f"COMPLETED copy from {src} to {dst} on location {location}"
                    )
                return [loc for loc in locations if loc != location]
    return locations


class SubprocessStreamWrapperContextManager(AsyncContextManager[StreamWrapper], ABC):
    def __init__(self, coro) -> None:
        self.coro = coro
        self.proc: asyncio.subprocess.Process | None = None
        self.stream: StreamWrapper | None = None


class SubprocessStreamReaderWrapperContextManager(
    SubprocessStreamWrapperContextManager
):
    async def __aenter__(self) -> StreamReaderWrapper:
        self.proc = await self.coro
        self.stream = StreamReaderWrapper(self.proc.stdout)
        return self.stream

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.proc.wait()
        if self.stream:
            await self.stream.close()


class SubprocessStreamWriterWrapperContextManager(
    SubprocessStreamWrapperContextManager
):
    async def __aenter__(self) -> StreamWriterWrapper:
        self.proc = await self.coro
        self.stream = StreamWriterWrapper(self.proc.stdin)
        return self.stream

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self.stream:
            await self.stream.close()
        await self.proc.wait()


class SubprocessShell(BaseShell):
    __slots__ = "_proc"

    def __init__(
        self,
        command: MutableSequence[str],
        buffer_size: int,
        process: asyncio.subprocess.Process,
    ) -> None:
        super().__init__(command, buffer_size)
        self._proc: asyncio.subprocess.Process = process
        self._reader = StreamReaderWrapper(self._proc.stdout)
        self._writer = StreamWriterWrapper(self._proc.stdin)

    async def _close(self) -> None:
        try:
            if self._proc.returncode is None:
                await self._writer.write(b"exit\n")
                try:
                    await asyncio.wait_for(self._proc.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    logger.warning(
                        f"Shell with command `{' '.join(self.command)}` "
                        "did not exit gracefully. Killing"
                    )
                    self._proc.kill()
                    await self._proc.wait()
        except Exception:
            with contextlib.suppress(Exception):
                self._proc.kill()
                await self._proc.wait()


class BaseConnector(Connector, FutureAware, ABC):
    def __init__(self, deployment_name: str, config_dir: str, transferBufferSize: int):
        super().__init__(
            deployment_name=deployment_name,
            config_dir=config_dir,
            transferBufferSize=transferBufferSize,
        )
        self._shells: MutableMapping[str, Shell] = {}
        self._shells_lock: asyncio.Lock = asyncio.Lock()

    async def _create_shell(
        self, command: MutableSequence[str], location: ExecutionLocation
    ) -> Shell:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        return SubprocessShell(
            command=command, buffer_size=self.transferBufferSize, process=process
        )

    async def copy_local_to_remote(
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
                    copy_local_to_remote(
                        connector=self,
                        location=location,
                        src=src,
                        dst=dst,
                        writer_command=["tar", "xf", "-", "-C", "/"],
                    )
                )
                for location in locations
            )
        )

    async def copy_remote_to_local(
        self,
        src: str,
        dst: str,
        location: ExecutionLocation,
        read_only: bool = False,
    ) -> None:
        await copy_remote_to_local(
            connector=self,
            location=location,
            src=src,
            dst=dst,
            reader_command=["tar", "chf", "-", "-C", *posixpath.split(src)],
        )

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
        if locations := await copy_same_connector(
            connector=self,
            locations=locations,
            source_location=source_location,
            src=src,
            dst=dst,
            read_only=read_only,
        ):
            # Perform remote to remote copy
            await copy_remote_to_remote(
                connector=self,
                locations=locations,
                src=src,
                dst=dst,
                source_connector=source_connector,
                source_location=source_location,
            )

    async def get_shell(
        self, command: MutableSequence[str], location: ExecutionLocation
    ) -> Shell:
        async with self._shells_lock:
            if (key := str(hash("".join(command)))) in self._shells:
                shell = self._shells[key]
                if not await shell.closed():
                    return shell
                else:
                    del self._shells[key]
            try:
                self._shells[key] = await self._create_shell(command, location)
                return self._shells[key]
            except Exception as e:
                raise WorkflowExecutionException(
                    f"Failed to create shell with command `{' '.join(command)}`: {e}"
                ) from e

    async def get_stream_reader(
        self,
        command: MutableSequence[str],
        location: ExecutionLocation,
    ) -> AsyncContextManager[StreamWrapper]:
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
    ) -> AsyncContextManager[StreamWrapper]:
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
        environment: MutableMapping[str, str] | None = None,
        workdir: str | None = None,
        stdin: int | str | None = None,
        stdout: int | str = asyncio.subprocess.STDOUT,
        stderr: int | str = asyncio.subprocess.STDOUT,
        capture_output: bool = False,
        timeout: int | None = None,
        job_name: str | None = None,
    ) -> tuple[str, int] | None:
        if job_name is None and stdin is None:
            with contextlib.suppress(WorkflowExecutionException):
                return await utils.run_in_shell(
                    shell=await self.get_shell(
                        command=["sh"], location=location
                    ),  # nosec
                    location=location,
                    command=command,
                    environment=environment,
                    workdir=workdir,
                    capture_output=capture_output,
                    timeout=timeout,
                )
        command_str = utils.create_command(
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
                    command=command_str,
                    location=location,
                    job=f"for job {job_name}" if job_name else "",
                )
            )
        return await utils.run_in_subprocess(
            location=location,
            command=[command_str],
            capture_output=capture_output,
            timeout=timeout,
        )

    async def undeploy(self, external: bool) -> None:
        async with self._shells_lock:
            await asyncio.gather(
                *(asyncio.create_task(shell.close()) for shell in self._shells.values())
            )
            self._shells.clear()


class BatchConnector(Connector, ABC):
    pass
