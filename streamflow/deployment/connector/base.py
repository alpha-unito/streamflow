from __future__ import annotations

import asyncio
import logging
import posixpath
import shlex
import tarfile
from abc import ABCMeta, abstractmethod
from typing import MutableSequence, TYPE_CHECKING, Tuple

from streamflow.core import utils
from streamflow.core.data import StreamWrapperContext
from streamflow.core.deployment import Connector, ConnectorCopyKind, LOCAL_LOCATION, Location
from streamflow.core.exception import WorkflowExecutionException
from streamflow.data import aiotarstream
from streamflow.data.stream import StreamReaderWrapper, StreamWriterWrapper, SubprocessStreamReaderWrapperContext
from streamflow.deployment.future import FutureConnector
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from typing import Any, Optional, MutableMapping, Union


class FutureMeta(ABCMeta):

    def __instancecheck__(self, instance):
        if isinstance(instance, FutureConnector):
            return super().__subclasscheck__(instance.type)
        else:
            return super().__instancecheck__(instance)


class FutureAware(metaclass=FutureMeta):
    __slots__ = ()


class BaseConnector(Connector, FutureAware):

    @staticmethod
    def get_option(name: str,
                   value: Any,
                   ) -> str:
        if len(name) > 1:
            name = "-{name} ".format(name=name)
        if isinstance(value, bool):
            return "-{name} ".format(name=name) if value else ""
        elif isinstance(value, str):
            return "-{name} \"{value}\" ".format(name=name, value=value)
        elif isinstance(value, MutableSequence):
            return "".join(["-{name} \"{value}\" ".format(name=name, value=item) for item in value])
        elif value is None:
            return ""
        else:
            raise TypeError("Unsupported value type")

    def __init__(self,
                 deployment_name: str,
                 config_dir: str,
                 transferBufferSize: int):
        super().__init__(deployment_name, config_dir)
        self.transferBufferSize: int = transferBufferSize
        self.is_deployed: bool = False

    async def _copy_local_to_remote(self,
                                    src: str,
                                    dst: str,
                                    locations: MutableSequence[Location],
                                    read_only: bool = False) -> None:
        await asyncio.gather(*(asyncio.create_task(
            self._copy_local_to_remote_single(
                src=src,
                dst=dst,
                location=location,
                read_only=read_only)
        ) for location in locations))

    async def _copy_local_to_remote_single(self,
                                           src: str,
                                           dst: str,
                                           location: Location,
                                           read_only: bool = False) -> None:
        proc = await asyncio.create_subprocess_exec(
            *shlex.split(self._get_run_command(
                command="tar xf - -C /",
                location=location,
                interactive=True)),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL)
        try:
            async with aiotarstream.open(
                    stream=StreamWriterWrapper(proc.stdin),
                    format=tarfile.GNU_FORMAT,
                    mode='w',
                    dereference=True,
                    copybufsize=self.transferBufferSize) as tar:
                await tar.add(src, arcname=dst)
        except tarfile.TarError as e:
            raise WorkflowExecutionException("Error copying {} to {} on location {}: {}".format(
                src, dst, location, str(e))) from e
        finally:
            proc.stdin.close()
            await proc.wait()

    async def _copy_remote_to_local(self,
                                    src: str,
                                    dst: str,
                                    location: Location,
                                    read_only: bool = False) -> None:
        proc = await asyncio.create_subprocess_exec(
            *shlex.split(self._get_run_command(
                command="tar chf - -C / " + posixpath.relpath(src, '/'),
                location=location)),
            stdin=None,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        try:
            async with aiotarstream.open(
                    stream=StreamReaderWrapper(proc.stdout),
                    mode='r',
                    copybufsize=self.transferBufferSize) as tar:
                await utils.extract_tar_stream(tar, src, dst, self.transferBufferSize)
        except tarfile.TarError as e:
            raise WorkflowExecutionException("Error copying {} from location {} to {}: {}".format(
                src, location, dst, str(e))) from e
        finally:
            await proc.wait()

    async def _copy_remote_to_remote(self,
                                     src: str,
                                     dst: str,
                                     locations: MutableSequence[Location],
                                     source_location: Location,
                                     source_connector: Optional[str] = None,
                                     read_only: bool = False) -> None:
        source_connector = source_connector or self
        if source_connector == self and source_location in locations:
            if src != dst:
                command = ['/bin/cp', "-rf", src, dst]
                await self.run(source_location, command)
                locations.remove(source_location)
        if locations:
            # Get write command
            write_command = " ".join(await utils.get_remote_to_remote_write_command(
                src_connector=source_connector,
                src_location=source_location,
                src=src,
                dst_connector=self,
                dst_locations=locations,
                dst=dst))
            # Open source StreamReader
            async with source_connector._get_stream_reader(source_location, src) as reader:
                # Open a target StreamWriter for each location
                writers = await asyncio.gather(*(asyncio.create_task(asyncio.create_subprocess_exec(
                    *shlex.split(self._get_run_command(
                        command=write_command,
                        location=location,
                        interactive=True)),
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL)) for location in locations))
                try:
                    # Multiplex the reader output to all the writers
                    while content := await reader.read(source_connector.transferBufferSize):
                        for writer in writers:
                            writer.stdin.write(content)
                        await asyncio.gather(*(asyncio.create_task(writer.stdin.drain()) for writer in writers))
                finally:
                    # Close all writers
                    for writer in writers:
                        writer.stdin.close()
                    await asyncio.gather(*(asyncio.create_task(writer.wait()) for writer in writers))

    @abstractmethod
    def _get_run_command(self,
                         command: str,
                         location: Location,
                         interactive: bool = False) -> str:
        ...

    def _get_stream_reader(self,
                           location: Location,
                           src: str) -> StreamWrapperContext:
        dirname, basename = posixpath.split(src)
        return SubprocessStreamReaderWrapperContext(
            coro=asyncio.create_subprocess_exec(
                *shlex.split(self._get_run_command(
                    command="tar chf - -C {} {}".format(dirname, basename),
                    location=location)),
                stdin=None,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE))

    async def copy(self,
                   src: str,
                   dst: str,
                   locations: MutableSequence[Location],
                   kind: ConnectorCopyKind,
                   source_connector: Optional[Connector] = None,
                   source_location: Optional[Location] = None,
                   read_only: bool = False) -> None:
        if kind == ConnectorCopyKind.REMOTE_TO_REMOTE:
            if source_location is None:
                raise Exception("Source location is mandatory for remote to remote copy")
            if logger.isEnabledFor(logging.INFO):
                if len(locations) > 1:
                    logger.info("Copying {src} on location {src_loc} to {dst} on locations:\n\t{locations}".format(
                        src_loc=source_location,
                        src=src,
                        dst=dst,
                        locations='\n\t'.join([str(loc) for loc in locations])
                    ))
                else:
                    logger.info("Copying {src} on location {src_loc} to {dst} on location {location}".format(
                        src_loc=source_location,
                        src=src,
                        dst=dst,
                        location=locations[0]))
            await self._copy_remote_to_remote(
                src=src,
                dst=dst,
                locations=locations,
                source_connector=source_connector,
                source_location=source_location,
                read_only=read_only)
        elif kind == ConnectorCopyKind.LOCAL_TO_REMOTE:
            if logger.isEnabledFor(logging.INFO):
                if len(locations) > 1:
                    logger.info("Copying {src} on local file-system to {dst} on locations:\n\t{locations}".format(
                        src=src,
                        dst=dst,
                        locations='\n\t'.join([str(loc) for loc in locations])
                    ))
                else:
                    logger.info("Copying {src} on local file-system to {dst} {location}".format(
                        src=src,
                        dst=dst,
                        location=("on local file-system" if locations[0].name == LOCAL_LOCATION
                                  else "on location {}".format(locations[0]))))
            await self._copy_local_to_remote(
                src=src,
                dst=dst,
                locations=locations,
                read_only=read_only)
        elif kind == ConnectorCopyKind.REMOTE_TO_LOCAL:
            if len(locations) > 1:
                raise Exception("Copy from multiple locations is not supported")
            if logger.isEnabledFor(logging.INFO):
                logger.info("Copying {src} on location {location} to {dst} on local file-system".format(
                    src=src,
                    dst=dst,
                    location=locations[0]
                ))
            await self._copy_remote_to_local(
                src=src,
                dst=dst,
                location=locations[0],
                read_only=read_only)
        else:
            raise NotImplementedError

    async def run(self,
                  location: Location,
                  command: MutableSequence[str],
                  environment: MutableMapping[str, str] = None,
                  workdir: Optional[str] = None,
                  stdin: Optional[Union[int, str]] = None,
                  stdout: Union[int, str] = asyncio.subprocess.STDOUT,
                  stderr: Union[int, str] = asyncio.subprocess.STDOUT,
                  capture_output: bool = False,
                  timeout: Optional[int] = None,
                  job_name: Optional[str] = None) -> Optional[Tuple[Optional[Any], int]]:
        command = utils.create_command(
            command, environment, workdir, stdin, stdout, stderr)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Executing command {command} on {location} {job}".format(
                command=command,
                location=location,
                job="for job {job}".format(job=job_name) if job_name else ""))
        command = utils.encode_command(command)
        run_command = self._get_run_command(command, location)
        proc = await asyncio.create_subprocess_exec(
            *shlex.split(run_command),
            stdin=None,
            stdout=asyncio.subprocess.PIPE if capture_output else asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE if capture_output else asyncio.subprocess.DEVNULL)
        if capture_output:
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            return stdout.decode().strip(), proc.returncode
        else:
            await asyncio.wait_for(proc.wait(), timeout=timeout)
