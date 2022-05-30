from __future__ import annotations

import asyncio
import posixpath
import shlex
import tarfile
from abc import ABC, abstractmethod
from typing import MutableSequence, TYPE_CHECKING, Tuple

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.data import LOCAL_LOCATION
from streamflow.core.deployment import Connector, ConnectorCopyKind
from streamflow.core.exception import WorkflowExecutionException
from streamflow.data import aiotarstream
from streamflow.data.aiotarstream import StreamReaderWrapper, StreamWriterWrapper
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from typing import Any, Optional, MutableMapping, Union


class BaseConnector(Connector, ABC):

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
                 context: StreamFlowContext,
                 transferBufferSize: int):
        super().__init__(deployment_name, context)
        self.transferBufferSize: int = transferBufferSize

    async def _copy_local_to_remote(self,
                                    src: str,
                                    dst: str,
                                    locations: MutableSequence[str],
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
                                           location: str,
                                           read_only: bool = False) -> None:
        proc = await self._run(
            location=location,
            command=["tar", "xf", "-", "-C", "/"],
            encode=False,
            interactive=True,
            stream=True)
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
                                    location: str,
                                    read_only: bool = False) -> None:
        proc = await self._run(
            location=location,
            command=["tar", "chf", "-", "-C", "/", posixpath.relpath(src, '/')],
            capture_output=True,
            encode=False,
            stream=True)
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
                                     locations: MutableSequence[str],
                                     source_location: str,
                                     read_only: bool = False) -> None:
        if source_location in locations:
            if src != dst:
                command = ['/bin/cp', "-rf", src, dst]
                await self.run(source_location, command)
                locations.remove(source_location)
        if locations:
            # Open source StreamReader
            dirname, basename = posixpath.split(src)
            reader = await self._run(
                location=source_location,
                command=["tar", "chf", "-", "-C", dirname, basename],
                capture_output=True,
                encode=False,
                stream=True)
            # Open a target StreamWriter for each location
            writers = await asyncio.gather(*(asyncio.create_task(self._run(
                location=location,
                command=["tar", "xf", "-", "-C", posixpath.dirname(dst)],
                encode=False,
                interactive=True,
                stream=True)) for location in locations))
            # Multiplex the reader output to all the writers
            while content := await reader.stdout.read(self.transferBufferSize):
                for writer in writers:
                    writer.stdin.write(content)
                await asyncio.gather(*(asyncio.create_task(writer.stdin.drain()) for writer in writers))
            # Wait for reader to finish
            await reader.wait()
            # Close all writers
            for wrtier in writers:
                wrtier.stdin.close()
            await asyncio.gather(*(asyncio.create_task(writer.wait()) for writer in writers))

    @abstractmethod
    def _get_run_command(self,
                         command: str,
                         location: str,
                         interactive: bool = False) -> str:
        ...

    async def _run(self,
                   location: str,
                   command: MutableSequence[str],
                   environment: MutableMapping[str, str] = None,
                   workdir: Optional[str] = None,
                   stdin: Optional[Union[int, str]] = None,
                   stdout: Union[int, str] = asyncio.subprocess.STDOUT,
                   stderr: Union[int, str] = asyncio.subprocess.STDOUT,
                   capture_output: bool = False,
                   job_name: Optional[str] = None,
                   encode: bool = True,
                   interactive: bool = False,
                   stream: bool = False) -> Union[Optional[Tuple[Optional[Any], int]], asyncio.subprocess.Process]:
        command = utils.create_command(
            command, environment, workdir, stdin, stdout, stderr)
        logger.debug("Executing command {command} on {location} {job}".format(
            command=command,
            location=location,
            job="for job {job}".format(job=job_name) if job_name else ""))
        if encode:
            command = utils.encode_command(command)
        run_command = self._get_run_command(command, location, interactive=interactive)
        proc = await asyncio.create_subprocess_exec(
            *shlex.split(run_command),
            stdin=asyncio.subprocess.PIPE if interactive else None,
            stdout=asyncio.subprocess.PIPE if capture_output else asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE if capture_output else asyncio.subprocess.DEVNULL)
        if stream:
            return proc
        elif capture_output:
            stdout, _ = await proc.communicate()
            return stdout.decode().strip(), proc.returncode
        else:
            await proc.wait()

    async def copy(self,
                   src: str,
                   dst: str,
                   locations: MutableSequence[str],
                   kind: ConnectorCopyKind,
                   source_location: Optional[str] = None,
                   read_only: bool = False) -> None:
        if kind == ConnectorCopyKind.REMOTE_TO_REMOTE:
            if source_location is None:
                raise Exception("Source location is mandatory for remote to remote copy")
            if len(locations) > 1:
                logger.info("Copying {src} {source_location} to {dst} on locations:\n\t{locations}".format(
                    source_location=("on local file-system" if source_location == LOCAL_LOCATION else
                                     "on location {res}".format(res=source_location)),
                    src=src,
                    dst=dst,
                    locations='\n\t'.join(locations)
                ))
            else:
                logger.info("Copying {src} {source_location} to {dst} {location}".format(
                    source_location=("on local file-system" if source_location == LOCAL_LOCATION else
                                     "on location {res}".format(res=source_location)),
                    src=src,
                    dst=dst,
                    location=("on local file-system" if locations[0] == LOCAL_LOCATION else
                              "on location {res}".format(res=locations[0]))
                ))
            await self._copy_remote_to_remote(
                src=src,
                dst=dst,
                locations=locations,
                source_location=source_location,
                read_only=read_only)
        elif kind == ConnectorCopyKind.LOCAL_TO_REMOTE:
            if len(locations) > 1:
                logger.info("Copying {src} on local file-system to {dst} on locations:\n\t{locations}".format(
                    src=src,
                    dst=dst,
                    locations='\n\t'.join(locations)
                ))
            else:
                logger.info("Copying {src} on local file-system to {dst} on location {location}".format(
                    src=src,
                    dst=dst,
                    location=locations[0]
                ))
            await self._copy_local_to_remote(
                src=src,
                dst=dst,
                locations=locations,
                read_only=read_only)
        elif kind == ConnectorCopyKind.REMOTE_TO_LOCAL:
            if len(locations) > 1:
                raise Exception("Copy from multiple locations is not supported")
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
                  location: str,
                  command: MutableSequence[str],
                  environment: MutableMapping[str, str] = None,
                  workdir: Optional[str] = None,
                  stdin: Optional[Union[int, str]] = None,
                  stdout: Union[int, str] = asyncio.subprocess.STDOUT,
                  stderr: Union[int, str] = asyncio.subprocess.STDOUT,
                  capture_output: bool = False,
                  job_name: Optional[str] = None) -> Optional[Tuple[Optional[Any], int]]:
        return await self._run(
            location=location,
            command=command,
            environment=environment,
            workdir=workdir,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            capture_output=capture_output,
            job_name=job_name)
