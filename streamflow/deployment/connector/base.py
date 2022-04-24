from __future__ import annotations

import asyncio
import io
import os
import posixpath
import shlex
import shutil
import tarfile
import tempfile
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, MutableSequence, Tuple, cast

from streamflow.core import utils
from streamflow.core.data import LOCAL_LOCATION
from streamflow.core.deployment import Connector, ConnectorCopyKind
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
                 streamflow_config_dir: str,
                 transferBufferSize: int):
        super().__init__(deployment_name, streamflow_config_dir)
        self.transferBufferSize: int = transferBufferSize

    async def _copy_local_to_remote(self,
                                    src: str,
                                    dst: str,
                                    locations: MutableSequence[str],
                                    read_only: bool = False) -> None:
        with tempfile.TemporaryFile() as tar_buffer:
            with tarfile.open(
                    fileobj=tar_buffer,
                    format=tarfile.GNU_FORMAT,
                    mode='w|',
                    dereference=True) as tar:
                tar.add(src, arcname=dst)
            tar_buffer.seek(0)
            await asyncio.gather(*(asyncio.create_task(
                self._copy_local_to_remote_single(
                    location=location,
                    tar_buffer=cast(io.BufferedRandom, tar_buffer),
                    read_only=read_only)
            ) for location in locations))

    async def _copy_local_to_remote_single(self,
                                           location: str,
                                           tar_buffer: io.BufferedRandom,
                                           read_only: bool = False) -> None:
        location_buffer = io.BufferedReader(tar_buffer.raw)
        proc = await self._run(
            location=location,
            command=["tar", "xf", "-", "-C", "/"],
            encode=False,
            interactive=True,
            stream=True
        )
        while content := location_buffer.read(self.transferBufferSize):
            proc.stdin.write(content)
            await proc.stdin.drain()
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
        with tempfile.TemporaryFile() as tar_buffer:
            while data := await proc.stdout.read(self.transferBufferSize):
                tar_buffer.write(data)
            await proc.wait()
            tar_buffer.seek(0)
            with tarfile.open(
                    fileobj=tar_buffer,
                    mode='r|') as tar:
                utils.extract_tar_stream(tar, src, dst)

    async def _copy_remote_to_remote(self,
                                     src: str,
                                     dst: str,
                                     locations: MutableSequence[str],
                                     source_location: str,
                                     read_only: bool = False) -> None:
        # Check for the need of a temporary copy
        temp_dir = None
        for location in locations:
            if source_location != location:
                temp_dir = tempfile.mkdtemp()
                await self._copy_remote_to_local(
                    src=src,
                    dst=temp_dir,
                    location=source_location,
                    read_only=read_only)
                break
        # Perform the actual copies
        copy_tasks = []
        for location in locations:
            copy_tasks.append(asyncio.create_task(
                self._copy_remote_to_remote_single(
                    src=src,
                    dst=dst,
                    location=location,
                    source_location=source_location,
                    temp_dir=temp_dir,
                    read_only=read_only)))
        await asyncio.gather(*copy_tasks)
        # If a temporary location was created, delete it
        if temp_dir is not None:
            shutil.rmtree(temp_dir)

    async def _copy_remote_to_remote_single(self,
                                            src: str,
                                            dst: str,
                                            location: str,
                                            source_location: str,
                                            temp_dir: Optional[str],
                                            read_only: bool = False) -> None:
        if source_location == location:
            if src != dst:
                command = ['/bin/cp', "-rf", src, dst]
                await self.run(location, command)
        else:
            copy_tasks = []
            for element in os.listdir(temp_dir):
                copy_tasks.append(asyncio.create_task(
                    self._copy_local_to_remote(
                        src=os.path.join(temp_dir, element),
                        dst=dst,
                        locations=[location],
                        read_only=read_only)))
            await asyncio.gather(*copy_tasks)

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
