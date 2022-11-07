from __future__ import annotations

import asyncio
import contextlib
import os
import posixpath
import tarfile
from asyncio import Condition
from asyncio.subprocess import STDOUT
from pathlib import PurePosixPath
from typing import Any, MutableMapping, MutableSequence, Optional, Tuple, Union

import asyncssh
import pkg_resources
from asyncssh import SSHClientConnection
from asyncssh.process import SSHClientProcess
from cachetools import Cache, LRUCache
from jinja2 import Template

from streamflow.core import utils
from streamflow.core.asyncache import cachedmethod
from streamflow.core.data import StreamWrapperContext
from streamflow.core.deployment import Connector, Location
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.scheduling import AvailableLocation, Hardware
from streamflow.data import aiotarstream
from streamflow.data.stream import StreamReaderWrapper, StreamWriterWrapper
from streamflow.deployment.connector.base import BaseConnector
from streamflow.log_handler import logger


async def _get_disk_usage(ssh_client: SSHClientConnection, directory: str) -> float:
    if directory:
        result = await ssh_client.run("df {} | tail -n 1 | awk '{{print $2}}'".format(directory), stderr=STDOUT)
        if result.returncode == 0:
            return float(result.stdout.strip()) / 2 ** 10
        else:
            raise WorkflowExecutionException(result.returncode)
    else:
        return float('inf')


def _parse_hostname(hostname):
    if ':' in hostname:
        hostname, port = hostname.split(':')
        port = int(port)
    else:
        port = 22
    return hostname, port


class SSHContext(object):

    def __init__(self,
                 streamflow_config_dir: str,
                 config: SSHConfig,
                 max_concurrent_sessions: int):
        self._streamflow_config_dir: str = streamflow_config_dir
        self._config: SSHConfig = config
        self._max_concurrent_sessions: int = max_concurrent_sessions
        self._ssh_connection: Optional[SSHClientConnection] = None
        self._sessions: int = 0

    async def __aenter__(self):
        if self._ssh_connection is None:
            self._ssh_connection = await self._get_connection(self._config)
        self._sessions += 1
        return self._ssh_connection

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._sessions -= 1

    async def _get_connection(self, config: SSHConfig) -> Optional[SSHClientConnection]:
        if config is None:
            return None
        (hostname, port) = _parse_hostname(config.hostname)
        passphrase = (self._get_param_from_file(config.ssh_key_passphrase_file)
                      if config.ssh_key_passphrase_file else None)
        password = self._get_param_from_file(config.password_file) if config.password_file else None
        return await asyncssh.connect(
            client_keys=config.client_keys,
            compression_algs=None,
            encryption_algs=[
                "aes128-gcm@openssh.com",
                "aes256-ctr",
                "aes192-ctr",
                "aes128-ctr"
            ],
            known_hosts=() if config.check_host_key else None,
            host=hostname,
            passphrase=passphrase,
            password=password,
            port=port,
            tunnel=await self._get_connection(config.tunnel),
            username=config.username)

    def _get_param_from_file(self,
                             file_path: str):
        if not os.path.isabs(file_path):
            file_path = os.path.join(self._streamflow_config_dir, file_path)
        with open(file_path) as f:
            return f.read().strip()

    async def close(self):
        if self._ssh_connection is not None:
            self._ssh_connection.close()

    def full(self) -> bool:
        return self._sessions == self._max_concurrent_sessions


class SSHContextManager(object):

    def __init__(self,
                 condition: Condition,
                 contexts: MutableSequence[SSHContext]):
        self._condition: Condition = condition
        self._contexts: MutableSequence[SSHContext] = contexts
        self._selected_context: Optional[SSHContext] = None

    async def __aenter__(self):
        async with self._condition:
            while True:
                for context in self._contexts:
                    if not context.full():
                        ssh_connection = await context.__aenter__()
                        self._selected_context = context
                        return ssh_connection
                await self._condition.wait()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        async with self._condition:
            if self._selected_context:
                await self._selected_context.__aexit__(exc_type, exc_val, exc_tb)
                self._condition.notify_all()


class SSHContextFactory(object):
    def __init__(self,
                 streamflow_config_dir: str,
                 config: SSHConfig,
                 max_concurrent_sessions: int,
                 max_connections: int):
        self._condition: Condition = Condition()
        self._contexts: MutableSequence[SSHContext] = [
            SSHContext(
                streamflow_config_dir=streamflow_config_dir,
                config=config,
                max_concurrent_sessions=max_concurrent_sessions)
            for _ in range(max_connections)]

    async def close(self):
        await asyncio.gather(*(asyncio.create_task(c.close()) for c in self._contexts))

    def get(self):
        return SSHContextManager(
            condition=self._condition,
            contexts=self._contexts)


class SSHStreamWrapperContext(StreamWrapperContext):

    def __init__(self,
                 src: str,
                 ssh_context: SSHContextManager):
        super().__init__()
        self.src: str = src
        self.ssh_context: SSHContextManager = ssh_context
        self.ssh_process: Optional[SSHClientProcess] = None
        self.stream: Optional[StreamReaderWrapper] = None

    async def __aenter__(self):
        ssh_client = await self.ssh_context.__aenter__()
        dirname, basename = posixpath.split(self.src)
        self.ssh_process = await (await ssh_client.create_process(
            "tar chf - -C {} {}".format(dirname, basename),
            stdin=asyncio.subprocess.DEVNULL,
            encoding=None)).__aenter__()
        self.stream = StreamReaderWrapper(self.ssh_process.stdout)
        return self.stream

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.stream:
            await self.stream.close()
        if self.ssh_process:
            await self.ssh_process.__aexit__(exc_type, exc_val, exc_tb)
        await self.ssh_context.__aexit__(exc_type, exc_val, exc_tb)


class SSHConfig(object):

    def __init__(self,
                 check_host_key: bool,
                 client_keys: MutableSequence[str],
                 hostname: str,
                 password_file: Optional[str],
                 ssh_key_passphrase_file: Optional[str],
                 tunnel: Optional[SSHConfig],
                 username: str):
        self.check_host_key: bool = check_host_key
        self.client_keys: MutableSequence[str] = client_keys
        self.hostname: str = hostname
        self.password_file: Optional[str] = password_file
        self.ssh_key_passphrase_file: Optional[str] = ssh_key_passphrase_file
        self.tunnel: Optional[SSHConfig] = tunnel
        self.username: str = username


class SSHConnector(BaseConnector):

    @staticmethod
    def _get_command(location: Location,
                     command: MutableSequence[str],
                     environment: MutableMapping[str, str] = None,
                     workdir: Optional[str] = None,
                     stdin: Optional[Union[int, str]] = None,
                     stdout: Union[int, str] = asyncio.subprocess.STDOUT,
                     stderr: Union[int, str] = asyncio.subprocess.STDOUT,
                     job_name: Optional[str] = None):
        command = utils.create_command(
            command=command,
            environment=environment,
            workdir=workdir,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr)
        logger.debug("Executing command {command} on {location} {job}".format(
            command=command,
            location=location,
            job="for job {job}".format(job=job_name) if job_name else ""))
        return utils.encode_command(command)

    def __init__(self,
                 deployment_name: str,
                 config_dir: str,
                 nodes: MutableSequence[Any],
                 username: str,
                 checkHostKey: bool = True,
                 dataTransferConnection: Optional[Union[str, MutableMapping[str, Any]]] = None,
                 file: Optional[str] = None,
                 maxConcurrentSessions: int = 10,
                 maxConnections: int = 1,
                 passwordFile: Optional[str] = None,
                 services: Optional[MutableMapping[str, str]] = None,
                 sharedPaths: Optional[MutableSequence[str]] = None,
                 sshKey: Optional[str] = None,
                 sshKeyPassphraseFile: Optional[str] = None,
                 tunnel: Optional[MutableMapping[str, Any]] = None,
                 transferBufferSize: int = 2 ** 16) -> None:
        super().__init__(
            deployment_name=deployment_name,
            config_dir=config_dir,
            transferBufferSize=transferBufferSize)
        self.templates: MutableMapping[str, Template] = {}
        if file is not None:
            logger.warn("The `file` keyword is deprecated and will be removed in StreamFlow 0.3.0. "
                        "Use `services` instead.")
            with open(os.path.join(self.config_dir, file)) as f:
                self.templates['__DEFAULT__'] = Template(f.read())
        else:
            self.templates['__DEFAULT__'] = Template('#!/bin/sh\n\n{{streamflow_command}}')
        if services:
            for name, service in services.items():
                with open(os.path.join(self.config_dir, service)) as f:
                    self.templates[name]: Optional[Template] = Template(f.read())
        self.checkHostKey: bool = checkHostKey
        self.passwordFile: Optional[str] = passwordFile
        self.maxConcurrentSessions: int = maxConcurrentSessions
        self.maxConnections: int = maxConnections
        self.sharedPaths: MutableSequence[str] = sharedPaths or []
        self.sshKey: Optional[str] = sshKey
        self.sshKeyPassphraseFile: Optional[str] = sshKeyPassphraseFile
        self.ssh_context_factories: MutableMapping[str, SSHContextFactory] = {}
        self.data_transfer_context_factories: MutableMapping[str, SSHContextFactory] = {}
        self.username: str = username
        self.tunnel: Optional[SSHConfig] = self._get_config(tunnel)
        self.dataTransferConfig: Optional[SSHConfig] = self._get_config(
            dataTransferConnection)
        self.nodes: MutableMapping[str, SSHConfig] = {n.hostname: n for n in [self._get_config(n) for n in nodes]}
        self.hardwareCache: Cache = LRUCache(maxsize=len(self.nodes))

    def _get_command_from_template(self,
                                   command: str,
                                   environment: MutableMapping[str, str] = None,
                                   template: Optional[str] = None,
                                   workdir: str = None) -> str:
        return self.templates[template or '__DEFAULT__'].render(
            streamflow_command="sh -c '{command}'".format(command=command),
            streamflow_environment=environment,
            streamflow_workdir=workdir)

    async def _copy_local_to_remote_single(self,
                                           src: str,
                                           dst: str,
                                           location: Location,
                                           read_only: bool = False):
        async with self._get_data_transfer_client(location.name) as ssh_client:
            async with ssh_client.create_process(
                    "tar xf - -C /",
                    stderr=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.DEVNULL,
                    encoding=None) as proc:
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

    async def _copy_remote_to_local(self,
                                    src: str,
                                    dst: str,
                                    location: Location,
                                    read_only: bool = False) -> None:
        async with self._get_data_transfer_client(location.name) as ssh_client:
            async with ssh_client.create_process(
                    "tar chf - -C / " + posixpath.relpath(src, '/'),
                    stdin=asyncio.subprocess.DEVNULL,
                    encoding=None) as proc:
                try:
                    async with aiotarstream.open(
                            stream=StreamReaderWrapper(proc.stdout),
                            mode='r',
                            copybufsize=self.transferBufferSize) as tar:
                        await utils.extract_tar_stream(tar, src, dst, self.transferBufferSize)
                except tarfile.TarError as e:
                    raise WorkflowExecutionException("Error copying {} from location {} to {}: {}".format(
                        src, location, dst, str(e))) from e

    async def _copy_remote_to_remote(self,
                                     src: str,
                                     dst: str,
                                     locations: MutableSequence[Location],
                                     source_location: Location,
                                     source_connector: Optional[Connector] = None,
                                     read_only: bool = False) -> None:
        source_connector = source_connector or self
        if source_connector == self and source_location.name in [loc.name for loc in locations]:
            if src != dst:
                command = ['/bin/cp', "-rf", src, dst]
                await self.run(source_location, command)
                locations.remove(source_location)
        write_command = " ".join(await utils.get_remote_to_remote_write_command(
            src_connector=source_connector,
            src_location=source_location,
            src=src,
            dst_connector=self,
            dst_locations=locations,
            dst=dst))
        if locations:
            async with source_connector._get_stream_reader(source_location, src) as reader:
                async with contextlib.AsyncExitStack() as exit_stack:
                    # Open a target StreamWriter for each location
                    writer_clients = await asyncio.gather(*(asyncio.create_task(
                        exit_stack.enter_async_context(self._get_data_transfer_client(location.name))
                    ) for location in locations))
                    async with contextlib.AsyncExitStack() as writers_stack:
                        writers = await asyncio.gather(*(asyncio.create_task(
                            writers_stack.enter_async_context(
                                client.create_process(
                                    write_command,
                                    stderr=asyncio.subprocess.DEVNULL,
                                    stdout=asyncio.subprocess.DEVNULL,
                                    encoding=None)
                            )) for client in writer_clients))
                        # Multiplex the reader output to all the writers
                        while content := await reader.read(source_connector.transferBufferSize):
                            for writer in writers:
                                writer.stdin.write(content)
                            await asyncio.gather(*(asyncio.create_task(writer.stdin.drain()) for writer in writers))

    def _get_config(self,
                    node: Union[str, MutableMapping[str, Any]]):
        if node is None:
            return None
        elif isinstance(node, str):
            node = {'hostname': node}
        ssh_key = node['sshKey'] if 'sshKey' in node else self.sshKey
        return SSHConfig(
            hostname=node['hostname'],
            username=node['username'] if 'username' in node else self.username,
            check_host_key=node['checkHostKey'] if 'checkHostKey' in node else self.checkHostKey,
            client_keys=[ssh_key] if ssh_key is not None else [],
            password_file=node['passwordFile'] if 'passwordFile' in node else self.passwordFile,
            ssh_key_passphrase_file=(node['sshKeyPassphraseFile'] if 'sshKeyPassphraseFile' in node else
                                     self.sshKeyPassphraseFile),
            tunnel=(self._get_config(node['tunnel']) if 'tunnel' in node else
                    self.tunnel if hasattr(self, 'tunnel') else
                    None))

    def _get_data_transfer_client(self, location: str) -> SSHContextManager:
        if self.dataTransferConfig:
            if location not in self.data_transfer_context_factories:
                self.data_transfer_context_factories[location] = SSHContextFactory(
                    streamflow_config_dir=self.config_dir,
                    config=self.dataTransferConfig,
                    max_concurrent_sessions=self.maxConcurrentSessions,
                    max_connections=self.maxConnections)
            return self.data_transfer_context_factories[location].get()
        else:
            return self._get_ssh_client(location)

    async def _get_existing_parent(self, location: str, directory: str):
        if directory is None:
            return None
        command_template = "test -e \"{path}\""
        async with self._get_ssh_client(location) as ssh_client:
            while True:
                result = await ssh_client.run(command_template.format(path=directory), stderr=STDOUT)
                if result.returncode == 0:
                    return directory
                elif result.returncode == 1:
                    directory = PurePosixPath(directory).parent
                else:
                    raise WorkflowExecutionException(result.stdout.strip())

    @cachedmethod(lambda self: self.hardwareCache)
    async def _get_location_hardware(self,
                                     location: str,
                                     input_directory: str,
                                     output_directory: str,
                                     tmp_directory: str) -> Hardware:
        try:
            async with self._get_ssh_client(location) as ssh_client:
                cores, memory, input_directory, output_directory, tmp_directory = await asyncio.gather(
                    asyncio.create_task(ssh_client.run("nproc", stderr=STDOUT)),
                    asyncio.create_task(ssh_client.run("free | grep Mem | awk '{print $2}'", stderr=STDOUT)),
                    asyncio.create_task(_get_disk_usage(ssh_client, input_directory)),
                    asyncio.create_task(_get_disk_usage(ssh_client, output_directory)),
                    asyncio.create_task(_get_disk_usage(ssh_client, tmp_directory)))
                if (cores.returncode == 0 and
                        memory.returncode == 0):
                    return Hardware(
                        cores=float(cores.stdout.strip()),
                        memory=float(memory.stdout.strip()) / 2 ** 10,
                        input_directory=input_directory,
                        output_directory=output_directory,
                        tmp_directory=tmp_directory)
                else:
                    raise WorkflowExecutionException(
                        "Impossible to retrieve locations for {location}".format(location=location))
        except WorkflowExecutionException:
            raise WorkflowExecutionException(
                "Impossible to retrieve locations for {location}".format(location=location))

    def _get_run_command(self,
                         command: str,
                         location: Location,
                         interactive: bool = False):
        return "".join([
            "ssh ",
            "{location} ",
            "{command}"
        ]).format(
            location=location.name,
            command=command)

    def _get_ssh_client(self, location: str) -> SSHContextManager:
        if location not in self.ssh_context_factories:
            self.ssh_context_factories[location] = SSHContextFactory(
                streamflow_config_dir=self.config_dir,
                config=self.nodes[location],
                max_concurrent_sessions=self.maxConcurrentSessions,
                max_connections=self.maxConnections)
        return self.ssh_context_factories[location].get()

    def _get_stream_reader(self,
                           location: Location,
                           src: str) -> StreamWrapperContext:
        return SSHStreamWrapperContext(
            src=src,
            ssh_context=self._get_data_transfer_client(location.name))

    async def deploy(self, external: bool) -> None:
        pass

    async def get_available_locations(self,
                                      service: Optional[str] = None,
                                      input_directory: Optional[str] = None,
                                      output_directory: Optional[str] = None,
                                      tmp_directory: Optional[str] = None) -> MutableMapping[str, AvailableLocation]:
        locations = {}
        for location_obj in self.nodes.values():
            inpdir, outdir, tmpdir = await asyncio.gather(
                asyncio.create_task(self._get_existing_parent(location_obj.hostname, input_directory)),
                asyncio.create_task(self._get_existing_parent(location_obj.hostname, output_directory)),
                asyncio.create_task(self._get_existing_parent(location_obj.hostname, tmp_directory)))
            hardware = await self._get_location_hardware(
                location=location_obj.hostname,
                input_directory=inpdir,
                output_directory=outdir,
                tmp_directory=tmpdir)
            locations[location_obj.hostname] = AvailableLocation(
                name=location_obj.hostname,
                deployment=self.deployment_name,
                service=service,
                hostname=location_obj.hostname,
                hardware=hardware)
        return locations

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join('schemas', 'ssh.json'))

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
        command = self._get_command(
            location=location,
            command=command,
            environment=environment,
            workdir=workdir,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            job_name=job_name)
        if job_name is not None:
            command = self._get_command_from_template(
                command=command,
                environment=environment,
                template=location.service,
                workdir=workdir)
            command = utils.encode_command(command)
            async with self._get_ssh_client(location.name) as ssh_client:
                result = await asyncio.wait_for(ssh_client.run(command, stderr=STDOUT), timeout=timeout)
        else:
            async with self._get_ssh_client(location.name) as ssh_client:
                result = await asyncio.wait_for(
                    ssh_client.run("sh -c '{command}'".format(command=command), stderr=STDOUT),
                    timeout=timeout)
        if capture_output:
            return result.stdout.strip(), result.returncode

    async def undeploy(self, external: bool) -> None:
        for ssh_context in self.ssh_context_factories.values():
            await ssh_context.close()
        self.ssh_context_factories = {}
        for ssh_context in self.data_transfer_context_factories.values():
            await ssh_context.close()
        self.data_transfer_context_factories = {}
