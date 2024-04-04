from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import posixpath
import tarfile
from pathlib import PurePosixPath
from typing import Any, MutableMapping, MutableSequence

import asyncssh
from asyncssh import ChannelOpenError, ConnectionLost
from importlib_resources import files

from streamflow.core import utils
from streamflow.core.data import StreamWrapperContextManager
from streamflow.core.deployment import Connector, ExecutionLocation
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.scheduling import AvailableLocation, Hardware
from streamflow.deployment import aiotarstream
from streamflow.deployment.connector.base import BaseConnector, extract_tar_stream
from streamflow.deployment.stream import StreamReaderWrapper, StreamWriterWrapper
from streamflow.deployment.template import CommandTemplateMap
from streamflow.log_handler import logger


def _parse_hostname(hostname):
    if ":" in hostname:
        hostname, port = hostname.split(":")
        port = int(port)
    else:
        port = 22
    return hostname, port


class SSHContext:
    def __init__(
        self,
        streamflow_config_dir: str,
        config: SSHConfig,
        max_concurrent_sessions: int,
        retries: int,
        retry_delay: int,
    ):
        self._streamflow_config_dir: str = streamflow_config_dir
        self._config: SSHConfig = config
        self._max_concurrent_sessions: int = max_concurrent_sessions
        self._ssh_connection: asyncssh.SSHClientConnection | None = None
        self._connecting = False
        self._retries = retries
        self._retry_delay = retry_delay
        self._connect_event: asyncio.Event = asyncio.Event()

    async def get_connection(self) -> asyncssh.SSHClientConnection:
        if self._ssh_connection is None:
            if not self._connecting:
                self._connecting = True
                for i in range(1, self._retries + 1):
                    try:
                        self._ssh_connection = await self._get_connection(self._config)
                        break
                    except (ConnectionError, ConnectionLost) as e:
                        if i == self._retries:
                            logger.exception(
                                f"Impossible to connect to {self._config.hostname}: {e}"
                            )
                            self._connect_event.set()
                            self.close()
                            raise
                        if logger.isEnabledFor(logging.WARNING):
                            logger.warning(
                                f"Connection to {self._config.hostname} failed: {e}. "
                                f"Waiting {self._retry_delay} seconds for the next attempt."
                            )
                    except asyncssh.Error:
                        self._connect_event.set()
                        self.close()
                        raise
                    await asyncio.sleep(self._retry_delay)
                self._connect_event.set()
            else:
                await self._connect_event.wait()
                if self._ssh_connection is None:
                    raise WorkflowExecutionException(
                        f"Impossible to connect to {self._config.hostname}"
                    )
        return self._ssh_connection

    def get_hostname(self) -> str:
        return self._config.hostname

    async def _get_connection(
        self, config: SSHConfig
    ) -> asyncssh.SSHClientConnection | None:
        if config is None:
            return None
        (hostname, port) = _parse_hostname(config.hostname)
        passphrase = (
            self._get_param_from_file(config.ssh_key_passphrase_file)
            if config.ssh_key_passphrase_file
            else None
        )
        password = (
            self._get_param_from_file(config.password_file)
            if config.password_file
            else None
        )
        return await asyncssh.connect(
            client_keys=config.client_keys,
            compression_algs=None,
            encryption_algs=[
                "aes128-gcm@openssh.com",
                "aes256-ctr",
                "aes192-ctr",
                "aes128-ctr",
            ],
            known_hosts=() if config.check_host_key else None,
            host=hostname,
            passphrase=passphrase,
            password=password,
            port=port,
            tunnel=await self._get_connection(config.tunnel),
            username=config.username,
        )

    def _get_param_from_file(self, file_path: str):
        if not os.path.isabs(file_path):
            file_path = os.path.join(self._streamflow_config_dir, file_path)
        with open(file_path) as f:
            return f.read().strip()

    def close(self):
        self._connecting = False
        if self._ssh_connection is not None:
            self._ssh_connection.close()
            self._ssh_connection = None
        if self._connect_event.is_set():
            self._connect_event.clear()

    def full(self) -> bool:
        if self._ssh_connection:
            return len(self._ssh_connection._channels) >= self._max_concurrent_sessions
        else:
            return False


class SSHContextManager:
    def __init__(
        self,
        condition: asyncio.Condition,
        contexts: MutableSequence[SSHContext],
        command: str,
        environment: MutableMapping[str, str] | None,
        stdin: int = asyncio.subprocess.PIPE,
        stdout: int = asyncio.subprocess.PIPE,
        stderr: int = asyncio.subprocess.PIPE,
        encoding: str | None = "utf-8",
    ):
        self.command: str = command
        self.environment: MutableMapping[str, str] | None = environment
        self.stdin: int = stdin
        self.stdout: int = stdout
        self.stderr: int = stderr
        self.encoding: str | None = encoding
        self._condition: asyncio.Condition = condition
        self._contexts: MutableSequence[SSHContext] = contexts
        self._selected_context: SSHContext | None = None
        self._proc: asyncssh.SSHClientProcess | None = None

    async def __aenter__(self) -> asyncssh.SSHClientProcess:
        async with self._condition:
            while True:
                for context in self._contexts:
                    if not context.full():
                        ssh_connection = await context.get_connection()
                        try:
                            self._selected_context = context
                            self._proc = await ssh_connection.create_process(
                                self.command,
                                env=self.environment,
                                stdin=self.stdin,
                                stdout=self.stdout,
                                stderr=self.stderr,
                                encoding=self.encoding,
                            )
                            await self._proc.__aenter__()
                            return self._proc
                        except ChannelOpenError as coe:
                            logger.warning(
                                f"Error opening SSH session to {context.get_hostname()} "
                                f"to execute command `{self.command}`: [{coe.code}] {coe.reason}"
                            )
                await self._condition.wait()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        async with self._condition:
            if self._selected_context:
                if self._proc:
                    await self._proc.__aexit__(exc_type, exc_val, exc_tb)
                self._condition.notify_all()


class SSHContextFactory:
    def __init__(
        self,
        streamflow_config_dir: str,
        config: SSHConfig,
        max_concurrent_sessions: int,
        max_connections: int,
        retries: int,
        retry_delay: int,
    ):
        self._condition: asyncio.Condition = asyncio.Condition()
        self._contexts: MutableSequence[SSHContext] = [
            SSHContext(
                streamflow_config_dir=streamflow_config_dir,
                config=config,
                max_concurrent_sessions=max_concurrent_sessions,
                retries=retries,
                retry_delay=retry_delay,
            )
            for _ in range(max_connections)
        ]

    def close(self):
        for c in self._contexts:
            c.close()

    def get(
        self,
        command: str,
        environment: MutableMapping[str, str] | None,
        stdin: int = asyncio.subprocess.PIPE,
        stdout: int = asyncio.subprocess.PIPE,
        stderr: int = asyncio.subprocess.PIPE,
        encoding: str | None = "utf-8",
    ):
        return SSHContextManager(
            condition=self._condition,
            contexts=self._contexts,
            command=command,
            environment=environment,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            encoding=encoding,
        )


class SSHStreamWrapperContextManager(StreamWrapperContextManager):
    def __init__(
        self,
        src: str,
        environment: MutableMapping[str, str],
        ssh_context_factory: SSHContextFactory,
    ):
        super().__init__()
        self.src: str = src
        self.environment: MutableMapping[str, str] = environment
        self.ssh_context_factory: SSHContextFactory = ssh_context_factory
        self.ssh_context: SSHContextManager | None = None
        self.stream: StreamReaderWrapper | None = None

    async def __aenter__(self):
        dirname, basename = posixpath.split(self.src)
        self.ssh_context = self.ssh_context_factory.get(
            command=f"tar chf - -C {dirname} {basename}",
            environment=self.environment,
            stdin=asyncio.subprocess.DEVNULL,
            encoding=None,
        )
        proc = await self.ssh_context.__aenter__()
        self.stream = StreamReaderWrapper(proc.stdout)
        return self.stream

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.stream:
            await self.stream.close()
        await self.ssh_context.__aexit__(exc_type, exc_val, exc_tb)


class SSHConfig:
    def __init__(
        self,
        check_host_key: bool,
        client_keys: MutableSequence[str],
        hostname: str,
        password_file: str | None,
        ssh_key_passphrase_file: str | None,
        tunnel: SSHConfig | None,
        username: str,
    ):
        self.check_host_key: bool = check_host_key
        self.client_keys: MutableSequence[str] = client_keys
        self.hostname: str = hostname
        self.password_file: str | None = password_file
        self.ssh_key_passphrase_file: str | None = ssh_key_passphrase_file
        self.tunnel: SSHConfig | None = tunnel
        self.username: str = username


class SSHConnector(BaseConnector):
    def __init__(
        self,
        deployment_name: str,
        config_dir: str,
        nodes: MutableSequence[Any],
        username: str | None = None,
        checkHostKey: bool = True,
        dataTransferConnection: str | MutableMapping[str, Any] | None = None,
        file: str | None = None,
        maxConcurrentSessions: int = 10,
        maxConnections: int = 1,
        retries: int = 3,
        retryDelay: int = 5,
        passwordFile: str | None = None,
        services: MutableMapping[str, str] | None = None,
        sharedPaths: MutableSequence[str] | None = None,
        sshKey: str | None = None,
        sshKeyPassphraseFile: str | None = None,
        tunnel: MutableMapping[str, Any] | None = None,
        transferBufferSize: int = 2**16,
    ) -> None:
        super().__init__(
            deployment_name=deployment_name,
            config_dir=config_dir,
            transferBufferSize=transferBufferSize,
        )
        services_map: MutableMapping[str, Any] = {}
        if services:
            for name, service in services.items():
                with open(os.path.join(self.config_dir, service)) as f:
                    services_map[name] = f.read()
        if file is not None:
            if logger.isEnabledFor(logging.WARNING):
                logger.warning(
                    "The `file` keyword is deprecated and will be removed in StreamFlow 0.3.0. "
                    "Use `services` instead."
                )
            with open(os.path.join(self.config_dir, file)) as f:
                self.template_map: CommandTemplateMap = CommandTemplateMap(
                    default=f.read(), template_map=services_map
                )
        else:
            self.template_map: CommandTemplateMap = CommandTemplateMap(
                default="#!/bin/sh\n\n{{streamflow_command}}",
                template_map=services_map,
            )
        self.checkHostKey: bool = checkHostKey
        self.passwordFile: str | None = passwordFile
        self.maxConcurrentSessions: int = maxConcurrentSessions
        self.maxConnections: int = maxConnections
        self.retries: int = retries
        self.retryDelay: int = retryDelay
        self.sharedPaths: MutableSequence[str] = sharedPaths or []
        self.sshKey: str | None = sshKey
        self.sshKeyPassphraseFile: str | None = sshKeyPassphraseFile
        self.ssh_context_factories: MutableMapping[str, SSHContextFactory] = {}
        self.data_transfer_context_factories: MutableMapping[str, SSHContextFactory] = (
            {}
        )
        self.username: str = username
        self.tunnel: SSHConfig | None = self._get_config(tunnel)
        self.dataTransferConfig: SSHConfig | None = self._get_config(
            dataTransferConnection
        )
        self.nodes: MutableMapping[str, SSHConfig] = {
            n.hostname: n for n in [self._get_config(n) for n in nodes]
        }

    async def _copy_local_to_remote_single(
        self, src: str, dst: str, location: ExecutionLocation, read_only: bool = False
    ):
        async with self._get_data_transfer_process(
            location=location.name,
            command="tar xf - -C /",
            stderr=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            encoding=None,
            environment=location.environment,
        ) as proc:
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

    async def _copy_remote_to_local(
        self, src: str, dst: str, location: ExecutionLocation, read_only: bool = False
    ) -> None:
        dirname, basename = posixpath.split(src)
        async with self._get_data_transfer_process(
            location=location.name,
            command=f"tar chf - -C {dirname} {basename}",
            stdin=asyncio.subprocess.DEVNULL,
            encoding=None,
            environment=location.environment,
        ) as proc:
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
        if source_connector == self and source_location.name in [
            loc.name for loc in locations
        ]:
            if src != dst:
                command = ["/bin/cp", "-rf", src, dst]
                await self.run(source_location, command)
                locations.remove(source_location)
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
        if locations:
            conn_per_round = min(len(locations), self.maxConcurrentSessions)
            rounds = self.maxConcurrentSessions // conn_per_round
            if len(locations) % conn_per_round != 0:
                rounds += 1
            location_groups = [
                locations[i : i + rounds] for i in range(0, len(locations), rounds)
            ]
            for location_group in location_groups:
                async with await source_connector.get_stream_reader(
                    source_location, src
                ) as reader:
                    async with contextlib.AsyncExitStack() as exit_stack:
                        # Open a target StreamWriter for each location
                        writers = await asyncio.gather(
                            *(
                                asyncio.create_task(
                                    exit_stack.enter_async_context(
                                        self._get_data_transfer_process(
                                            location=location.name,
                                            command=write_command,
                                            stderr=asyncio.subprocess.DEVNULL,
                                            stdout=asyncio.subprocess.DEVNULL,
                                            encoding=None,
                                            environment=location.environment,
                                        )
                                    )
                                )
                                for location in location_group
                            )
                        )
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

    async def _get_available_location(
        self,
        location: str,
        input_directory: str | None,
        output_directory: str | None,
        tmp_directory: str | None,
    ) -> Hardware:
        existing_directories = await self._get_existing_parents(
            location,
            {
                directory
                for directory in [input_directory, output_directory, tmp_directory]
                if directory is not None
            },
        )
        directories = " ".join(existing_directories.values())
        async with self._get_ssh_client_process(
            location=location,
            command="nproc && "
            "free | grep Mem | awk '{print $2}' && "
            f"df {directories} | tail -n +2 | awk '{{print $2}}'",
            stderr=asyncio.subprocess.STDOUT,
        ) as proc:
            result = await proc.wait()
            if result.returncode == 0:
                cores, memory, *dir_size_list = str(result.stdout.strip()).split("\n")
                hardware = Hardware(
                    cores=float(cores),
                    memory=float(memory) / 2**10,
                )

                dir_sizes = dict(zip(existing_directories.values(), dir_size_list))
                if input_directory is not None:
                    indir_size = dir_sizes[existing_directories[input_directory]]
                    hardware.input_directory = float(indir_size) / 2**10
                else:
                    hardware.input_directory = float("inf")

                if output_directory is not None:
                    outdir_size = dir_sizes[existing_directories[output_directory]]
                    hardware.output_directory = float(outdir_size) / 2**10
                else:
                    hardware.output_directory = float("inf")

                if tmp_directory is not None:
                    tmpdir_size = dir_sizes[existing_directories[tmp_directory]]
                    hardware.tmp_directory = (
                        float(tmpdir_size) / 2**10 if tmp_directory else float("inf")
                    )
                else:
                    hardware.tmp_directory = float("inf")
                return hardware
            else:
                raise WorkflowExecutionException(result.returncode)

    def _get_command(
        self,
        location: ExecutionLocation,
        command: MutableSequence[str],
        environment: MutableMapping[str, str] = None,
        workdir: str | None = None,
        stdin: int | str | None = None,
        stdout: int | str = asyncio.subprocess.STDOUT,
        stderr: int | str = asyncio.subprocess.STDOUT,
        job_name: str | None = None,
    ):
        command = utils.create_command(
            class_name=self.__class__.__name__,
            command=command,
            environment=environment,
            workdir=workdir,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
        )
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                f"EXECUTING command {command} on {location}" f" for job {job_name}"
                if job_name
                else ""
            )
        return utils.encode_command(command)

    def _get_config(self, node: str | MutableMapping[str, Any]):
        if node is None:
            return None
        elif isinstance(node, str):
            node = {"hostname": node}
        ssh_key = node["sshKey"] if "sshKey" in node else self.sshKey
        return SSHConfig(
            hostname=node["hostname"],
            username=node["username"] if "username" in node else self.username,
            check_host_key=(
                node["checkHostKey"] if "checkHostKey" in node else self.checkHostKey
            ),
            client_keys=[ssh_key] if ssh_key is not None else [],
            password_file=(
                node["passwordFile"] if "passwordFile" in node else self.passwordFile
            ),
            ssh_key_passphrase_file=(
                node["sshKeyPassphraseFile"]
                if "sshKeyPassphraseFile" in node
                else self.sshKeyPassphraseFile
            ),
            tunnel=(
                self._get_config(node["tunnel"])
                if "tunnel" in node
                else self.tunnel if hasattr(self, "tunnel") else None
            ),
        )

    def _get_data_transfer_process(
        self,
        location: str,
        command: str,
        stdin: int = asyncio.subprocess.PIPE,
        stdout: int = asyncio.subprocess.PIPE,
        stderr: int = asyncio.subprocess.PIPE,
        encoding: str | None = "utf-8",
        environment: MutableMapping[str, str] | None = None,
    ) -> SSHContextManager:
        if self.dataTransferConfig:
            if location not in self.data_transfer_context_factories:
                self.data_transfer_context_factories[location] = SSHContextFactory(
                    streamflow_config_dir=self.config_dir,
                    config=self.dataTransferConfig,
                    max_concurrent_sessions=self.maxConcurrentSessions,
                    max_connections=self.maxConnections,
                    retries=self.retries,
                    retry_delay=self.retryDelay,
                )
            return self.data_transfer_context_factories[location].get(
                command=command,
                environment=environment,
                stdin=stdin,
                stdout=stdout,
                stderr=stderr,
                encoding=encoding,
            )
        else:
            return self._get_ssh_client_process(
                location=location,
                command=command,
                stdin=stdin,
                stdout=stdout,
                stderr=stderr,
                encoding=encoding,
                environment=environment,
            )

    async def _get_existing_parents(
        self, location: str, directories: set[str]
    ) -> MutableMapping[str, str]:
        """
        This method returns a mapping between input directories and their existing parents
        """
        if not directories:
            return {}
        existing_directories = {}
        missing_directories = {
            directory: PurePosixPath(directory) for directory in directories
        }
        while missing_directories:
            async with self._get_ssh_client_process(
                location=location,
                command=" && ".join(
                    [
                        f'test -e "{directory}" ; echo $?'
                        for directory in missing_directories.values()
                    ]
                ),
                stderr=asyncio.subprocess.STDOUT,
            ) as proc:
                result = await proc.wait()
                if result.returncode == 0:
                    for (input_dir, checked_dir), response in zip(
                        missing_directories.items(),
                        str(result.stdout.strip()).split("\n"),
                    ):
                        if int(response) == 0:
                            existing_directories[input_dir] = checked_dir.as_posix()
                        elif int(response) == 1:
                            # Tested directory does not exist: the parent dir will be verified in next iteration
                            missing_directories[input_dir] = checked_dir.parent
                        else:
                            raise WorkflowExecutionException(
                                f"An error occurred while trying to check existence of directory {checked_dir} "
                                f"on location {location} (error code: {response})"
                            )
                else:
                    raise WorkflowExecutionException(result.stdout.strip())
            missing_directories = {
                k: v
                for k, v in missing_directories.items()
                if k not in existing_directories.keys()
            }
        return existing_directories

    def _get_run_command(
        self, command: str, location: ExecutionLocation, interactive: bool = False
    ):
        return f"ssh {location.name} {command}"

    def _get_ssh_client_process(
        self,
        location: str,
        command: str,
        stdin: int = asyncio.subprocess.PIPE,
        stdout: int = asyncio.subprocess.PIPE,
        stderr: int = asyncio.subprocess.PIPE,
        encoding: str | None = "utf-8",
        environment: MutableMapping[str, str] | None = None,
    ) -> SSHContextManager:
        if location not in self.ssh_context_factories:
            self.ssh_context_factories[location] = SSHContextFactory(
                streamflow_config_dir=self.config_dir,
                config=self.nodes[location],
                max_concurrent_sessions=self.maxConcurrentSessions,
                max_connections=self.maxConnections,
                retries=self.retries,
                retry_delay=self.retryDelay,
            )
        return self.ssh_context_factories[location].get(
            command=command,
            environment=environment,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            encoding=encoding,
        )

    async def get_stream_reader(
        self, location: ExecutionLocation, src: str
    ) -> StreamWrapperContextManager:
        if self.dataTransferConfig:
            if location not in self.data_transfer_context_factories:
                self.data_transfer_context_factories[location.name] = SSHContextFactory(
                    streamflow_config_dir=self.config_dir,
                    config=self.dataTransferConfig,
                    max_concurrent_sessions=self.maxConcurrentSessions,
                    max_connections=self.maxConnections,
                    retries=self.retries,
                    retry_delay=self.retryDelay,
                )
            ssh_context_factory = self.data_transfer_context_factories[location.name]
        else:
            if location not in self.ssh_context_factories:
                self.ssh_context_factories[location.name] = SSHContextFactory(
                    streamflow_config_dir=self.config_dir,
                    config=self.nodes[location.name],
                    max_concurrent_sessions=self.maxConcurrentSessions,
                    max_connections=self.maxConnections,
                    retries=self.retries,
                    retry_delay=self.retryDelay,
                )
            ssh_context_factory = self.ssh_context_factories[location.name]
        return SSHStreamWrapperContextManager(
            src=src,
            environment=location.environment,
            ssh_context_factory=ssh_context_factory,
        )

    async def deploy(self, external: bool) -> None:
        pass

    async def get_available_locations(
        self,
        service: str | None = None,
        input_directory: str | None = None,
        output_directory: str | None = None,
        tmp_directory: str | None = None,
    ) -> MutableMapping[str, AvailableLocation]:
        locations = {}
        hardware_locations = await asyncio.gather(
            *(
                asyncio.create_task(
                    self._get_available_location(
                        location=location_obj.hostname,
                        input_directory=input_directory,
                        output_directory=output_directory,
                        tmp_directory=tmp_directory,
                    )
                )
                for location_obj in self.nodes.values()
            )
        )
        for location_obj, hardware in zip(self.nodes.values(), hardware_locations):
            locations[location_obj.hostname] = AvailableLocation(
                name=location_obj.hostname,
                deployment=self.deployment_name,
                service=service,
                hostname=location_obj.hostname,
                hardware=hardware,
            )
        return locations

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("ssh.json")
            .read_text("utf-8")
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
        command = self._get_command(
            location=location,
            command=command,
            environment=environment,
            workdir=workdir,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            job_name=job_name,
        )
        if job_name is not None:
            if logger.isEnabledFor(logging.WARNING):
                if not self.template_map.is_empty() and location.service is None:
                    logger.warning(
                        f"Deployment {self.deployment_name} contains some service definitions, "
                        f"but none of them has been specified to execute job {job_name}. Execution "
                        f"will fall back to the default template."
                    )
            command = self.template_map.get_command(
                command=command,
                template=location.service,
                environment=environment,
                workdir=workdir,
            )
            command = utils.encode_command(command)
            async with self._get_ssh_client_process(
                location=location.name,
                command=command,
                stderr=asyncio.subprocess.STDOUT,
                environment=environment,
            ) as proc:
                result = await proc.wait(timeout=timeout)
        else:
            async with self._get_ssh_client_process(
                location=location.name,
                command=command,
                stderr=asyncio.subprocess.STDOUT,
                environment=environment,
            ) as proc:
                result = await proc.wait(timeout=timeout)
        return result.stdout.strip(), result.returncode if capture_output else None

    async def undeploy(self, external: bool) -> None:
        for ssh_context in self.ssh_context_factories.values():
            ssh_context.close()
        self.ssh_context_factories = {}
        for ssh_context in self.data_transfer_context_factories.values():
            ssh_context.close()
        self.data_transfer_context_factories = {}
