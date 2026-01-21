from __future__ import annotations

import asyncio
import logging
import os
from abc import ABC
from collections.abc import MutableMapping, MutableSequence
from importlib.resources import files
from typing import Any, AsyncContextManager

import asyncssh
from asyncssh import ChannelOpenError, ConnectionLost, DisconnectError

from streamflow.core import utils
from streamflow.core.data import StreamWrapper
from streamflow.core.deployment import Connector, ExecutionLocation
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.scheduling import AvailableLocation, Hardware, Storage
from streamflow.deployment.connector.base import (
    FS_TYPES_TO_SKIP,
    BaseConnector,
    copy_remote_to_remote,
    copy_same_connector,
)
from streamflow.deployment.stream import StreamReaderWrapper, StreamWriterWrapper
from streamflow.deployment.template import CommandTemplateMap
from streamflow.log_handler import logger


def parse_hostname(hostname: str) -> tuple[str, int]:
    if ":" in hostname:
        hostname, port = hostname.split(":")
        port = int(port)
    else:
        port = 22
    return hostname, port


def get_param_from_file(
    file_path: str | None, workdir: str | None = None
) -> str | None:
    if file_path is None:
        return None
    if not os.path.isabs(file_path):
        file_path = os.path.join(workdir, file_path)
    with open(file_path) as f:
        return f.read().strip()


class SSHContext:
    def __init__(
        self,
        streamflow_config_dir: str,
        config: SSHConfig,
        max_concurrent_sessions: int,
    ):
        self._streamflow_config_dir: str = streamflow_config_dir
        self._config: SSHConfig = config
        self._max_concurrent_sessions: int = max_concurrent_sessions
        self._ssh_connection: asyncssh.SSHClientConnection | None = None
        self._connecting: bool = False
        self._connect_event: asyncio.Event = asyncio.Event()
        self.connection_attempts: int = 0

    async def _get_connection(
        self, config: SSHConfig | None
    ) -> asyncssh.SSHClientConnection | None:
        if config is None:
            return None
        hostname, port = parse_hostname(config.hostname)
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
            passphrase=get_param_from_file(
                config.ssh_key_passphrase_file, self._streamflow_config_dir
            ),
            password=get_param_from_file(
                config.password_file, self._streamflow_config_dir
            ),
            port=port,
            tunnel=await self._get_connection(config.tunnel),
            username=config.username,
            connect_timeout=config.connect_timeout,
        )

    async def close(self) -> None:
        if self._ssh_connection is not None:
            if (
                logger.isEnabledFor(logging.WARNING)
                and len(self._ssh_connection._channels) > 0
            ):
                logger.warning(
                    f"Channels still open after closing the SSH connection to {self.get_hostname()}. Forcing closing."
                )
            self._ssh_connection.close()
            await self._ssh_connection.wait_closed()
            self._ssh_connection = None
        self._connecting = False

    def full(self) -> bool:
        return (
            self._ssh_connection
            and len(self._ssh_connection._channels) >= self._max_concurrent_sessions
        )

    async def get_connection(self) -> asyncssh.SSHClientConnection:
        if self._ssh_connection is None:
            if not self._connecting:
                self._connecting = True
                try:
                    self._ssh_connection = await self._get_connection(self._config)
                except (ConnectionError, asyncssh.Error, asyncio.TimeoutError) as err:
                    await self.close()
                    if isinstance(err, asyncio.TimeoutError):
                        raise asyncio.TimeoutError(
                            f"SSH connection to {self.get_hostname()} failed: connection timed out"
                        )
                    else:
                        raise err
                finally:
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

    async def reset(self) -> None:
        await self.close()
        self.connection_attempts += 1
        self._connect_event.clear()


class SSHContextManager:
    def __init__(
        self,
        condition: asyncio.Condition,
        contexts: MutableSequence[SSHContext],
        command: str,
        environment: MutableMapping[str, str] | None,
        retries: int,
        retry_delay: int,
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
        self._retries: int = retries
        self._retry_delay: int = retry_delay
        self._selected_context: SSHContext | None = None
        self._proc: asyncssh.SSHClientProcess | None = None

    async def __aenter__(self) -> asyncssh.SSHClientProcess:
        async with self._condition:
            available_contexts = self._contexts
            while True:
                if (
                    len(
                        available_contexts := [
                            c
                            for c in available_contexts
                            if c.connection_attempts < self._retries
                        ]
                    )
                    == 0
                ):
                    raise WorkflowExecutionException(
                        f"Hosts {[c.get_hostname() for c in self._contexts]} have no "
                        f"more available contexts: terminating."
                    )
                elif (
                    len(
                        free_contexts := [c for c in available_contexts if not c.full()]
                    )
                    == 0
                ):
                    await self._condition.wait()
                else:
                    for context in free_contexts:
                        try:
                            ssh_connection = await context.get_connection()
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
                            self._selected_context.connection_attempts = 0
                            return self._proc
                        except (
                            ChannelOpenError,
                            ConnectionError,
                            ConnectionLost,
                            DisconnectError,
                            asyncio.TimeoutError,
                        ) as err:
                            if logger.isEnabledFor(logging.WARNING):
                                logger.warning(
                                    f"Error {type(err).__name__} opening SSH session to {context.get_hostname()} "
                                    f"to execute command `{self.command}`: "
                                    f"{f'({err.code}) ' if isinstance(err, asyncssh.Error) else ''}{str(err)}"
                                )
                            if (
                                not isinstance(err, ChannelOpenError)
                                or ssh_connection.is_closed()
                            ):
                                if logger.isEnabledFor(logging.WARNING):
                                    logger.warning(
                                        f"Connection to {context.get_hostname()}: attempt {context.connection_attempts}"
                                    )
                                self._selected_context = None
                                await context.reset()
                    await asyncio.sleep(self._retry_delay)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        async with self._condition:
            if self._selected_context:
                if self._proc:
                    await self._proc.__aexit__(exc_type, exc_val, exc_tb)
                self._condition.notify_all()


class SSHContextFactory:
    def __init__(
        self,
        cls_context: type[SSHContext],
        streamflow_config_dir: str,
        config: SSHConfig,
        max_concurrent_sessions: int,
        max_connections: int,
        retries: int,
        retry_delay: int,
    ):
        self._condition: asyncio.Condition = asyncio.Condition()
        self._contexts: MutableSequence[SSHContext] = [
            cls_context(
                streamflow_config_dir=streamflow_config_dir,
                config=config,
                max_concurrent_sessions=max_concurrent_sessions,
            )
            for _ in range(max_connections)
        ]
        self._retries = retries
        self._retry_delay = retry_delay

    async def close(self) -> None:
        await asyncio.gather(*(asyncio.create_task(c.close()) for c in self._contexts))

    def get(
        self,
        command: str,
        environment: MutableMapping[str, str] | None,
        stdin: int = asyncio.subprocess.PIPE,
        stdout: int = asyncio.subprocess.PIPE,
        stderr: int = asyncio.subprocess.PIPE,
        encoding: str | None = "utf-8",
    ) -> SSHContextManager:
        return SSHContextManager(
            condition=self._condition,
            contexts=self._contexts,
            command=command,
            environment=environment,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            encoding=encoding,
            retries=self._retries,
            retry_delay=self._retry_delay,
        )


class SSHStreamWrapperContextManager(AsyncContextManager[StreamWrapper], ABC):
    def __init__(
        self,
        command: MutableSequence[str],
        environment: MutableMapping[str, str],
        ssh_context_factory: SSHContextFactory,
    ):
        super().__init__()
        self.command: MutableSequence[str] = command
        self.environment: MutableMapping[str, str] = environment
        self.ssh_context_factory: SSHContextFactory = ssh_context_factory
        self.ssh_context: SSHContextManager | None = None
        self.stream: StreamWrapper | None = None

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.stream:
            await self.stream.close()
        await self.ssh_context.__aexit__(exc_type, exc_val, exc_tb)


class SSHStreamReaderWrapperContextManager(SSHStreamWrapperContextManager):
    async def __aenter__(self) -> StreamReaderWrapper:
        self.ssh_context = self.ssh_context_factory.get(
            command=" ".join(self.command),
            environment=self.environment,
            stdin=asyncio.subprocess.DEVNULL,
            encoding=None,
        )
        proc = await self.ssh_context.__aenter__()
        self.stream = StreamReaderWrapper(proc.stdout)
        return self.stream


class SSHStreamWriterWrapperContextManager(SSHStreamWrapperContextManager):
    async def __aenter__(self) -> StreamWriterWrapper:
        self.ssh_context = self.ssh_context_factory.get(
            command=" ".join(self.command),
            environment=self.environment,
            stderr=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            encoding=None,
        )
        proc = await self.ssh_context.__aenter__()
        self.stream = StreamWriterWrapper(proc.stdin)
        return self.stream


class SSHConfig:
    def __init__(
        self,
        check_host_key: bool,
        client_keys: MutableSequence[str],
        connect_timeout: int,
        hostname: str,
        password_file: str | None,
        ssh_key_passphrase_file: str | None,
        tunnel: SSHConfig | None,
        username: str,
    ) -> None:
        self.check_host_key: bool = check_host_key
        self.client_keys: MutableSequence[str] = client_keys
        self.connect_timeout: int = connect_timeout
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
        connectTimeout: int = 30,
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
        self.connectTimeout: int = connectTimeout
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
        self.hardware: MutableMapping[str, Hardware] = {}
        self._cls_context: type[SSHContext] = SSHContext

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
            conn_per_round = min(len(locations), self.maxConcurrentSessions)
            rounds = self.maxConcurrentSessions // conn_per_round
            if len(locations) % conn_per_round != 0:
                rounds += 1
            location_groups = [
                locations[i : i + rounds] for i in range(0, len(locations), rounds)
            ]
            for location_group in location_groups:
                # Perform remote to remote copy
                await copy_remote_to_remote(
                    connector=self,
                    locations=location_group,
                    src=src,
                    dst=dst,
                    source_connector=source_connector,
                    source_location=source_location,
                )

    async def _get_available_location(self, location: str) -> Hardware:
        if location not in self.hardware.keys():
            async with self._get_ssh_client_process(
                location=location,
                command="nproc && "
                "free | grep Mem | awk '{print $2}' && "
                "df -aT | tail -n +2 | awk 'NF == 1 {device = $1; getline; $0 = device $0} {print $7, $2, $5}'",
                stderr=asyncio.subprocess.STDOUT,
            ) as proc:
                result = await proc.wait()
                if result.returncode == 0:
                    cores, memory, *dir_info_list = str(result.stdout.strip()).split(
                        "\n"
                    )
                    try:
                        self.hardware[location] = Hardware(float(cores), float(memory))
                    except ValueError:
                        raise WorkflowExecutionException(
                            f"Impossible to retrieve available hardware for location {self.deployment_name}. "
                            f"An error message occurred: {cores}"
                        ) from None
                    for line in dir_info_list:
                        try:
                            mount_point, fs_type, size = line.split(" ")
                            if fs_type not in FS_TYPES_TO_SKIP:
                                self.hardware[location].storage[mount_point] = Storage(
                                    mount_point=mount_point,
                                    size=float(size) / 2**10,
                                )
                        except ValueError as e:
                            logger.warning(
                                f"Skipping line {line} for deployment {self.deployment_name}: {e}"
                            )
                        except Exception as e:
                            raise WorkflowExecutionException(
                                f"Error parsing {line} for deployment {self.deployment_name}: {e}"
                            )
                else:
                    raise WorkflowExecutionException(result.returncode)
        return self.hardware[location]

    def _get_command(
        self,
        location: ExecutionLocation,
        command: MutableSequence[str],
        environment: MutableMapping[str, str] | None = None,
        workdir: str | None = None,
        stdin: int | str | None = None,
        stdout: int | str = asyncio.subprocess.STDOUT,
        stderr: int | str = asyncio.subprocess.STDOUT,
        job_name: str | None = None,
    ) -> str:
        command_str = utils.create_command(
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
                "EXECUTING command {} on {}{}".format(
                    command_str, location, f" for job {job_name}" if job_name else ""
                )
            )
        return utils.encode_command(command_str)

    def _get_config(
        self, node: str | MutableMapping[str, Any] | None
    ) -> SSHConfig | None:
        if node is None:
            return None
        elif isinstance(node, str):
            node = {"hostname": node}
        ssh_key = node.get("sshKey", self.sshKey)
        return SSHConfig(
            hostname=node["hostname"],
            username=node.get("username", self.username),
            check_host_key=node.get("checkHostKey", self.checkHostKey),
            client_keys=[ssh_key] if ssh_key is not None else [],
            connect_timeout=node.get("connectTimeout", self.connectTimeout),
            password_file=node.get("passwordFile", self.passwordFile),
            ssh_key_passphrase_file=node.get(
                "sshKeyPassphraseFile", self.sshKeyPassphraseFile
            ),
            tunnel=(
                self._get_config(node["tunnel"])
                if "tunnel" in node
                else self.tunnel if hasattr(self, "tunnel") else None
            ),
        )

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
                cls_context=self._cls_context,
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

    def _get_ssh_context_factory(
        self, location: ExecutionLocation
    ) -> SSHContextFactory:
        if self.dataTransferConfig:
            if location not in self.data_transfer_context_factories:
                self.data_transfer_context_factories[location.name] = SSHContextFactory(
                    cls_context=self._cls_context,
                    streamflow_config_dir=self.config_dir,
                    config=self.dataTransferConfig,
                    max_concurrent_sessions=self.maxConcurrentSessions,
                    max_connections=self.maxConnections,
                    retries=self.retries,
                    retry_delay=self.retryDelay,
                )
            return self.data_transfer_context_factories[location.name]
        else:
            if location not in self.ssh_context_factories:
                self.ssh_context_factories[location.name] = SSHContextFactory(
                    cls_context=self._cls_context,
                    streamflow_config_dir=self.config_dir,
                    config=self.nodes[location.name],
                    max_concurrent_sessions=self.maxConcurrentSessions,
                    max_connections=self.maxConnections,
                    retries=self.retries,
                    retry_delay=self.retryDelay,
                )
            return self.ssh_context_factories[location.name]

    async def deploy(self, external: bool) -> None:
        pass

    async def get_available_locations(
        self, service: str | None = None
    ) -> MutableMapping[str, AvailableLocation]:
        locations = {}
        hardware_locations = await asyncio.gather(
            *(
                asyncio.create_task(
                    self._get_available_location(location=location_obj.hostname)
                )
                for location_obj in self.nodes.values()
            )
        )
        for location_obj, hardware in zip(
            self.nodes.values(), hardware_locations, strict=True
        ):
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

    async def get_stream_reader(
        self, command: MutableSequence[str], location: ExecutionLocation
    ) -> AsyncContextManager[StreamWrapper]:
        return SSHStreamReaderWrapperContextManager(
            command=command,
            environment=location.environment,
            ssh_context_factory=self._get_ssh_context_factory(location),
        )

    async def get_stream_writer(
        self, command: MutableSequence[str], location: ExecutionLocation
    ) -> AsyncContextManager[StreamWrapper]:
        return SSHStreamWriterWrapperContextManager(
            command=command,
            environment=location.environment,
            ssh_context_factory=self._get_ssh_context_factory(location),
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
        return (result.stdout.strip(), result.returncode) if capture_output else None

    async def undeploy(self, external: bool) -> None:
        await asyncio.gather(
            *(
                asyncio.create_task(ssh_context.close())
                for ssh_context in self.ssh_context_factories.values()
            )
        )
        self.ssh_context_factories = {}
        await asyncio.gather(
            *(
                asyncio.create_task(ssh_context.close())
                for ssh_context in self.data_transfer_context_factories.values()
            )
        )
        self.data_transfer_context_factories = {}
