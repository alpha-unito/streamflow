from __future__ import annotations

import asyncio
from collections.abc import MutableMapping, MutableSequence
from typing import Any, cast

import asyncssh
from asyncssh import SSHClient, SSHClientConnection

from streamflow.core.data import StreamWrapperContextManager
from streamflow.core.deployment import Connector, ExecutionLocation
from streamflow.core.scheduling import AvailableLocation, Hardware
from streamflow.deployment.connector import LocalConnector, SSHConnector
from streamflow.deployment.connector.ssh import (
    SSHConfig,
    SSHContext,
    get_param_from_file,
    parse_hostname,
)
from streamflow.log_handler import logger


class FailureConnectorException(Exception):
    pass


class FailureConnector(Connector):
    @classmethod
    def get_schema(cls) -> str:
        pass

    async def copy_local_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[ExecutionLocation],
        read_only: bool = False,
    ) -> None:
        raise FailureConnectorException("FailureConnector copy_local_to_remote")

    async def copy_remote_to_local(
        self,
        src: str,
        dst: str,
        location: ExecutionLocation,
        read_only: bool = False,
    ) -> None:
        raise FailureConnectorException("FailureConnector copy_remote_to_local")

    async def copy_remote_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[ExecutionLocation],
        source_location: ExecutionLocation,
        source_connector: Connector | None = None,
        read_only: bool = False,
    ) -> None:
        raise FailureConnectorException("FailureConnector copy_remote_to_remote")

    async def deploy(self, external: bool) -> None:
        await asyncio.sleep(5)
        logger.info("FailureConnector deploy")
        raise FailureConnectorException("FailureConnector deploy")

    async def get_available_locations(
        self, service: str | None = None
    ) -> MutableMapping[str, AvailableLocation]:
        raise FailureConnectorException("FailureConnector get_available_locations")

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
        raise FailureConnectorException("FailureConnector run")

    async def undeploy(self, external: bool) -> None:
        raise FailureConnectorException("FailureConnector undeploy")

    async def get_stream_reader(
        self, command: MutableSequence[str], location: ExecutionLocation
    ) -> StreamWrapperContextManager:
        raise FailureConnectorException("FailureConnector get_stream_reader")

    async def get_stream_writer(
        self, command: MutableSequence[str], location: ExecutionLocation
    ) -> StreamWrapperContextManager:
        raise FailureConnectorException("FailureConnector get_stream_writer")


class ParameterizableHardwareConnector(LocalConnector):
    def __init__(
        self, deployment_name: str, config_dir: str, transferBufferSize: int = 2**16
    ):
        super().__init__(deployment_name, config_dir, transferBufferSize)
        self.hardware = None

    def set_hardware(self, hardware: Hardware):
        self.hardware = hardware

    async def get_available_locations(
        self, service: str | None = None
    ) -> MutableMapping[str, AvailableLocation]:
        return {
            "custom-hardware": AvailableLocation(
                name="custom-hardware",
                deployment=self.deployment_name,
                service=service,
                hostname="localhost",
                slots=1,
                hardware=self.hardware,
            )
        }


class SSHChannelErrorClient(SSHClient):
    def __init__(self, connector: SSHChannelErrorConnector) -> None:
        self.connector = connector

    def connection_made(self, conn: SSHClientConnection) -> None:
        if self.connector.counter_err == 0:
            conn.close()
            self.connector.counter_err += 1


class SSHChannelErrorContext(SSHContext):
    async def _get_connection(
        self, config: SSHConfig | None
    ) -> asyncssh.SSHClientConnection | None:
        config = cast(SSHChannelErrorConfig, config)
        if config is None:
            return None
        (hostname, port) = parse_hostname(config.hostname)

        def get_client_factory():
            return SSHChannelErrorClient(config.connector)

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
            client_factory=get_client_factory,
        )


class SSHChannelErrorConfig(SSHConfig):
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
        connector: SSHChannelErrorConnector,
    ) -> None:
        super().__init__(
            check_host_key=check_host_key,
            client_keys=client_keys,
            connect_timeout=connect_timeout,
            hostname=hostname,
            password_file=password_file,
            ssh_key_passphrase_file=ssh_key_passphrase_file,
            tunnel=tunnel,
            username=username,
        )
        self.connector = connector


class SSHChannelErrorConnector(SSHConnector):
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
    ):
        self.counter_err = 0
        super().__init__(
            deployment_name=deployment_name,
            config_dir=config_dir,
            nodes=nodes,
            username=username,
            checkHostKey=checkHostKey,
            dataTransferConnection=dataTransferConnection,
            file=file,
            maxConcurrentSessions=maxConcurrentSessions,
            maxConnections=maxConnections,
            retries=retries,
            retryDelay=retryDelay,
            passwordFile=passwordFile,
            services=services,
            sharedPaths=sharedPaths,
            sshKey=sshKey,
            sshKeyPassphraseFile=sshKeyPassphraseFile,
            tunnel=tunnel,
            transferBufferSize=transferBufferSize,
        )
        self._cls_context: type[SSHContext] = SSHChannelErrorContext

    def _get_config(self, node: str | MutableMapping[str, Any]) -> SSHConfig | None:
        if config := super()._get_config(node):
            config = SSHChannelErrorConfig(
                check_host_key=config.check_host_key,
                client_keys=config.client_keys,
                connect_timeout=config.connect_timeout,
                connector=self,
                hostname=config.hostname,
                password_file=config.password_file,
                ssh_key_passphrase_file=config.ssh_key_passphrase_file,
                tunnel=config.tunnel,
                username=config.username,
            )
        return config
