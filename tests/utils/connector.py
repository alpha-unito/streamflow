from __future__ import annotations

import asyncio
import json
import os
import tarfile
from collections.abc import MutableMapping, MutableSequence
from types import TracebackType
from typing import Any, AsyncContextManager, Literal, cast

import asyncssh
from asyncssh import SSHClient, SSHClientConnection

from streamflow.core.data import StreamWrapper
from streamflow.core.deployment import Connector, ExecutionLocation, Shell
from streamflow.core.scheduling import AvailableLocation, Hardware
from streamflow.deployment.connector import LocalConnector, SSHConnector
from streamflow.deployment.connector.base import BaseConnector
from streamflow.deployment.connector.ssh import (
    SSHConfig,
    SSHContext,
    get_param_from_file,
    parse_hostname,
)
from streamflow.deployment.stream import StreamReaderWrapper, StreamWriterWrapper
from streamflow.log_handler import logger


def _get_path_from_cmd(command: MutableSequence[str]) -> str:
    match command[0]:
        case "tee":
            return command[1]
        case "tar":
            if command[1] == "chf" and len(command) == 6:
                return str(os.path.join(*command[-2:]))
            else:
                raise NotImplementedError(command)
        case _:
            raise NotImplementedError(command)


class AioTarStreamReaderWrapper(StreamReaderWrapper):
    async def close(self) -> None:
        if self.stream:
            os.close(self.stream)
            self.stream = None

    async def read(self, size: int | None = None) -> bytes:
        return os.read(self.stream, size)


class AioTarStreamWriterWrapper(StreamWriterWrapper):
    async def close(self) -> None:
        self.stream.close()

    async def write(self, data: Any) -> None:
        self.stream.write(data)


class AioTarStreamWrapperContextManager(AsyncContextManager[StreamWrapper]):
    def __init__(
        self, fileobj: Any, mode: Literal["r", "a", "w", "x"], tar_format: str
    ) -> None:
        super().__init__()
        self.fileobj: Any = fileobj
        self.stream: StreamWrapper | None = None
        self.mode: Literal["r", "a", "w", "x"] = mode
        # TODO: add type (in particular to test `sparse`)
        # FORMATS:
        # - gnu           GNU tar 1.13.x format.
        # - oldgnu        GNU format as per tar <= 1.12.
        # - pax, posix    POSIX 1003.1 - 2001 (pax) format.
        # - ustar         POSIX 1003.1 - 1988 (ustar) format.
        # - v7            Old V7 tar format.
        match tar_format:
            case "gnu":
                self.tar_format: int = tarfile.GNU_FORMAT
            case "pax" | "posix":
                self.tar_format: int = tarfile.PAX_FORMAT
            case "ustar" | "v7":
                self.tar_format: int = tarfile.USTAR_FORMAT
            case _:
                raise ValueError(f"Unknown format: {tar_format}")

    async def __aenter__(self) -> StreamWrapper:
        if self.mode == "w":
            self.stream = AioTarStreamWriterWrapper(self.fileobj)
        else:
            read_fd, write_fd = os.pipe()
            with os.fdopen(write_fd, "wb") as f:
                with tarfile.open(fileobj=f, mode="w|", dereference=True) as tar:
                    tar.add(self.fileobj, arcname=os.path.basename(self.fileobj))
            self.stream = AioTarStreamReaderWrapper(read_fd)
        return self.stream

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self.stream:
            await self.stream.close()
        if not isinstance(self.fileobj, str):
            self.fileobj.close()


class AioTarConnector(BaseConnector):
    def __init__(
        self,
        deployment_name: str,
        config_dir: str,
        tar_format: str,
        transferBufferSize: int = 64,
    ) -> None:
        super().__init__(
            deployment_name=deployment_name,
            config_dir=config_dir,
            transferBufferSize=transferBufferSize,
        )
        self.tar_format: str = tar_format

    async def deploy(self, external: bool) -> None:
        pass

    async def get_available_locations(
        self, service: str | None = None
    ) -> MutableMapping[str, AvailableLocation]:
        return {
            f"aiotar-{self.tar_format}": AvailableLocation(
                name=f"aiotar-{self.tar_format}",
                deployment=self.deployment_name,
                service=service,
                hostname="localhost",
                local=False,  # to simulate remote location
                slots=1,
                hardware=None,
            )
        }

    async def get_shell(
        self, command: MutableSequence[str], location: ExecutionLocation
    ) -> Shell:
        raise NotImplementedError("AioTarConnector get_shell")

    @classmethod
    def get_schema(cls) -> str:
        return json.dumps(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$id": "https://streamflow.di.unito.it/schemas/tests/utils/connector/aiotar_connector.json",
                "type": "object",
                "properties": {
                    "tar_format": {"type": "string", "description": "Tar format"}
                },
                "additionalProperties": False,
            }
        )

    async def get_stream_reader(
        self, command: MutableSequence[str], location: ExecutionLocation
    ) -> AsyncContextManager[StreamWrapper]:
        path = _get_path_from_cmd(command)
        return AioTarStreamWrapperContextManager(
            fileobj=path, mode="r", tar_format=self.tar_format
        )

    async def get_stream_writer(
        self, command: MutableSequence[str], location: ExecutionLocation
    ) -> AsyncContextManager[StreamWrapper]:
        path = _get_path_from_cmd(command)
        return AioTarStreamWrapperContextManager(
            fileobj=open(path, "wb"), mode="w", tar_format=self.tar_format
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
        # The connector simulates a remote location, but it manipulates the files
        # on the local filesystem.
        # However, the remote location assumed by StreamFlow must be a Linux OS,
        # because of the system commands that are executed.
        # For example, all commands within RemoteStreamFlowPath are Linux-specific.
        # Executing these commands on Mac or Windows will lead to errors.
        raise NotImplementedError("AioTarConnector run")

    async def undeploy(self, external: bool) -> None:
        pass


class FailureConnectorException(Exception):
    pass


class FailureConnector(Connector):
    @classmethod
    def get_schema(cls) -> str:
        return json.dumps(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$id": "https://streamflow.di.unito.it/schemas/tests/utils/connector/failure_connector.json",
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            }
        )

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
    ) -> tuple[str, int] | None:
        raise FailureConnectorException("FailureConnector run")

    async def undeploy(self, external: bool) -> None:
        raise FailureConnectorException("FailureConnector undeploy")

    async def get_shell(
        self, command: MutableSequence[str], location: ExecutionLocation
    ):
        raise FailureConnectorException("FailureConnector get_shell")

    async def get_stream_reader(
        self, command: MutableSequence[str], location: ExecutionLocation
    ) -> AsyncContextManager[StreamWrapper]:
        raise FailureConnectorException("FailureConnector get_stream_reader")

    async def get_stream_writer(
        self, command: MutableSequence[str], location: ExecutionLocation
    ) -> AsyncContextManager[StreamWrapper]:
        raise FailureConnectorException("FailureConnector get_stream_writer")


class ParameterizableHardwareConnector(LocalConnector):
    def __init__(
        self, deployment_name: str, config_dir: str, transferBufferSize: int = 2**16
    ) -> None:
        super().__init__(deployment_name, config_dir, transferBufferSize)
        self.hardware = None

    def set_hardware(self, hardware: Hardware) -> None:
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
        hostname, port = parse_hostname(config.hostname)

        def get_client_factory() -> SSHChannelErrorClient:
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
