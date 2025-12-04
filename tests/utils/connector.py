from __future__ import annotations

import asyncio
import os
from collections.abc import MutableMapping, MutableSequence
from typing import Any, AsyncContextManager, cast

import asyncssh
from asyncssh import SSHClient, SSHClientConnection

from streamflow.core.data import StreamWrapper
from streamflow.core.deployment import Connector, ExecutionLocation
from streamflow.core.scheduling import AvailableLocation, Hardware
from streamflow.deployment.connector import LocalConnector, SSHConnector
from streamflow.deployment.connector.base import BaseConnector
from streamflow.deployment.connector.ssh import (
    SSHConfig,
    SSHContext,
    get_param_from_file,
    parse_hostname,
)
from streamflow.log_handler import logger


import asyncio
import tarfile
from collections.abc import MutableSequence
from contextlib import AbstractAsyncContextManager
from typing import Any, AsyncContextManager

from streamflow.core.data import StreamWrapper
from streamflow.core.deployment import ExecutionLocation


class TarStreamWrapper(StreamWrapper):
    """A StreamWrapper that pipes read/write calls to the underlying tarfile object."""

    def __init__(self, tar_obj: tarfile.TarFile):
        self.tar_obj = tar_obj
        # This will hold the TarInfo/TarFile object for current reading/writing
        self._current_tarinfo: tarfile.TarInfo | None = None
        self._current_member_file: Any | None = None  # File-like object for the member content

    async def close(self) -> None:
        """Close the underlying tarfile object."""
        self.tar_obj.close()

    # NOTE: Read/Write implementations for tar are complex and depend on the
    # specific use case (e.g., streaming file-by-file or reading/writing entire archive).
    # Below is a simplified, non-streaming example to show how tarfile is exposed.

    async def read(self, size: int | None = None) -> bytes:
        """
        Simplified read: extracts a single member if provided its path, or
        reads the next member's content (requires more complex state management).
        For simplicity, this example raises an error.
        A proper implementation would manage reading members one by one.
        """
        raise NotImplementedError("Reading from a tar stream requires complex member iteration logic.")

    async def write(self, data: Any):
        """
        Simplified write: takes a file path or directory and adds it to the tar stream.
        This assumes 'data' is a path.
        """
        if isinstance(data, str) and os.path.exists(data):
            # This is a synchronous blocking operation!
            # In a real async environment, you should run this in an executor.
            await asyncio.to_thread(self.tar_obj.add, data, arcname=os.path.basename(data))
        else:
            raise TypeError("TarStreamWrapper.write expects a valid path string in this simplified example.")


class TarStreamContextManager(AbstractAsyncContextManager[TarStreamWrapper]):
    """
    Async Context Manager to handle opening and closing the tarfile object.
    It takes an existing file-like object (a stream) as its target.
    """

    def __init__(self, target_stream: Any, mode: str):
        self.target_stream = target_stream
        self.mode = mode
        self.tar_obj: tarfile.TarFile | None = None
        self.wrapper: TarStreamWrapper | None = None

    async def __aenter__(self):
        # Open the tarfile object synchronously since it doesn't have an async API
        # but uses the provided async stream as its fileobj.
        # It's crucial that target_stream (e.g., proc.stdin/stdout) is non-blocking.
        self.tar_obj = tarfile.open(
            fileobj=self.target_stream,
            mode=self.mode,  # 'r|' for reading, 'w|' for writing
            format=tarfile.PAX_FORMAT
        )
        self.wrapper = TarStreamWrapper(self.tar_obj)
        return self.wrapper

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.wrapper:
            # Closing the wrapper closes the tarfile, which should flush/finish the stream
            await self.wrapper.close()
        # The target_stream (e.g., proc.stdin/stdout) might also need closing/waiting
        # depending on its nature, but tarfile.close() usually handles the fileobj.


# --- Overridden Connector Implementation ---

class TarConnector(LocalConnector):
    """
    A connector that uses tarfile to inject/extract streams.
    """

    async def get_available_locations(
        self, service: str | None = None
    ) -> MutableMapping[str, AvailableLocation]:
        return {
            "aiotar": AvailableLocation(
                name="aiotar",
                deployment=self.deployment_name,
                service=service,
                hostname="localhost",
                local=False,
                slots=1,
                hardware=None,
            )
        }


    # NOTE: The method signatures must match the base class.

    async def get_stream_reader(
            self,
            command: MutableSequence[str],
            location: ExecutionLocation,
    ) -> AsyncContextManager[TarStreamWrapper]:
        """
        Override: Instead of reading stdout, this will return a context manager
        that reads (untars) from a file on the local filesystem.

        To truly stream, this would need to:
        1. Open a local file stream.
        2. Create a TarStreamContextManager that uses this local file stream.
        """
        # Assuming 'command' contains the path to the tar file to be read
        tar_file_path = command[0] if command else None

        if not tar_file_path or not os.path.isfile(tar_file_path):
            # For a real implementation, you would need an async file reader
            # This example simulates the tar stream context manager:
            raise ValueError("Must provide a path to an existing tar file for reading.")

        # In a real world application, you'd be reading from a remote/local stream.
        # This is simplified to read from an opened file object.
        file_obj = open(tar_file_path, 'rb')

        class TarFileReaderContext(AbstractAsyncContextManager[TarStreamWrapper]):
            async def __aenter__(self):
                self.tar_obj = tarfile.open(fileobj=file_obj, mode='r')
                return TarStreamWrapper(self.tar_obj)

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                self.tar_obj.close()
                file_obj.close()

        # Returning a context manager that manages reading from a local tar file
        return TarFileReaderContext()

    async def get_stream_writer(
            self,
            command: MutableSequence[str],
            location: ExecutionLocation,
    ) -> AsyncContextManager[TarStreamWrapper]:
        """
        Override: Instead of writing to stdin, this returns a context manager
        that writes (tars) to a file on the local filesystem.
        """
        # Assuming 'command' contains the path where the resulting tar file should be saved
        output_file_path = command[0] if command else None

        if not output_file_path:
            raise ValueError("Must provide an output file path for writing the tar stream.")

        # In a real world application, you'd be writing to a remote/local stream.
        # This is simplified to write to an opened file object.
        file_obj = open(output_file_path, 'wb')

        class TarFileWriterContext(AbstractAsyncContextManager[TarStreamWrapper]):
            async def __aenter__(self):
                # 'w' mode for writing to a seekable file object
                self.tar_obj = tarfile.open(fileobj=file_obj, mode='w', format=tarfile.PAX_FORMAT)
                return TarStreamWrapper(self.tar_obj)

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                # Closing the tarfile object is crucial for flushing and writing headers
                self.tar_obj.close()
                file_obj.close()

        # Returning a context manager that manages writing to a local tar file
        return TarFileWriterContext()


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
    ) -> tuple[str, int] | None:
        raise FailureConnectorException("FailureConnector run")

    async def undeploy(self, external: bool) -> None:
        raise FailureConnectorException("FailureConnector undeploy")

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
