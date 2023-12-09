from __future__ import annotations

import asyncio
from collections.abc import MutableMapping, MutableSequence
from typing import Any

from streamflow.core.data import StreamWrapperContextManager
from streamflow.core.deployment import Connector, ExecutionLocation
from streamflow.core.scheduling import AvailableLocation, Hardware
from streamflow.deployment.connector import LocalConnector
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
