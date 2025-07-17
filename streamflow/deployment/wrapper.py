from __future__ import annotations

import asyncio
from abc import ABC
from collections.abc import MutableMapping, MutableSequence
from typing import AsyncContextManager

from streamflow.core.data import StreamWrapper
from streamflow.core.deployment import Connector, ExecutionLocation
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.scheduling import AvailableLocation
from streamflow.deployment.future import FutureAware


def get_inner_location(location: ExecutionLocation) -> ExecutionLocation:
    if location.wraps is None:
        raise WorkflowExecutionException(
            f"Location {location.name} does not wrap any inner location"
        )
    return location.wraps


def get_inner_locations(
    locations: MutableSequence[ExecutionLocation],
) -> MutableSequence[ExecutionLocation]:
    return list({get_inner_location(loc) for loc in locations})


class ConnectorWrapper(Connector, FutureAware, ABC):
    def __init__(
        self,
        deployment_name: str,
        config_dir: str,
        connector: Connector,
        service: str | None,
        transferBufferSize: int,
    ):
        super().__init__(
            deployment_name=deployment_name,
            config_dir=config_dir,
            transferBufferSize=transferBufferSize,
        )
        self.connector: Connector = connector
        self.service: str | None = service

    async def copy_local_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[ExecutionLocation],
        read_only: bool = False,
    ) -> None:
        await self.connector.copy_local_to_remote(
            src=src,
            dst=dst,
            locations=locations,
            read_only=read_only,
        )

    async def copy_remote_to_local(
        self,
        src: str,
        dst: str,
        location: ExecutionLocation,
        read_only: bool = False,
    ) -> None:
        await self.connector.copy_remote_to_local(
            src=src,
            dst=dst,
            location=location,
            read_only=read_only,
        )

    async def copy_remote_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[ExecutionLocation],
        source_location: ExecutionLocation,
        source_connector: Connector | None = None,
        read_only: bool = False,
    ) -> None:
        await self.connector.copy_remote_to_remote(
            src=src,
            dst=dst,
            locations=locations,
            source_location=source_location,
            source_connector=source_connector,
            read_only=read_only,
        )

    async def deploy(self, external: bool) -> None:
        return None

    async def get_available_locations(
        self, service: str | None = None
    ) -> MutableMapping[str, AvailableLocation]:
        return await self.connector.get_available_locations(service=service)

    async def get_stream_reader(
        self, command: MutableSequence[str], location: ExecutionLocation
    ) -> AsyncContextManager[StreamWrapper]:
        return await self.connector.get_stream_reader(command, location)

    async def get_stream_writer(
        self, command: MutableSequence[str], location: ExecutionLocation
    ) -> AsyncContextManager[StreamWrapper]:
        return await self.connector.get_stream_writer(command, location)

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
        return await self.connector.run(
            location=location,
            command=command,
            environment=environment,
            workdir=workdir,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            capture_output=capture_output,
            timeout=timeout,
            job_name=job_name,
        )

    async def undeploy(self, external: bool) -> None:
        return None
