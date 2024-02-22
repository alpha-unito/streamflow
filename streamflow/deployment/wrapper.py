from __future__ import annotations

import asyncio
from abc import ABC
from typing import Any, MutableMapping, MutableSequence

from streamflow.core.data import StreamWrapperContextManager
from streamflow.core.deployment import Connector, ExecutionLocation
from streamflow.core.scheduling import AvailableLocation
from streamflow.deployment.future import FutureAware
from streamflow.deployment.utils import get_inner_location, get_inner_locations


class ConnectorWrapper(Connector, FutureAware, ABC):
    def __init__(
        self,
        deployment_name: str,
        config_dir: str,
        connector: Connector,
        service: str | None,
        transferBufferSize: int,
    ):
        super().__init__(deployment_name, config_dir, transferBufferSize)
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
            locations=get_inner_locations(locations),
            read_only=read_only,
        )

    async def copy_remote_to_local(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[ExecutionLocation],
        read_only: bool = False,
    ) -> None:
        await self.connector.copy_remote_to_local(
            src=src,
            dst=dst,
            locations=get_inner_locations(locations),
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
            locations=get_inner_locations(locations),
            source_location=source_location,
            source_connector=source_connector,
            read_only=read_only,
        )

    async def deploy(self, external: bool) -> None:
        return None

    async def get_available_locations(
        self,
        service: str | None = None,
        input_directory: str | None = None,
        output_directory: str | None = None,
        tmp_directory: str | None = None,
    ) -> MutableMapping[str, AvailableLocation]:
        return await self.connector.get_available_locations(
            service=service,
            input_directory=input_directory,
            output_directory=output_directory,
            tmp_directory=tmp_directory,
        )

    async def get_stream_reader(
        self, location: ExecutionLocation, src: str
    ) -> StreamWrapperContextManager:
        return await self.connector.get_stream_reader(get_inner_location(location), src)

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
        return await self.connector.run(
            location=get_inner_location(location),
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
