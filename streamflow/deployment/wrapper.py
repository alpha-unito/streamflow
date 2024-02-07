from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Any, MutableMapping, MutableSequence

from streamflow.core.data import StreamWrapperContextManager
from streamflow.core.deployment import Connector, Location
from streamflow.core.scheduling import AvailableLocation
from streamflow.deployment.future import FutureAware


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

    async def _get_inner_location(self, location: Location) -> Location:
        return next(iter(await self._get_inner_locations([location])))

    @abstractmethod
    async def _get_inner_locations(
        self, locations: MutableSequence[Location]
    ) -> MutableSequence[Location]: ...

    async def copy_local_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[Location],
        read_only: bool = False,
    ) -> None:
        await self.connector.copy_local_to_remote(
            src=src,
            dst=dst,
            locations=await self._get_inner_locations(locations),
            read_only=read_only,
        )

    async def copy_remote_to_local(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[Location],
        read_only: bool = False,
    ) -> None:
        await self.connector.copy_remote_to_local(
            src=src,
            dst=dst,
            locations=await self._get_inner_locations(locations),
            read_only=read_only,
        )

    async def copy_remote_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[Location],
        source_location: Location,
        source_connector: Connector | None = None,
        read_only: bool = False,
    ) -> None:
        await self.connector.copy_remote_to_remote(
            src=src,
            dst=dst,
            locations=await self._get_inner_locations(locations),
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
        self, location: Location, src: str
    ) -> StreamWrapperContextManager:
        return await self.connector.get_stream_reader(
            await self._get_inner_location(location), src
        )

    async def run(
        self,
        location: Location,
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
            location=await self._get_inner_location(location),
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
