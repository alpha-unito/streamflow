from __future__ import annotations

import asyncio
import logging
from typing import Any, MutableMapping, MutableSequence

from streamflow.core.deployment import Connector, ConnectorCopyKind, Location
from streamflow.core.scheduling import AvailableLocation
from streamflow.log_handler import logger


class FutureConnector(Connector):
    def __init__(
        self,
        name: str,
        config_dir: str,
        connector_type: type[Connector],
        external: bool,
        **kwargs,
    ):
        super().__init__(name, config_dir)
        self.type: type[Connector] = connector_type
        self.external: bool = external
        self.parameters: MutableMapping[str, Any] = kwargs
        self.deploying: bool = False
        self.deploy_event: asyncio.Event = asyncio.Event()
        self.connector: Connector | None = None

    async def copy(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[Location],
        kind: ConnectorCopyKind,
        source_connector: Connector | None = None,
        source_location: str | None = None,
        read_only: bool = False,
    ) -> None:
        if self.connector is None:
            if not self.deploying:
                self.deploying = True
                await self.deploy(self.external)
            else:
                await self.deploy_event.wait()
        if isinstance(source_connector, FutureConnector):
            source_connector = source_connector.connector
        await self.connector.copy(
            src=src,
            dst=dst,
            locations=locations,
            kind=kind,
            source_connector=source_connector,
            source_location=source_location,
            read_only=read_only,
        )

    async def deploy(self, external: bool) -> None:
        # noinspection PyArgumentList
        connector = self.type(self.deployment_name, self.config_dir, **self.parameters)
        if logger.isEnabledFor(logging.INFO):
            if not external:
                logger.info(f"DEPLOYING {self.deployment_name}")
        await connector.deploy(external)
        if logger.isEnabledFor(logging.INFO):
            if not external:
                logger.info(f"COMPLETED Deployment of {self.deployment_name}")
        self.connector = connector
        self.deploy_event.set()

    async def get_available_locations(
        self,
        service: str | None = None,
        input_directory: str | None = None,
        output_directory: str | None = None,
        tmp_directory: str | None = None,
    ) -> MutableMapping[str, AvailableLocation]:
        if self.connector is None:
            if not self.deploying:
                self.deploying = True
                await self.deploy(self.external)
            else:
                await self.deploy_event.wait()
        return await self.connector.get_available_locations(
            service=service,
            input_directory=input_directory,
            output_directory=output_directory,
            tmp_directory=tmp_directory,
        )

    def get_schema(self) -> str:
        return self.type.get_schema()

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
        if self.connector is None:
            if not self.deploying:
                self.deploying = True
                await self.deploy(self.external)
            else:
                await self.deploy_event.wait()
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
        if self.connector is not None:
            await self.connector.undeploy(external)
