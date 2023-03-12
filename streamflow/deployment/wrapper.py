import asyncio
from abc import ABC
from typing import Any, MutableMapping, MutableSequence, Optional, Tuple, Union

from streamflow.core.deployment import Connector, ConnectorCopyKind, Location
from streamflow.core.scheduling import AvailableLocation


class ConnectorWrapper(Connector, ABC):
    def __init__(self, deployment_name: str, config_dir: str, connector: Connector):
        super().__init__(deployment_name, config_dir)
        self.connector: Connector = connector

    async def copy(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[Location],
        kind: ConnectorCopyKind,
        source_connector: Optional[Connector] = None,
        source_location: Optional[Location] = None,
        read_only: bool = False,
    ) -> None:
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
        return None

    async def get_available_locations(
        self,
        service: Optional[str] = None,
        input_directory: Optional[str] = None,
        output_directory: Optional[str] = None,
        tmp_directory: Optional[str] = None,
    ) -> MutableMapping[str, AvailableLocation]:
        return await self.connector.get_available_locations(
            service=service,
            input_directory=input_directory,
            output_directory=output_directory,
            tmp_directory=tmp_directory,
        )

    async def run(
        self,
        location: Location,
        command: MutableSequence[str],
        environment: MutableMapping[str, str] = None,
        workdir: Optional[str] = None,
        stdin: Optional[Union[int, str]] = None,
        stdout: Union[int, str] = asyncio.subprocess.STDOUT,
        stderr: Union[int, str] = asyncio.subprocess.STDOUT,
        capture_output: bool = False,
        timeout: Optional[int] = None,
        job_name: Optional[str] = None,
    ) -> Optional[Tuple[Optional[Any], int]]:
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
