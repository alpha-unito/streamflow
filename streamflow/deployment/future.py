from __future__ import annotations

import asyncio
import logging
from abc import ABCMeta
from collections.abc import MutableMapping, MutableSequence
from typing import Any, AsyncContextManager

from streamflow.core.data import StreamWrapper
from streamflow.core.deployment import Connector, ExecutionLocation
from streamflow.core.exception import WorkflowExecutionException
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
        super().__init__(
            deployment_name=name,
            config_dir=config_dir,
            transferBufferSize=kwargs.pop("transferBufferSize", 2**16),
        )
        self.type: type[Connector] = connector_type
        self.external: bool = external
        self.parameters: MutableMapping[str, Any] = kwargs
        self.deploying: bool = False
        self.deploy_event: asyncio.Event = asyncio.Event()
        self._connector: Connector | None = None

    async def _safe_deploy_event_wait(self):
        await self.deploy_event.wait()
        if self._connector is None:
            raise WorkflowExecutionException(
                f"FAILED deployment of {self.deployment_name}"
            )

    @property
    def connector(self):
        if hasattr(self._connector, "connector"):
            return self._connector.connector
        else:
            raise WorkflowExecutionException(
                f"Trying to access inner connector for instance {self._connector.deployment_name}, "
                "which is not a `ConnectorWrapper` instance."
            )

    async def copy_local_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[ExecutionLocation],
        read_only: bool = False,
    ) -> None:
        if self._connector is None:
            if not self.deploying:
                self.deploying = True
                await self.deploy(self.external)
            else:
                await self._safe_deploy_event_wait()
        await self._connector.copy_local_to_remote(
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
        if self._connector is None:
            if not self.deploying:
                self.deploying = True
                await self.deploy(self.external)
            else:
                await self._safe_deploy_event_wait()
        await self._connector.copy_remote_to_local(
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
        if self._connector is None:
            if not self.deploying:
                self.deploying = True
                await self.deploy(self.external)
            else:
                await self._safe_deploy_event_wait()
        if isinstance(source_connector, FutureConnector):
            source_connector = source_connector._connector
        await self._connector.copy_remote_to_remote(
            src=src,
            dst=dst,
            locations=locations,
            source_location=source_location,
            source_connector=source_connector,
            read_only=read_only,
        )

    async def deploy(self, external: bool) -> None:
        # noinspection PyArgumentList
        connector = self.type(
            deployment_name=self.deployment_name,
            config_dir=self.config_dir,
            transferBufferSize=self.transferBufferSize,
            **self.parameters,
        )
        if logger.isEnabledFor(logging.INFO):
            if not external:
                logger.info(f"DEPLOYING {self.deployment_name}")
        try:
            await connector.deploy(external)
        except Exception:
            self._connector = None
            self.deploy_event.set()
            raise
        if logger.isEnabledFor(logging.INFO):
            if not external:
                logger.info(f"COMPLETED deployment of {self.deployment_name}")
        self._connector = connector
        self.deploy_event.set()

    async def get_available_locations(
        self, service: str | None = None
    ) -> MutableMapping[str, AvailableLocation]:
        if self._connector is None:
            if not self.deploying:
                self.deploying = True
                await self.deploy(self.external)
            else:
                await self._safe_deploy_event_wait()
        return await self._connector.get_available_locations(service=service)

    async def get_stream_reader(
        self, command: MutableSequence[str], location: ExecutionLocation
    ) -> AsyncContextManager[StreamWrapper]:
        if self._connector is None:
            if not self.deploying:
                self.deploying = True
                await self.deploy(self.external)
            else:
                await self._safe_deploy_event_wait()
        return await self._connector.get_stream_reader(command, location)

    async def get_stream_writer(
        self, command: MutableSequence[str], location: ExecutionLocation
    ) -> AsyncContextManager[StreamWrapper]:
        if self._connector is None:
            if not self.deploying:
                self.deploying = True
                await self.deploy(self.external)
            else:
                await self._safe_deploy_event_wait()
        return await self._connector.get_stream_writer(command, location)

    def get_schema(self) -> str:
        return self.type.get_schema()

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
        if self._connector is None:
            if not self.deploying:
                self.deploying = True
                await self.deploy(self.external)
            else:
                await self._safe_deploy_event_wait()
        return await self._connector.run(
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
        if self._connector is not None:
            await self._connector.undeploy(external)


class FutureMeta(ABCMeta):
    def __instancecheck__(self, instance):
        if isinstance(instance, FutureConnector):
            return super().__subclasscheck__(instance.type)
        else:
            return super().__instancecheck__(instance)


class FutureAware(metaclass=FutureMeta):
    __slots__ = ()
