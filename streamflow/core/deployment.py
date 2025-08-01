from __future__ import annotations

import asyncio
import os
import posixpath
import tempfile
from abc import abstractmethod
from collections.abc import MutableMapping, MutableSequence
from typing import TYPE_CHECKING, AsyncContextManager, cast

from typing_extensions import Self

from streamflow.core import utils
from streamflow.core.config import Config
from streamflow.core.context import SchemaEntity
from streamflow.core.persistence import DatabaseLoadingContext, PersistableEntity

if TYPE_CHECKING:
    from typing import Any

    from streamflow.core.context import StreamFlowContext
    from streamflow.core.data import StreamWrapper
    from streamflow.core.scheduling import AvailableLocation
    from streamflow.core.workflow import Job


class ExecutionLocation:
    __slots__ = (
        "deployment",
        "environment",
        "hostname",
        "local",
        "mounts",
        "name",
        "service",
        "stacked",
        "wraps",
    )

    def __init__(
        self,
        name: str,
        deployment: str,
        environment: MutableMapping[str, str] | None = None,
        hostname: str | None = None,
        local: bool = False,
        mounts: MutableMapping[str, str] | None = None,
        service: str | None = None,
        stacked: bool = False,
        wraps: ExecutionLocation | None = None,
    ):
        self.deployment: str = deployment
        self.environment: MutableMapping[str, str] = environment or {}
        self.hostname: str | None = hostname
        self.local: bool = local
        self.mounts: MutableMapping[str, str] = mounts or {}
        self.name: str = name
        self.service: str | None = service
        self.stacked: bool = stacked
        self.wraps: ExecutionLocation | None = wraps

    def __str__(self) -> str:
        if self.service:
            return posixpath.join(self.deployment, self.service, self.name)
        else:
            return posixpath.join(self.deployment, self.name)


class BindingFilter(SchemaEntity):
    @abstractmethod
    async def get_targets(
        self, job: Job, targets: MutableSequence[Target]
    ) -> MutableSequence[Target]: ...


class Connector(SchemaEntity):
    def __init__(self, deployment_name: str, config_dir: str, transferBufferSize: int):
        self.deployment_name: str = deployment_name
        self.config_dir: str = config_dir
        self.transferBufferSize: int = transferBufferSize

    @abstractmethod
    async def copy_local_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[ExecutionLocation],
        read_only: bool = False,
    ) -> None: ...

    @abstractmethod
    async def copy_remote_to_local(
        self,
        src: str,
        dst: str,
        location: ExecutionLocation,
        read_only: bool = False,
    ) -> None: ...

    @abstractmethod
    async def copy_remote_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[ExecutionLocation],
        source_location: ExecutionLocation,
        source_connector: Connector | None = None,
        read_only: bool = False,
    ) -> None: ...

    @abstractmethod
    async def deploy(self, external: bool) -> None: ...

    @abstractmethod
    async def get_available_locations(
        self, service: str | None = None
    ) -> MutableMapping[str, AvailableLocation]: ...

    @abstractmethod
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
    ) -> tuple[str, int] | None: ...

    @abstractmethod
    async def undeploy(self, external: bool) -> None: ...

    @abstractmethod
    async def get_stream_reader(
        self, command: MutableSequence[str], location: ExecutionLocation
    ) -> AsyncContextManager[StreamWrapper]: ...

    @abstractmethod
    async def get_stream_writer(
        self, command: MutableSequence[str], location: ExecutionLocation
    ) -> AsyncContextManager[StreamWrapper]: ...


class DeploymentManager(SchemaEntity):
    def __init__(self, context: StreamFlowContext) -> None:
        self.context: StreamFlowContext = context

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    async def deploy(self, deployment_config: DeploymentConfig) -> None: ...

    @abstractmethod
    def get_connector(self, deployment_name: str) -> Connector | None: ...

    @abstractmethod
    async def undeploy(self, deployment_name: str) -> None: ...

    @abstractmethod
    async def undeploy_all(self) -> None: ...


class DeploymentConfig(PersistableEntity):
    __slots__ = (
        "name",
        "type",
        "config",
        "external",
        "lazy",
        "scheduling_policy",
        "workdir",
        "wraps",
    )

    def __init__(
        self,
        name: str,
        type: str,
        config: MutableMapping[str, Any],
        external: bool = False,
        lazy: bool = True,
        scheduling_policy: Config | None = None,
        workdir: str | None = None,
        wraps: WrapsConfig | None = None,
    ) -> None:
        super().__init__()
        self.name: str = name
        self.type: str = type
        self.config: MutableMapping[str, Any] = config or {}
        self.external: bool = external
        self.lazy: bool = lazy
        self.scheduling_policy: Config = scheduling_policy or Config(
            name="__DEFAULT__", type="data_locality", config={}
        )
        self.workdir: str | None = workdir
        self.wraps: WrapsConfig | None = wraps

    @classmethod
    async def load(
        cls,
        context: StreamFlowContext,
        persistent_id: int,
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        row = await context.database.get_deployment(persistent_id)
        obj = cls(
            name=row["name"],
            type=row["type"],
            config=row["config"],
            external=row["external"],
            lazy=row["lazy"],
            scheduling_policy=await Config.load(
                context=context,
                row=row["scheduling_policy"],
                loading_context=loading_context,
            ),
            workdir=row["workdir"],
            wraps=(
                await WrapsConfig.load(
                    context=context,
                    row=row["wraps"],
                    loading_context=loading_context,
                )
                if row["wraps"] is not None
                else None
            ),
        )
        loading_context.add_deployment(persistent_id, obj)
        return obj

    async def save(self, context: StreamFlowContext) -> None:
        async with self.persistence_lock:
            if not self.persistent_id:
                self.persistent_id = await context.database.add_deployment(
                    name=self.name,
                    type=self.type,
                    config=self.config,
                    external=self.external,
                    lazy=self.lazy,
                    scheduling_policy=await self.scheduling_policy.save(context),
                    workdir=self.workdir,
                    wraps=(
                        await self.wraps.save(context)
                        if self.wraps is not None
                        else None
                    ),
                )


class Target(PersistableEntity):
    def __init__(
        self,
        deployment: DeploymentConfig,
        locations: int = 1,
        service: str | None = None,
        workdir: str | None = None,
    ):
        super().__init__()
        self.deployment: DeploymentConfig = deployment
        self.locations: int = locations
        self.service: str | None = service
        self.workdir: str = (
            workdir
            or self.deployment.workdir
            or (
                os.path.join(os.path.realpath(tempfile.gettempdir()), "streamflow")
                if deployment.type == "local"
                else posixpath.join("/tmp", "streamflow")  # nosec
            )
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return {}

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        return cls(
            deployment=await DeploymentConfig.load(
                context, row["deployment"], loading_context
            ),
            locations=row["locations"],
            service=row["service"],
            workdir=row["workdir"],
        )

    @classmethod
    async def load(
        cls,
        context: StreamFlowContext,
        persistent_id: int,
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        row = await context.database.get_target(persistent_id)
        type_ = cast(Self, utils.get_class_from_name(row["type"]))
        obj = await type_._load(context, row, loading_context)
        loading_context.add_target(persistent_id, obj)
        return obj

    async def save(self, context: StreamFlowContext) -> None:
        await self.deployment.save(context)
        async with self.persistence_lock:
            if not self.persistent_id:
                self.persistent_id = await context.database.add_target(
                    deployment=self.deployment.persistent_id,
                    type=type(self),
                    params=await self._save_additional_params(context),
                    locations=self.locations,
                    service=self.service,
                    workdir=self.workdir,
                )


class LocalTarget(Target):
    deployment_name = "__LOCAL__"
    __deployment_config = None

    def __init__(self, workdir: str | None = None):
        super().__init__(
            deployment=self._get_deployment_config(), locations=1, workdir=workdir
        )

    @classmethod
    def _get_deployment_config(cls) -> DeploymentConfig:
        if cls.__deployment_config is None:
            cls.__deployment_config = DeploymentConfig(
                name=cls.deployment_name,
                type="local",
                config={},
                external=True,
                lazy=False,
            )
        return cls.__deployment_config

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        return cls(workdir=row["workdir"])


class FilterConfig(PersistableEntity):
    __slots__ = ("name", "type", "config")

    def __init__(self, name: str, type: str, config: MutableMapping[str, Any]):
        super().__init__()
        self.name: str = name
        self.type: str = type
        self.config: MutableMapping[str, Any] = config or {}

    @classmethod
    async def load(
        cls,
        context: StreamFlowContext,
        persistent_id: int,
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        row = await context.database.get_filter(persistent_id)
        obj = cls(
            name=row["name"],
            type=row["type"],
            config=row["config"],
        )
        loading_context.add_filter(persistent_id, obj)
        return obj

    async def save(self, context: StreamFlowContext) -> None:
        async with self.persistence_lock:
            if not self.persistent_id:
                self.persistent_id = await context.database.add_filter(
                    name=self.name,
                    type=self.type,
                    config=self.config,
                )


class WrapsConfig:
    def __init__(self, deployment: str, service: str | None = None):
        self.deployment: str = deployment
        self.service: str | None = service

    @classmethod
    async def load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        return cls(deployment=row["deployment"], service=row.get("service"))

    async def save(self, context: StreamFlowContext):
        row = {"deployment": self.deployment}
        if self.service is not None:
            row["service"] = self.service
        return row
