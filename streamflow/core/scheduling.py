from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Any, TYPE_CHECKING, Type, cast

from streamflow.core import utils
from streamflow.core.config import BindingConfig, Config
from streamflow.core.context import SchemaEntity, StreamFlowContext
from streamflow.core.deployment import Connector, ExecutionLocation, Target
from streamflow.core.persistence import DatabaseLoadingContext

if TYPE_CHECKING:
    from streamflow.core.workflow import Job, Status, Token
    from typing import MutableSequence, MutableMapping


class DeploymentHardware:
    def __init__(
        self,
        cores: float,
        memory: float,
        storage: MutableMapping[str, float],
    ):
        self.cores: float = cores
        self.memory: float = memory
        self.storage: MutableMapping[str, float] = storage


class JobHardwareRequirement:
    def __init__(
        self, cores: float, memory: float, tmp_dir_size: float, out_dir_size: float
    ):
        self.cores = cores
        self.memory = memory
        self.tmp_dir_size = tmp_dir_size
        self.out_dir_size = out_dir_size
        # todo: add timeout


class HardwareRequirement(ABC):
    @classmethod
    @abstractmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ): ...

    @abstractmethod
    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]: ...

    @abstractmethod
    def eval(self, inputs: MutableMapping[str, Token]) -> JobHardwareRequirement: ...

    @classmethod
    async def load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ):
        type = utils.get_class_from_name(row["type"])
        return await cast(Type[HardwareRequirement], type)._load(
            context, row["params"], loading_context
        )

    async def save(self, context: StreamFlowContext):
        return {
            "type": utils.get_class_fullname(type(self)),
            "params": await self._save_additional_params(context),
        }


class JobAllocation:
    __slots__ = ("job", "target", "locations", "status", "hardware")

    def __init__(
        self,
        job: str,
        target: Target,
        locations: MutableSequence[ExecutionLocation],
        status: Status,
        hardware: JobHardwareRequirement,
    ):
        self.job: str = job
        self.target: Target = target
        self.locations: MutableSequence[ExecutionLocation] = locations
        self.status: Status = status
        self.hardware: JobHardwareRequirement = hardware


class AvailableLocation:
    __slots__ = (
        "hardware",
        "location",
        "slots",
        "wraps",
    )

    def __init__(
        self,
        name: str,
        deployment: str,
        hostname: str,
        service: str | None = None,
        slots: int = 1,
        hardware: DeploymentHardware | None = None,
        wraps: AvailableLocation | None = None,
    ):
        self.hardware: DeploymentHardware | None = hardware
        self.location: ExecutionLocation = ExecutionLocation(
            deployment=deployment,
            hostname=hostname,
            name=name,
            service=service,
            wraps=wraps.location if wraps else None,
        )
        self.slots: int = slots
        self.wraps: AvailableLocation | None = wraps

    @property
    def deployment(self) -> str:
        return self.location.deployment

    @property
    def hostname(self) -> str:
        return self.location.hostname

    @property
    def name(self) -> str:
        return self.location.name

    @property
    def service(self) -> str | None:
        return self.location.service


class LocationAllocation:
    __slots__ = ("name", "deployment", "jobs")

    def __init__(self, name: str, deployment: str):
        self.name: str = name
        self.deployment: str = deployment
        self.jobs: MutableSequence[str] = []


class PolicyConfig(Config):
    async def save(self, context: StreamFlowContext):
        # TODO
        return self.config


class JobContext:
    __slots__ = ("job", "event", "targets", "hardware_requirement")

    def __init__(
        self,
        job: Job,
        targets: MutableSequence[Target],
        hardware_requirement: JobHardwareRequirement,
    ) -> None:
        self.job: Job = job
        self.event: asyncio.Event = asyncio.Event()
        self.targets: MutableSequence[Target] = targets
        self.hardware_requirement: JobHardwareRequirement = hardware_requirement


class Policy(SchemaEntity):
    @abstractmethod
    async def get_location(
        self,
        context: StreamFlowContext,
        pending_jobs: MutableSequence[JobContext],
        available_locations: MutableMapping[str, AvailableLocation],
        scheduled_jobs: MutableSequence[JobAllocation],
        locations: MutableMapping[str, MutableMapping[str, LocationAllocation]],
    ) -> MutableMapping[str, AvailableLocation]: ...


class Scheduler(SchemaEntity):
    def __init__(self, context: StreamFlowContext):
        self.context: StreamFlowContext = context
        self.job_allocations: MutableMapping[str, JobAllocation] = {}
        self.location_allocations: MutableMapping[
            str, MutableMapping[str, LocationAllocation]
        ] = {}

    @abstractmethod
    async def close(self) -> None: ...

    def get_allocation(self, job_name: str) -> JobAllocation | None:
        return self.job_allocations.get(job_name)

    def get_hardware(self, job_name: str) -> DeploymentHardware | None:
        allocation = self.get_allocation(job_name)
        return allocation.hardware if allocation else None

    def get_connector(self, job_name: str) -> Connector | None:
        allocation = self.get_allocation(job_name)
        return (
            self.context.deployment_manager.get_connector(
                allocation.target.deployment.name
            )
            if allocation
            else None
        )

    def get_locations(
        self, job_name: str, statuses: MutableSequence[Status] | None = None
    ) -> MutableSequence[ExecutionLocation]:
        allocation = self.get_allocation(job_name)
        return (
            allocation.locations
            if allocation is not None
            and (statuses is None or allocation.status in statuses)
            else []
        )

    def get_service(self, job_name: str) -> str | None:
        allocation = self.get_allocation(job_name)
        return allocation.target.service if allocation else None

    @abstractmethod
    async def notify_status(self, job_name: str, status: Status) -> None: ...

    @abstractmethod
    async def schedule(
        self,
        job: Job,
        binding_config: BindingConfig,
        hardware_requirement: JobHardwareRequirement,
    ) -> None: ...


def diff_hw(
    deployment_hw: DeploymentHardware, hw_requirement: JobHardwareRequirement
) -> DeploymentHardware:
    # job = job_context.job
    # hw_requirement = job_context.hardware_requirement
    # storage = {}
    # for path, size in deployment_hw.storage.items():
    #     # todo: it is not really correct. They can have the same starts but be in different volumes
    #     #  e.g. / -> volume1 and /mnt/data -> volume2
    #     if job.tmp_directory.startswith(path):
    #         size -= hw_requirement.tmp_dir_size
    #     elif job.output_directory.startswith(path):
    #         size -= hw_requirement.out_dir_size
    #     storage[path] = size
    return DeploymentHardware(
        cores=deployment_hw.cores - hw_requirement.cores,
        memory=deployment_hw.memory - hw_requirement.memory,
        storage=deployment_hw.storage,
    )


def sum_job_req(
    job_hw_reqs: MutableSequence[JobHardwareRequirement],
) -> JobHardwareRequirement:
    cores = 0
    memory = 0
    tmp_dir_size = 0
    out_dir_size = 0
    for job_hw_req in job_hw_reqs:
        cores += job_hw_req.cores
        memory += job_hw_req.memory
        tmp_dir_size += job_hw_req.tmp_dir_size
        out_dir_size += job_hw_req.out_dir_size
    return JobHardwareRequirement(
        cores=cores,
        memory=memory,
        tmp_dir_size=tmp_dir_size,
        out_dir_size=out_dir_size,
    )


def greater_eq_hw(
    deployment_hw: DeploymentHardware, job_hw_req: JobHardwareRequirement
) -> bool:
    # todo: dirs comparison
    return (
        deployment_hw.cores >= job_hw_req.cores
        and deployment_hw.memory >= job_hw_req.memory
    )
