from __future__ import annotations

import asyncio
import logging
import os
import posixpath
from typing import MutableSequence, TYPE_CHECKING, cast

from importlib_resources import files

from streamflow.core.config import BindingConfig, Config
from streamflow.core.deployment import (
    BindingFilter,
    FilterConfig,
    Target,
    Connector,
)
from streamflow.core.scheduling import (
    Hardware,
    JobAllocation,
    LocationAllocation,
    Policy,
    Scheduler,
    HardwareRequirement,
    Storage,
)
from streamflow.core.workflow import Job, Status
from streamflow.data import remotepath
from streamflow.deployment.connector import LocalConnector
from streamflow.deployment.filter import binding_filter_classes
from streamflow.deployment.wrapper import ConnectorWrapper
from streamflow.log_handler import logger
from streamflow.scheduling.policy import policy_classes

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.scheduling import AvailableLocation
    from typing import MutableMapping


def _get_location_for_requirement(
    connector: Connector, location: AvailableLocation
) -> AvailableLocation:
    loc = location
    # Check if a location provides hardware capabilities or slots
    while (
        loc.hardware is None and loc.slots is None and loc is not None and loc.stacked
    ):
        loc = loc.wraps
        connector = cast(ConnectorWrapper, connector).connector
        # todo: storage paths could be wrong when we go to the inner location
    return loc or location


class JobContext:
    __slots__ = ("job", "lock", "scheduled")

    def __init__(self, job: Job):
        self.job: Job = job
        self.lock: asyncio.Lock = asyncio.Lock()
        self.scheduled: bool = False


class DefaultScheduler(Scheduler):
    def __init__(
        self, context: StreamFlowContext, retry_delay: int | None = None
    ) -> None:
        super().__init__(context)
        self.binding_filter_map: MutableMapping[str, BindingFilter] = {}
        self.hardware_locations: MutableMapping[str, Hardware] = {}
        self.policy_map: MutableMapping[str, Policy] = {}
        self.retry_interval: int | None = retry_delay if retry_delay != 0 else None
        self.wait_queues: MutableMapping[str, asyncio.Condition] = {}

    def _allocate_job(
        self,
        job: Job,
        hardware: MutableMapping[str, Hardware],
        selected_locations: MutableSequence[AvailableLocation],
        target: Target,
    ):
        if logger.isEnabledFor(logging.DEBUG):
            if len(selected_locations) == 1:
                is_local = isinstance(
                    self.context.deployment_manager.get_connector(
                        selected_locations[0].location.deployment
                    ),
                    LocalConnector,
                )
                logger.debug(
                    "Job {name} allocated {location}".format(
                        name=job.name,
                        location=(
                            "locally"
                            if is_local
                            else f"on location {selected_locations[0]}"
                        ),
                    )
                )
            else:
                logger.debug(
                    f"Job {job.name} allocated on locations "
                    f"{', '.join([str(loc) for loc in selected_locations])}"
                )
        self.job_allocations[job.name] = JobAllocation(
            job=job.name,
            target=target,
            locations=[loc.location for loc in selected_locations],
            status=Status.FIREABLE,
            hardware=hardware[next(loc.name for loc in selected_locations)],
        )
        for loc in selected_locations:
            while loc is not None:
                if loc.location not in self.location_allocations:
                    self.location_allocations.setdefault(
                        loc.location.deployment, {}
                    ).setdefault(
                        loc.location.name,
                        LocationAllocation(
                            name=loc.location.name, deployment=loc.location.deployment
                        ),
                    ).jobs.append(
                        job.name
                    )
                    # fixme: wraps not considered
                    if loc.name in hardware.keys() and hardware[loc.name]:
                        if loc.name in self.hardware_locations.keys():
                            self.hardware_locations[loc.name] += hardware[loc.name]
                        else:
                            self.hardware_locations[loc.name] = hardware[loc.name]
                    loc = loc.wraps if loc.stacked else None

    def _deallocate_job(self, job: str):
        job_allocation = self.job_allocations.pop(job)
        for loc in job_allocation.locations:
            self.location_allocations[loc.deployment][loc.name].jobs.remove(job)
        if logger.isEnabledFor(logging.INFO):
            if len(job_allocation.locations) == 1:
                is_local = isinstance(
                    self.context.deployment_manager.get_connector(
                        job_allocation.locations[0].deployment
                    ),
                    LocalConnector,
                )
                logger.info(
                    "Job {name} deallocated {location}".format(
                        name=job,
                        location=(
                            "from local location"
                            if is_local
                            else f"from location {job_allocation.locations[0]}"
                        ),
                    )
                )
            else:
                logger.info(
                    "Job {job} deallocated from locations "
                    ", ".join([str(loc) for loc in job_allocation.locations])
                )

    def _get_binding_filter(self, config: FilterConfig):
        if config.name not in self.binding_filter_map:
            self.binding_filter_map[config.name] = binding_filter_classes[config.type](
                **config.config
            )
        return self.binding_filter_map[config.name]

    async def _get_locations(
        self,
        job: Job,
        hardware_requirement: Hardware,
        locations: int,
        scheduling_policy: Policy,
        available_locations: MutableMapping[str, AvailableLocation],
    ) -> MutableSequence[AvailableLocation]:
        # If available locations are exactly the amount of required locations, simply use them
        if len(available_locations) == locations:
            return list(available_locations.values())
        # Otherwise, use the `Policy` to select the best set of locations to allocate
        else:
            selected_locations = []
            for _ in range(locations):
                if selected_location := await scheduling_policy.get_location(
                    context=self.context,
                    job=job,
                    hardware_requirement=hardware_requirement,
                    available_locations=available_locations,
                    jobs=self.job_allocations,
                    locations=self.location_allocations,
                ):
                    selected_locations.append(selected_location)
                    available_locations = {
                        k: v
                        for k, v in available_locations.items()
                        if v != selected_location
                    }
                else:
                    return []
            return selected_locations

    def _get_policy(self, config: Config):
        if config.name not in self.policy_map:
            self.policy_map[config.name] = policy_classes[config.type](**config.config)
        return self.policy_map[config.name]

    def _get_running_jobs(self, location: AvailableLocation):
        if location.name in self.location_allocations.get(location.deployment, {}):
            return list(
                filter(
                    lambda x: (
                        self.job_allocations[x].status == Status.RUNNING
                        or self.job_allocations[x].status == Status.FIREABLE
                    ),
                    self.location_allocations[location.deployment][location.name].jobs,
                )
            )
        else:
            return []

    def _is_valid(
        self,
        connector: Connector,
        location: AvailableLocation,
        hardware_requirement: Hardware,
    ) -> bool:
        location = _get_location_for_requirement(connector, location)
        # If at least one location provides hardware capabilities
        if location.hardware is not None:
            # Compute the used amount of locations
            return (
                location.hardware
                - self.hardware_locations.get(location.name, Hardware())
            ) >= hardware_requirement
        # Otherwise, simply compute the number of allocated slots
        else:
            slots = location.slots if location.slots is not None else 1
            return len(self._get_running_jobs(location)) < slots

    async def _process_target(
        self,
        target: Target,
        job_context: JobContext,
        hardware_requirement: HardwareRequirement | None,
    ):
        deployment = target.deployment.name
        if deployment not in self.wait_queues:
            self.wait_queues[deployment] = asyncio.Condition()
        async with self.wait_queues[deployment]:
            while True:
                async with job_context.lock:
                    if job_context.scheduled:
                        return
                    connector = self.context.deployment_manager.get_connector(
                        deployment
                    )
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(
                            "Retrieving available locations for job {} on {}.".format(
                                job_context.job.name,
                                (
                                    posixpath.join(deployment, target.service)
                                    if target.service
                                    else deployment
                                ),
                            )
                        )

                    job = Job(
                        name=job_context.job.name,
                        workflow_id=job_context.job.workflow_id,
                        inputs=job_context.job.inputs,
                        input_directory=job_context.job.input_directory
                        or target.workdir,
                        output_directory=job_context.job.output_directory
                        or target.workdir,
                        tmp_directory=job_context.job.tmp_directory or target.workdir,
                    )
                    available_locations = dict(
                        await connector.get_available_locations(service=target.service)
                    )
                    job_hardware = (
                        hardware_requirement.eval(job)
                        if hardware_requirement
                        else Hardware()
                    )
                    hardware_requirements = {
                        location: hardware_requirement
                        for location, hardware_requirement in zip(
                            available_locations.keys(),
                            await asyncio.gather(
                                *(
                                    asyncio.create_task(
                                        self._resolve_hardware_requirement(
                                            connector, location, job_hardware
                                        )
                                    )
                                    for location in available_locations.values()
                                )
                            ),
                        )
                    }
                    valid_locations = {
                        k: loc
                        for k, loc in available_locations.items()
                        if self._is_valid(
                            connector=connector,
                            location=loc,
                            hardware_requirement=hardware_requirements[loc.name],
                        )
                    }
                    if len(valid_locations) >= target.locations:
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(
                                "Available locations for job {} on {} are {}.".format(
                                    job_context.job.name,
                                    (
                                        posixpath.join(deployment, target.service)
                                        if target.service
                                        else deployment
                                    ),
                                    list(valid_locations.keys()),
                                )
                            )
                        if selected_locations := await self._get_locations(
                            job=job_context.job,
                            hardware_requirement=job_hardware,
                            locations=target.locations,
                            scheduling_policy=self._get_policy(
                                target.deployment.scheduling_policy
                            ),
                            available_locations=valid_locations,
                        ):
                            self._allocate_job(
                                job=job_context.job,
                                hardware=hardware_requirements,
                                selected_locations=selected_locations,
                                target=target,
                            )
                            job_context.scheduled = True
                            return
                    else:
                        if logger.isEnabledFor(logging.DEBUG):
                            deployment_name = (
                                posixpath.join(deployment, target.service)
                                if target.service
                                else deployment
                            )
                            logger.debug(
                                f"Not enough available locations: job {job_context.job.name} "
                                f"requires {target.locations} locations "
                                f"on deployment {deployment_name}, "
                                f"but only {len(valid_locations)} are available."
                            )
                try:
                    await asyncio.wait_for(
                        self.wait_queues[deployment].wait(), timeout=self.retry_interval
                    )
                except (TimeoutError, asyncio.exceptions.TimeoutError):
                    if logger.isEnabledFor(logging.DEBUG):
                        target_name = (
                            "/".join([target.deployment.name, target.service])
                            if target.service is not None
                            else target.deployment.name
                        )
                        logger.debug(
                            f"No locations available for job {job_context.job.name} "
                            f"in target {target_name}. Waiting {self.retry_interval} seconds."
                        )

    async def _resolve_hardware_requirement(
        self,
        connector: Connector,
        location: AvailableLocation,
        hardware_requirement: Hardware,
    ) -> Hardware:
        location = _get_location_for_requirement(connector, location)
        # The `paths` of the `hardware_requirement.storage.values()` are not added in the `paths` attribute of
        # the new `Hardware` because the job dirs still do not exist. The directories will be added
        # when they are created in the `ScheduleStep`
        if location.hardware is not None:
            storage = {}
            for key, disk in hardware_requirement.storage.items():
                for path in disk.paths:
                    mount_point = await remotepath.get_mount_point(
                        self.context, connector, location, path
                    )
                    storage[key] = Storage(mount_point, disk.size)
            return Hardware(
                cores=hardware_requirement.cores,
                memory=hardware_requirement.memory,
                storage=storage,
            )
        else:
            return Hardware(
                cores=hardware_requirement.cores,
                memory=hardware_requirement.memory,
                storage={
                    key: Storage(os.sep, disk.size)
                    for key, disk in hardware_requirement.storage.items()
                },
            )

    async def close(self):
        pass

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("scheduler.json")
            .read_text("utf-8")
        )

    async def notify_status(self, job_name: str, status: Status) -> None:
        connector = self.get_connector(job_name)
        if connector:
            if connector.deployment_name in self.wait_queues:
                async with self.wait_queues[connector.deployment_name]:
                    if job_name in self.job_allocations:
                        job_allocation = self.job_allocations[job_name]
                        if status != job_allocation.status:
                            job_allocation.status = status
                            if logger.isEnabledFor(logging.DEBUG):
                                logger.debug(
                                    f"Job {job_name} changed status to {status.name}"
                                )
                        if status in [Status.COMPLETED, Status.FAILED]:
                            if job_hardware := job_allocation.hardware:
                                for loc in job_allocation.locations:
                                    if loc.name in self.hardware_locations.keys():
                                        # It is executed `remotepath.get_storage_usages` to get the actual job output size.
                                        # It is supposed that the output directory was empty before the `Job` execution
                                        self.hardware_locations[
                                            loc.name
                                        ].print("Pre")
                                        self.hardware_locations[
                                            loc.name
                                        ] -= job_hardware.get_free_resources(
                                            await remotepath.get_storage_usages(
                                                connector, loc, job_hardware
                                            )
                                        )
                                        self.hardware_locations[
                                            loc.name
                                        ].print("post")
                            self.wait_queues[connector.deployment_name].notify_all()

    async def schedule(
        self,
        job: Job,
        binding_config: BindingConfig,
        hardware_requirement: HardwareRequirement | None,
    ) -> None:
        job_context = JobContext(job)
        targets = list(binding_config.targets)
        for f in (self._get_binding_filter(f) for f in binding_config.filters):
            targets = await f.get_targets(job, targets)
        wait_tasks = [
            asyncio.create_task(
                self._process_target(
                    target=target,
                    job_context=job_context,
                    hardware_requirement=hardware_requirement,
                )
            )
            for target in targets
        ]
        # Capture finished tasks and call result() to check for exceptions
        finished, _ = await asyncio.wait(
            wait_tasks, return_when=asyncio.FIRST_COMPLETED
        )
        for task in finished:
            if task.cancelled():
                continue
            task.result()
