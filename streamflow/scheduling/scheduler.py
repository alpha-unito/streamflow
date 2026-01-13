from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import posixpath
from collections.abc import MutableMapping, MutableSequence
from importlib.resources import files
from typing import TYPE_CHECKING, cast

from streamflow.core.config import BindingConfig, Config
from streamflow.core.deployment import BindingFilter, Connector, FilterConfig, Target
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.scheduling import (
    Hardware,
    HardwareRequirement,
    JobAllocation,
    LocationAllocation,
    Policy,
    Scheduler,
    Storage,
)
from streamflow.core.utils import compare_tags, get_job_step_name, get_job_tag
from streamflow.core.workflow import Job, Status
from streamflow.data import remotepath, utils
from streamflow.deployment.filter import binding_filter_classes
from streamflow.deployment.wrapper import ConnectorWrapper
from streamflow.log_handler import logger
from streamflow.scheduling.policy import policy_classes

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.scheduling import AvailableLocation


def _get_connector_stack(connector: Connector) -> MutableSequence[Connector]:
    connectors = []
    while connector is not None:
        connectors.append(connector)
        connector = (
            connector.connector if isinstance(connector, ConnectorWrapper) else None
        )
    return connectors


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
        self.locks: MutableMapping[str, asyncio.Lock] = {}
        self.policy_map: MutableMapping[str, Policy] = {}
        self.retry_interval: int | None = retry_delay if retry_delay != 0 else None
        self.wait_queues: MutableMapping[str, asyncio.Condition] = {}

    def _allocate_job(
        self,
        job: Job,
        hardware: MutableMapping[str, Hardware],
        connector: Connector,
        selected_locations: MutableSequence[AvailableLocation],
        target: Target,
    ):
        if logger.isEnabledFor(logging.DEBUG):
            if len(selected_locations) == 1:
                logger.debug(
                    "Job {name} allocated {location}".format(
                        name=job.name,
                        location=(
                            "locally"
                            if selected_locations[0].local
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
            hardware=hardware[
                posixpath.join(
                    connector.deployment_name,
                    next(loc.name for loc in selected_locations),
                )
            ],
        )
        for loc in selected_locations:
            conn = connector
            while loc is not None:
                self.location_allocations.setdefault(
                    loc.location.deployment, {}
                ).setdefault(
                    loc.location.name,
                    LocationAllocation(
                        name=loc.location.name,
                        deployment=loc.location.deployment,
                    ),
                ).jobs.append(
                    job.name
                )
                key = posixpath.join(
                    conn.deployment_name,
                    loc.name,
                )
                if key in hardware.keys() and hardware[key]:
                    if loc.name in self.hardware_locations.keys():
                        self.hardware_locations[loc.name] += hardware[key]
                    else:
                        # Get normalized hardware for the hardware location
                        self.hardware_locations[loc.name] = Hardware() + hardware[key]
                if loc := loc.wraps if loc.stacked else None:
                    conn = cast(ConnectorWrapper, conn).connector

    async def _free_resources(
        self, connector: Connector, job_allocation: JobAllocation, status: Status
    ) -> None:
        async with contextlib.AsyncExitStack() as exit_stack:
            for conn in _get_connector_stack(connector):
                await exit_stack.enter_async_context(self.locks[conn.deployment_name])
            conn = connector
            locations = job_allocation.locations
            job_hardware = job_allocation.hardware
            while locations:
                for loc in locations:
                    if loc.name in self.hardware_locations.keys():
                        try:
                            storage_usage = Hardware(
                                storage=(
                                    {
                                        k: Storage(
                                            mount_point=job_hardware.storage[
                                                k
                                            ].mount_point,
                                            size=size / 2**20,
                                        )
                                        for k, size in (
                                            await remotepath.get_storage_usages(
                                                self.context,
                                                loc,
                                                job_hardware,
                                            )
                                        ).items()
                                    }
                                )
                            )
                        except WorkflowExecutionException as err:
                            logger.warning(
                                f"Impossible to retrieve the actual storage usage in "
                                f"the {job_allocation.job} job working directories: {err}"
                            )
                            storage_usage = Hardware()
                        self.hardware_locations[loc.name] = (
                            self.hardware_locations[loc.name] - job_hardware
                        ) + storage_usage
                if locations := [loc.wraps for loc in locations if loc.stacked]:
                    conn = cast(ConnectorWrapper, conn).connector
                    for execution_loc in locations:
                        job_hardware = await utils.bind_mount_point(
                            self.context,
                            conn,
                            next(
                                available_loc
                                for available_loc in (
                                    await conn.get_available_locations(
                                        execution_loc.service
                                    )
                                ).values()
                                if available_loc.name == execution_loc.name
                            ),
                            job_hardware,
                        )

    def _get_binding_filter(self, config: FilterConfig):
        if config.name not in self.binding_filter_map:
            self.binding_filter_map[config.name] = binding_filter_classes[config.type](
                config.name, **config.config
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

    def _get_running_jobs(
        self, job_name: str, location: AvailableLocation
    ) -> MutableSequence[str]:
        if location.name in self.location_allocations.get(location.deployment, {}):
            return list(
                filter(
                    lambda x: (
                        self.job_allocations[x].status == Status.RUNNING
                        or self.job_allocations[x].status == Status.FIREABLE
                        or (
                            self.job_allocations[x].status == Status.ROLLBACK
                            and get_job_step_name(x) == get_job_step_name(job_name)
                            and compare_tags(get_job_tag(x), get_job_tag(job_name)) < 0
                        )
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
        hardware_requirements: MutableMapping[str, Hardware],
        job_name: str,
    ) -> bool:
        hardware_requirement = hardware_requirements[
            posixpath.join(connector.deployment_name, location.name)
        ]
        while location is not None:
            # If at least one location provides hardware capabilities
            if location.hardware is not None:
                # Compute the used amount of locations
                if not (
                    (
                        location.hardware
                        - self.hardware_locations.get(location.name, Hardware())
                    ).satisfies(hardware_requirement)
                ):
                    return False
            # Otherwise, simply compute the number of allocated slots
            else:
                slots = location.slots if location.slots is not None else 1
                if not len(self._get_running_jobs(job_name, location)) < slots:
                    return False
            # If AvailableLocation is stacked, evaluate also the inner location
            if location := location.wraps if location.stacked else None:
                connector = cast(ConnectorWrapper, connector).connector
                hardware_requirement = hardware_requirements[
                    posixpath.join(connector.deployment_name, location.name)
                ]
        return True

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
                    hardware_requirements = {}
                    for requirements in await asyncio.gather(
                        *(
                            asyncio.create_task(
                                self._resolve_hardware_requirement(
                                    connector, location, job_hardware
                                )
                            )
                            for location in available_locations.values()
                        )
                    ):
                        for key, hardware in requirements.items():
                            if key not in hardware_requirements:
                                hardware_requirements[key] = hardware
                            else:
                                hardware_requirements[key] |= hardware
                    async with contextlib.AsyncExitStack() as exit_stack:
                        for conn in _get_connector_stack(connector):
                            if conn.deployment_name not in self.locks:
                                self.locks[conn.deployment_name] = asyncio.Lock()
                            await exit_stack.enter_async_context(
                                self.locks[conn.deployment_name]
                            )
                        valid_locations = {
                            k: loc
                            for k, loc in available_locations.items()
                            if self._is_valid(
                                connector=connector,
                                location=loc,
                                hardware_requirements=hardware_requirements,
                                job_name=job.name,
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
                                    connector=connector,
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
    ) -> MutableMapping[str, Hardware]:
        hardware = {}
        while location is not None:
            # If AvailableLocation defines the Hardware capabilities, process the requirement
            if location.hardware is not None:
                storage = {}
                for key, disk in hardware_requirement.storage.items():
                    for path in disk.paths:
                        mount_point = await utils.get_mount_point(
                            self.context, location, path
                        )
                        storage[key] = Storage(
                            mount_point=mount_point,
                            size=disk.size,
                            paths={path},
                            bind=location.hardware.get_storage(mount_point).bind,
                        )
                current_hw = Hardware(
                    cores=hardware_requirement.cores,
                    memory=hardware_requirement.memory,
                    storage=storage,
                )
            # Otherwise, simply add the requirement as is
            else:
                current_hw = Hardware(
                    cores=hardware_requirement.cores,
                    memory=hardware_requirement.memory,
                    storage={
                        key: Storage(mount_point=os.sep, size=disk.size)
                        for key, disk in hardware_requirement.storage.items()
                    },
                )
            hardware[posixpath.join(connector.deployment_name, location.name)] = (
                current_hw
            )
            # If AvailableLocation is stacked and wraps another location, compute the inner requirement
            if location := location.wraps if location.stacked else None:
                connector = cast(ConnectorWrapper, connector).connector
                hardware_requirement = await utils.bind_mount_point(
                    self.context, connector, location, current_hw
                )
        return hardware

    async def close(self) -> None:
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
        if (
            connector := self.get_connector(job_name)
        ).deployment_name in self.wait_queues:
            async with self.wait_queues[connector.deployment_name]:
                if job_allocation := self.job_allocations.get(job_name):
                    if status != (previous_status := job_allocation.status):
                        job_allocation.status = status
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(
                                f"Job {job_name} changed status to {status.name}"
                            )
                    # Job was running and changed status, or the job was ready to
                    # run (i.e., fireable) but changed to a status different from the running one.
                    if status != previous_status and (
                        previous_status == Status.RUNNING
                        or (
                            previous_status == Status.FIREABLE
                            and status != Status.RUNNING
                        )
                    ):
                        await self._free_resources(connector, job_allocation, status)
                    if status == Status.ROLLBACK:
                        for loc in job_allocation.locations:
                            if (
                                job_name
                                in self.location_allocations[loc.deployment][
                                    loc.name
                                ].jobs
                            ):
                                self.location_allocations[loc.deployment][
                                    loc.name
                                ].jobs.remove(job_name)
                        job_allocation.locations.clear()
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
