from __future__ import annotations

import asyncio
import logging
import os
import posixpath
from typing import MutableSequence, TYPE_CHECKING

import pkg_resources

from streamflow.core.config import BindingConfig, Config
from streamflow.core.deployment import BindingFilter, Location, Target
from streamflow.core.scheduling import (
    Hardware,
    JobAllocation,
    LocationAllocation,
    Policy,
    Scheduler,
)
from streamflow.core.workflow import Job, Status
from streamflow.deployment.connector import LocalConnector
from streamflow.deployment.filter import binding_filter_classes
from streamflow.log_handler import logger
from streamflow.scheduling.policy import policy_classes

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.scheduling import AvailableLocation
    from typing import MutableMapping


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
        self.allocation_groups: MutableMapping[str, MutableSequence[Job]] = {}
        self.binding_filter_map: MutableMapping[str, BindingFilter] = {}
        self.policy_map: MutableMapping[str, Policy] = {}
        self.retry_interval: int | None = retry_delay if retry_delay != 0 else None
        self.scheduling_groups: MutableMapping[str, MutableSequence[str]] = {}
        self.wait_queues: MutableMapping[str, asyncio.Condition] = {}

    def _allocate_job(
        self,
        job: Job,
        hardware: Hardware,
        selected_locations: MutableSequence[Location],
        target: Target,
    ):
        if logger.isEnabledFor(logging.DEBUG):
            if len(selected_locations) == 1:
                is_local = isinstance(
                    self.context.deployment_manager.get_connector(
                        selected_locations[0].deployment
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
            locations=selected_locations,
            status=Status.FIREABLE,
            hardware=hardware or Hardware(),
        )
        for loc in selected_locations:
            if loc not in self.location_allocations:
                self.location_allocations.setdefault(loc.deployment, {}).setdefault(
                    loc.name,
                    LocationAllocation(name=loc.name, deployment=loc.deployment),
                ).jobs.append(job.name)

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

    def _get_binding_filter(self, config: Config):
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
    ) -> MutableSequence[Location] | None:
        selected_locations = []
        for _ in range(locations):
            selected_location = await scheduling_policy.get_location(
                context=self.context,
                job=job,
                hardware_requirement=hardware_requirement,
                available_locations=available_locations,
                jobs=self.job_allocations,
                locations=self.location_allocations,
            )
            if selected_location is not None:
                selected_locations.append(selected_location)
                available_locations = {
                    k: v
                    for k, v in available_locations.items()
                    if v != selected_location
                }
            else:
                return None
        return selected_locations

    def _get_policy(self, config: Config):
        if config.name not in self.policy_map:
            self.policy_map[config.name] = policy_classes[config.type](**config.config)
        return self.policy_map[config.name]

    def _is_valid(
        self, location: AvailableLocation, hardware_requirement: Hardware
    ) -> bool:
        if location.name in self.location_allocations.get(location.deployment, {}):
            running_jobs = list(
                filter(
                    lambda x: (
                        self.job_allocations[x].status == Status.RUNNING
                        or self.job_allocations[x].status == Status.FIREABLE
                    ),
                    self.location_allocations[location.deployment][location.name].jobs,
                )
            )
        else:
            running_jobs = []
        # If location is segmentable and job provides requirements, compute the used amount of locations
        if location.hardware is not None and hardware_requirement is not None:
            used_hardware = sum(
                (self.job_allocations[j].hardware for j in running_jobs),
                start=hardware_requirement.__class__(),
            )
            if (location.hardware - used_hardware) >= hardware_requirement:
                return True
            else:
                return False
        # If location is segmentable but job does not provide requirements, treat it as null-weighted
        elif location.hardware is not None:
            return True
        # Otherwise, simply compute the number of allocated slots
        else:
            return len(running_jobs) < location.slots

    async def _process_target(
        self, target: Target, job_context: JobContext, hardware_requirement: Hardware
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
                                posixpath.join(deployment, target.service)
                                if target.service
                                else deployment,
                            )
                        )
                    available_locations = dict(
                        await connector.get_available_locations(
                            service=target.service,
                            input_directory=job_context.job.input_directory
                            or target.workdir,
                            output_directory=job_context.job.output_directory
                            or target.workdir,
                            tmp_directory=job_context.job.tmp_directory
                            or target.workdir,
                        )
                    )
                    valid_locations = {
                        k: loc
                        for k, loc in available_locations.items()
                        if self._is_valid(
                            location=loc, hardware_requirement=hardware_requirement
                        )
                    }
                    if valid_locations:
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(
                                "Available locations for job {} on {} are {}.".format(
                                    job_context.job.name,
                                    posixpath.join(deployment, target.service)
                                    if target.service
                                    else deployment,
                                    list(valid_locations.keys()),
                                )
                            )
                        if target.scheduling_group is not None:
                            if target.scheduling_group not in self.allocation_groups:
                                self.allocation_groups[target.scheduling_group] = []
                            self.allocation_groups[target.scheduling_group].append(
                                job_context.job
                            )
                            group_size = len(
                                self.scheduling_groups[target.scheduling_group]
                            )
                            if (
                                len(
                                    self.allocation_groups.get(
                                        target.scheduling_group, []
                                    )
                                )
                                == group_size
                            ):
                                allocated_jobs = []
                                for j in self.allocation_groups[
                                    target.scheduling_group
                                ]:
                                    selected_locations = await self._get_locations(
                                        job=job_context.job,
                                        hardware_requirement=hardware_requirement,
                                        locations=target.locations,
                                        scheduling_policy=self._get_policy(
                                            target.scheduling_policy
                                        ),
                                        available_locations=valid_locations,
                                    )
                                    if selected_locations is None:
                                        break
                                    self._allocate_job(
                                        job=j,
                                        hardware=hardware_requirement,
                                        selected_locations=selected_locations,
                                        target=target,
                                    )
                                    allocated_jobs.append(j)
                                if len(allocated_jobs) < group_size:
                                    for j in allocated_jobs:
                                        self._deallocate_job(j.name)
                                else:
                                    job_context.scheduled = True
                                    return
                        else:
                            selected_locations = await self._get_locations(
                                job=job_context.job,
                                hardware_requirement=hardware_requirement,
                                locations=target.locations,
                                scheduling_policy=self._get_policy(
                                    target.scheduling_policy
                                ),
                                available_locations=valid_locations,
                            )
                            if selected_locations is not None:
                                self._allocate_job(
                                    job=job_context.job,
                                    hardware=hardware_requirement,
                                    selected_locations=selected_locations,
                                    target=target,
                                )
                                job_context.scheduled = True
                                return
                    else:
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(
                                "No location available for job {} on deployment {}.".format(
                                    job_context.job.name,
                                    posixpath.join(deployment, target.service)
                                    if target.service
                                    else deployment,
                                )
                            )
                try:
                    await asyncio.wait_for(
                        self.wait_queues[deployment].wait(), timeout=self.retry_interval
                    )
                except TimeoutError:
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

    async def close(self):
        pass

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "scheduler.json")
        )

    async def notify_status(self, job_name: str, status: Status) -> None:
        connector = self.get_connector(job_name)
        if connector:
            if connector.deployment_name in self.wait_queues:
                async with self.wait_queues[connector.deployment_name]:
                    if job_name in self.job_allocations:
                        if status != self.job_allocations[job_name].status:
                            self.job_allocations[job_name].status = status
                            if logger.isEnabledFor(logging.DEBUG):
                                logger.debug(
                                    f"Job {job_name} changed status to {status.name}"
                                )
                        if status in [Status.COMPLETED, Status.FAILED]:
                            self.wait_queues[connector.deployment_name].notify_all()

    async def schedule(
        self, job: Job, binding_config: BindingConfig, hardware_requirement: Hardware
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
