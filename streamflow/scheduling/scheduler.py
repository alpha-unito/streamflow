from __future__ import annotations

import asyncio
import logging
from typing import MutableSequence, TYPE_CHECKING

from importlib_resources import files

from streamflow.core.config import BindingConfig, Config
from streamflow.core.deployment import (
    BindingFilter,
    ExecutionLocation,
    FilterConfig,
    Target,
)
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.scheduling import (
    Hardware,
    JobAllocation,
    LocationAllocation,
    Policy,
    Scheduler,
    JobContext,
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


class DefaultScheduler(Scheduler):
    def __init__(
        self, context: StreamFlowContext, retry_delay: int | None = None
    ) -> None:
        super().__init__(context)
        self.binding_filter_map: MutableMapping[str, BindingFilter] = {}
        self.pending_jobs: MutableSequence[JobContext] = []
        self.pending_job_event = asyncio.Event()
        self.policy_map: MutableMapping[str, Policy] = {}
        self.retry_interval: int | None = retry_delay if retry_delay != 0 else None
        self.scheduling_groups: MutableMapping[str, MutableSequence[str]] = {}
        self.scheduling_task: asyncio.Task = asyncio.create_task(
            self._scheduling_task()
        )

    def _allocate_job(
        self,
        job: Job,
        hardware: Hardware,
        selected_locations: MutableSequence[ExecutionLocation],
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
            hardware=hardware or None,
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

    def _get_binding_filter(self, config: FilterConfig):
        if config.name not in self.binding_filter_map:
            self.binding_filter_map[config.name] = binding_filter_classes[config.type](
                **config.config
            )
        return self.binding_filter_map[config.name]

    # FIXME:
    async def _get_deployments(
        self,
    ) -> MutableMapping[str, MutableMapping[str, AvailableLocation]]:
        """Get all the deployment involved by the jobs and related available locations"""
        deployments = {}
        for job_context in self.pending_jobs:
            for target in job_context.targets:
                deployment = target.deployment.name
                connector = self.context.deployment_manager.get_connector(deployment)

                # Some jobs can have the same deployment, but in the available location will have different directories
                # Moreover, they can have different hardware_requirement, which are used in the valid_locations choice
                # todo: Maybe it is possible make the get_available_locations just one time for each deployment,
                #  and then each job will check its valid_locations
                available_locations = await connector.get_available_locations(
                    service=target.service,
                    directories=[
                        job_context.job.input_directory or target.workdir,
                        job_context.job.output_directory or target.workdir,
                        job_context.job.tmp_directory or target.workdir,
                    ],
                )
                valid_locations = {
                    k: loc
                    for k, loc in available_locations.items()
                    if self._is_valid(
                        location=loc,
                        hardware_requirement=job_context.hardware_requirement,
                    )
                }
                deployments.setdefault(job_context.job.name, {})
                for k, v in valid_locations.items():

                    # todo: is it necessary this check? Should be always false. TO CHECK <-- NO
                    if k in deployments[job_context.job.name].keys():
                        raise WorkflowExecutionException(
                            f"Scheduling failed: The deployment {k} can have just one location. "
                            f"Instead got: {[deployments[job_context.job.name], v]}"
                        )
                    deployments[job_context.job.name].setdefault(k, []).append(v)
        return deployments

    async def _get_locations(
        self,
        scheduling_policy: Policy,
        available_locations: MutableMapping[
            str, MutableMapping[str, AvailableLocation]
        ],
    ) -> MutableMapping[str, MutableSequence[ExecutionLocation]]:
        jobs_to_schedule = await scheduling_policy.get_location(
            context=self.context,
            pending_jobs=self.pending_jobs,
            available_locations=available_locations,
            scheduled_jobs=self.job_allocations,
            locations=self.location_allocations,
        )
        return {
            job_name: [loc.location for loc in available_locations]
            for job_name, available_locations in jobs_to_schedule.items()
        }

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

    async def _scheduling_task(self):
        try:
            while True:
                await self.pending_job_event.wait()
                if self.pending_jobs:
                    logger.info("Start scheduling")
                    deployments = {
                        target.deployment
                        for job_context in self.pending_jobs
                        for target in job_context.targets
                    }
                    for deployment in deployments:
                        targets = {
                            target
                            for job_context in self.pending_jobs
                            for target in job_context.targets
                            if target.deployment == deployment
                        }

                    deployments = await self._get_deployments()

                    # todo: each job can have different target and the targets can have different policies.
                    #  To think how to manage it
                    target = self.pending_jobs[0].targets[0]

                    jobs_to_schedule = await self._get_locations(
                        self._get_policy(target.scheduling_policy), deployments
                    )
                    for job_name, locs in jobs_to_schedule.items():
                        job_context = next(
                            job_context
                            for job_context in self.pending_jobs
                            if job_context.job.name == job_name
                        )
                        self.pending_jobs.remove(job_context)
                        self._allocate_job(
                            job_context.job,
                            job_context.hardware_requirement,
                            locs,
                            job_context.targets[0],
                        )
                        job_context.event.set()
                logger.info("Sleep")
                if not self.pending_jobs:
                    self.pending_job_event.clear()
                # await asyncio.sleep(5)
        except Exception as e:
            logger.exception(f"Scheduler failed: {e}")
            raise

    async def close(self):
        self.scheduling_task.cancel()

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("scheduler.json")
            .read_text("utf-8")
        )

    async def notify_status(self, job_name: str, status: Status) -> None:
        if job_name in self.job_allocations:
            if status != self.job_allocations[job_name].status:
                self.job_allocations[job_name].status = status
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Job {job_name} changed status to {status.name}")

                # Notify scheduling loop: there are free resources
                self.pending_job_event.set()

    async def schedule(
        self, job: Job, binding_config: BindingConfig, hardware_requirement: Hardware
    ) -> None:
        logger.info(f"Adding job {job.name} in pending jobs to schedule")
        targets = list(binding_config.targets)
        for f in (self._get_binding_filter(f) for f in binding_config.filters):
            targets = await f.get_targets(job, targets)
        job_context = JobContext(job, targets, hardware_requirement)
        self.pending_jobs.append(job_context)

        # Notify scheduling loop: there is a job to schedule
        self.pending_job_event.set()

        # Wait the job is scheduled
        await job_context.event.wait()
