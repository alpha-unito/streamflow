from __future__ import annotations

import asyncio
import logging
import posixpath
from typing import MutableSequence, TYPE_CHECKING

from importlib_resources import files

from streamflow.core.config import BindingConfig, Config
from streamflow.core.deployment import (
    BindingFilter,
    ExecutionLocation,
    FilterConfig,
    Target,
)
from streamflow.core.scheduling import (
    JobAllocation,
    LocationAllocation,
    Policy,
    Scheduler,
    JobContext,
    JobHardwareRequirement,
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
        self.pending_jobs: MutableMapping[str, MutableSequence[JobContext]] = {}
        self.pending_jobs_conditional = asyncio.Condition()
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
        hardware: JobHardwareRequirement,
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

    async def _get_jobs_to_schedule(
        self,
        scheduling_policy: Policy,
        job_contexts: MutableSequence[JobContext],
        valid_locations: MutableMapping[str, AvailableLocation],
    ) -> MutableMapping[str, MutableSequence[ExecutionLocation]]:
        scheduled_jobs = []
        for valid_location in valid_locations.values():
            if valid_location.name in self.location_allocations.get(
                valid_location.deployment, {}
            ):
                for job_name in self.location_allocations[valid_location.deployment][
                    valid_location.name
                ].jobs:
                    scheduled_jobs.append(self.job_allocations[job_name])

        jobs_to_schedule = await scheduling_policy.get_location(
            context=self.context,
            pending_jobs=job_contexts,
            available_locations=valid_locations,
            scheduled_jobs=scheduled_jobs,
            locations=self.location_allocations,
        )
        return {
            job_name: [available_location.location]
            for job_name, available_location in jobs_to_schedule.items()
        }

    def _get_policy(self, config: Config):
        if config.name not in self.policy_map:
            self.policy_map[config.name] = policy_classes[config.type](**config.config)
        return self.policy_map[config.name]

    async def _get_available_locations(
        self, target: Target, job_contexts: MutableSequence[JobContext]
    ) -> MutableMapping[str, AvailableLocation]:
        available_locations = {}
        for job_context in job_contexts:
            directories = {
                job_context.job.input_directory or target.workdir,
                job_context.job.tmp_directory or target.workdir,
                job_context.job.output_directory or target.workdir,
            }
            connector = self.context.deployment_manager.get_connector(
                target.deployment.name
            )
            available_locations = await connector.get_available_locations(
                service=target.service, directories=list(directories)
            )
        return available_locations

    async def _scheduling_task(self):
        try:
            while True:
                # Events that awake the scheduling task:
                #   - there is a new job to schedule
                #   - some resources are released
                await self.pending_job_event.wait()
                async with self.pending_jobs_conditional:
                    self.pending_job_event.clear()
                for deployment_name, job_contexts in self.pending_jobs.items():
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(
                            f"Checking jobs scheduling on deployment {deployment_name}"
                        )

                    # deployments = {
                    #     target.deployment
                    #     for job_context in self.pending_jobs
                    #     for target in job_context.targets
                    # }
                    # for deployment in deployments:
                    #     targets = {
                    #         target
                    #         for job_context in self.pending_jobs
                    #         for target in job_context.targets
                    #         if target.deployment == deployment
                    #     }

                    if not job_contexts:
                        continue
                    target = next(
                        target
                        for job_context in job_contexts
                        for target in job_context.targets
                        if target.deployment.name == deployment_name
                    )
                    valid_locations = await self._get_available_locations(
                        target, job_contexts
                    )

                    scheduling_policy = self._get_policy(target.deployment.policy)
                    jobs_to_schedule = await self._get_jobs_to_schedule(
                        scheduling_policy,
                        job_contexts,
                        valid_locations,
                    )
                    if logger.isEnabledFor(logging.DEBUG):
                        if jobs_to_schedule:
                            for job_name, locs in jobs_to_schedule.items():
                                logger.debug(
                                    "Available locations for job {} on {} are {}.".format(
                                        job_name,
                                        (
                                            posixpath.join(
                                                deployment_name, target.service
                                            )
                                            if target.service
                                            else deployment_name
                                        ),
                                        {str(loc) for loc in locs},
                                    )
                                )
                        else:
                            logger.debug(
                                "No location available for job {} on deployment {}.".format(
                                    [
                                        job_context.job.name
                                        for job_context in job_contexts
                                    ],
                                    (
                                        posixpath.join(deployment_name, target.service)
                                        if target.service
                                        else deployment_name
                                    ),
                                )
                            )

                    for job_name, locs in jobs_to_schedule.items():
                        job_context = next(
                            job_context
                            for job_context in job_contexts
                            if job_context.job.name == job_name
                        )
                        self.pending_jobs[deployment_name].remove(job_context)
                        self._allocate_job(
                            job_context.job,
                            job_context.hardware_requirement,
                            locs,
                            job_context.targets[0],
                        )
                        job_context.event.set()
                        # todo: fix job with multi-targets case
        except Exception as e:
            # todo: propagate error to context (?)
            logger.exception(f"Scheduler failed: {e}")
            await self.context.close()
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

                # Notify scheduling task: there are free resources
                async with self.pending_jobs_conditional:
                    self.pending_job_event.set()

    async def schedule(
        self,
        job: Job,
        binding_config: BindingConfig,
        hardware_requirement: JobHardwareRequirement,
    ) -> None:
        logger.info(f"Adding job {job.name} in pending jobs to schedule")
        targets = list(binding_config.targets)
        for f in (self._get_binding_filter(f) for f in binding_config.filters):
            targets = await f.get_targets(job, targets)
        job_context = JobContext(job, targets, hardware_requirement)
        for target in targets:
            deployment = target.deployment
            self.pending_jobs.setdefault(deployment.name, []).append(job_context)

        # Notify scheduling task: there is a job to schedule
        async with self.pending_jobs_conditional:
            self.pending_job_event.set()

        # Wait the job is scheduled
        await job_context.event.wait()
        logger.info(f"Job {job.name} scheduled")
