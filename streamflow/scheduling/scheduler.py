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


def _get_job_contexts_hardware_requirement(
    job_contexts: MutableSequence[JobContext], target: Target
) -> MutableMapping[str, Hardware]:
    job_hardware_requirements = {}
    for job_context in job_contexts:
        hardware_requirement = None
        if job_context.hardware_requirement:
            storage = {}
            for path, size in job_context.hardware_requirement.storage.items():
                key = path
                if key == "tmp_directory":
                    key = target.workdir
                elif key == "output_directory":
                    key = target.workdir

                if key not in storage.keys():
                    storage[key] = size
                else:
                    # `tmp_directory` and `output_directory` are in the same volume
                    storage[key] += size
            hardware_requirement = Hardware(
                cores=job_context.hardware_requirement.cores,
                memory=job_context.hardware_requirement.memory,
                storage=storage,
            )
        job_hardware_requirements[job_context.job.name] = hardware_requirement
    return job_hardware_requirements


class DefaultScheduler(Scheduler):
    def __init__(
        self, context: StreamFlowContext, retry_delay: int | None = None
    ) -> None:
        super().__init__(context)
        self.binding_filter_map: MutableMapping[str, BindingFilter] = {}
        self.pending_jobs: MutableMapping[str, MutableSequence[JobContext]] = {}
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

    async def _get_jobs_to_schedule(
        self,
        scheduling_policy: Policy,
        job_contexts: MutableSequence[JobContext],
        hardware_requirements: MutableMapping[str, Hardware],
        valid_locations: MutableMapping[str, AvailableLocation],
    ) -> MutableMapping[str, MutableSequence[ExecutionLocation]]:
        jobs_to_schedule = await scheduling_policy.get_location(
            context=self.context,
            pending_jobs=[j.job for j in job_contexts],
            hardware_requirements=hardware_requirements,
            available_locations=valid_locations,
            scheduled_jobs=self.job_allocations,
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
                await self.pending_job_event.wait()
                for deployment_name, job_contexts in self.pending_jobs.items():
                    logger.info("Start scheduling")

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

                    hardware_requirements = _get_job_contexts_hardware_requirement(
                        job_contexts, target
                    )

                    scheduling_policy = self._get_policy(target.deployment.policy)
                    jobs_to_schedule = await self._get_jobs_to_schedule(
                        scheduling_policy,
                        job_contexts,
                        hardware_requirements,
                        valid_locations,
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

                        # todo: awake scheduling:
                        #   - there is a new job to schedule
                        #   - some resources are released and there are some pending jobs
                        # self.pending_job_event.clear()
                logger.info("Sleep")
                await asyncio.sleep(5)
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
        for target in targets:
            deployment = target.deployment
            self.pending_jobs.setdefault(deployment.name, []).append(job_context)

        # Notify scheduling loop: there is a job to schedule
        self.pending_job_event.set()

        # Wait the job is scheduled
        await job_context.event.wait()
        logger.info(f"Job {job.name} scheduled")
