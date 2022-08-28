from __future__ import annotations

import asyncio
import os
from asyncio import Condition
from typing import MutableSequence, Optional, TYPE_CHECKING

import pkg_resources

from streamflow.core.config import Config
from streamflow.core.data import LOCAL_LOCATION
from streamflow.core.deployment import Target
from streamflow.core.scheduling import Hardware, JobAllocation, LocationAllocation, Policy, Scheduler
from streamflow.core.workflow import Job, Status
from streamflow.log_handler import logger
from streamflow.scheduling.policy import policy_classes

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.scheduling import Location
    from typing import MutableMapping


class DefaultScheduler(Scheduler):

    def __init__(self,
                 context: StreamFlowContext,
                 retry_delay: Optional[int] = None) -> None:
        super().__init__(context)
        self.allocation_groups: MutableMapping[str, MutableSequence[Job]] = {}
        self.policy_map: MutableMapping[str, Policy] = {}
        self.retry_interval: Optional[int] = retry_delay
        self.scheduling_groups: MutableMapping[str, MutableSequence[str]] = {}
        self.wait_queues: MutableMapping[str, Condition] = {}

    def _allocate_job(self,
                      job: Job,
                      hardware: Hardware,
                      selected_locations: MutableSequence[str],
                      target: Target):
        if len(selected_locations) == 1:
            logger.debug(
                "Job {name} allocated {location}".format(
                    name=job.name,
                    location=("locally" if selected_locations[0] == LOCAL_LOCATION else
                              "on location {loc}".format(loc=selected_locations[0]))))
        else:
            logger.debug(
                "Job {name} allocated on locations {locations}".format(
                    name=job.name,
                    locations=', '.join(selected_locations)))
        self.job_allocations[job.name] = JobAllocation(
            job=job.name,
            target=target,
            locations=selected_locations,
            status=Status.FIREABLE,
            hardware=hardware or Hardware())
        for selected_location in selected_locations:
            if selected_location not in self.location_allocations:
                self.location_allocations[selected_location] = LocationAllocation(
                    name=selected_location,
                    deployment=target.deployment.name)
            self.location_allocations[selected_location].jobs.append(job.name)

    def _deallocate_job(self, job: str):
        job_allocation = self.job_allocations.pop(job)
        for selected_location in job_allocation.locations:
            self.location_allocations[selected_location].jobs.remove(job)
        if len(job_allocation.locations) == 1:
            logger.info(
                "Job {name} deallocated {location}".format(
                    name=job,
                    location=("from local location" if job_allocation.locations[0] == LOCAL_LOCATION else
                              "from location {loc}".format(loc=job_allocation.locations[0]))))
        else:
            logger.info(
                "Job {name} deallocated from locations {locations}".format(
                    name=job,
                    locations=', '.join(job_allocation.locations)))

    async def _get_locations(self,
                             job: Job,
                             deployment: str,
                             hardware_requirement: Hardware,
                             locations: int,
                             scheduling_policy: Policy,
                             available_locations: MutableMapping[str, Location]) -> Optional[MutableSequence[str]]:
        selected_locations = []
        for i in range(locations):
            selected_location = await scheduling_policy.get_location(
                context=self.context,
                job=job,
                deployment=deployment,
                hardware_requirement=hardware_requirement,
                available_locations=available_locations,
                jobs=self.job_allocations,
                locations=self.location_allocations)
            if selected_location is not None:
                selected_locations.append(selected_location)
                available_locations.pop(selected_location)
            else:
                return None
        return selected_locations

    def _get_policy(self, policy_config: Config):
        if policy_config.name not in self.policy_map:
            self.policy_map[policy_config.name] = policy_classes[policy_config.type](**policy_config.config)
        return self.policy_map[policy_config.name]

    async def close(self):
        pass

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join('schemas', 'scheduler.json'))

    async def notify_status(self, job_name: str, status: Status) -> None:
        connector = self.get_connector(job_name)
        if connector:
            if connector.deployment_name in self.wait_queues:
                async with self.wait_queues[connector.deployment_name]:
                    if job_name in self.job_allocations:
                        if status != self.job_allocations[job_name].status:
                            self.job_allocations[job_name].status = status
                            logger.debug(
                                "Job {name} changed status to {status}".format(name=job_name, status=status.name))
                        if status in [Status.COMPLETED, Status.FAILED]:
                            self.wait_queues[connector.deployment_name].notify_all()

    async def schedule(self,
                       job: Job,
                       target: Target,
                       hardware_requirement: Hardware):
        deployment = target.deployment.name
        if deployment not in self.wait_queues:
            self.wait_queues[deployment] = Condition()
        async with self.wait_queues[deployment]:
            while True:
                connector = self.context.deployment_manager.get_connector(target.deployment.name)
                logger.debug("Retreiving available locations for job {}".format(job.name))
                available_locations = dict(await connector.get_available_locations(
                    service=target.service,
                    input_directory=job.input_directory,
                    output_directory=job.output_directory,
                    tmp_directory=job.tmp_directory))
                logger.debug("Available locations for job {} are {}".format(
                    job.name, {k: l.hostname for k, l in available_locations.items()}))
                if available_locations:
                    if target.scheduling_group is not None:
                        if target.scheduling_group not in self.allocation_groups:
                            self.allocation_groups[target.scheduling_group] = []
                        self.allocation_groups[target.scheduling_group].append(job)
                        group_size = len(self.scheduling_groups[target.scheduling_group])
                        if len(self.allocation_groups.get(target.scheduling_group, [])) == group_size:
                            allocated_jobs = []
                            for j in self.allocation_groups[target.scheduling_group]:
                                selected_locations = await self._get_locations(
                                    job=job,
                                    deployment=target.deployment.name,
                                    hardware_requirement=hardware_requirement,
                                    locations=target.locations,
                                    scheduling_policy=self._get_policy(target.scheduling_policy),
                                    available_locations=available_locations)
                                if selected_locations is None:
                                    break
                                self._allocate_job(
                                    job=j,
                                    hardware=hardware_requirement,
                                    selected_locations=selected_locations,
                                    target=target)
                                allocated_jobs.append(j)
                            if len(allocated_jobs) < group_size:
                                for j in allocated_jobs:
                                    self._deallocate_job(j.name)
                            else:
                                return
                    else:
                        selected_locations = await self._get_locations(
                            job=job,
                            deployment=target.deployment.name,
                            hardware_requirement=hardware_requirement,
                            locations=target.locations,
                            scheduling_policy=self._get_policy(target.scheduling_policy),
                            available_locations=available_locations)
                        if selected_locations is not None:
                            self._allocate_job(
                                job=job,
                                hardware=hardware_requirement,
                                selected_locations=selected_locations,
                                target=target)
                            return
                try:
                    await asyncio.wait_for(self.wait_queues[deployment].wait(), timeout=self.retry_interval)
                except asyncio.TimeoutError:
                    pass
