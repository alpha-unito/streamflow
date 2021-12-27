from __future__ import annotations

import asyncio
from asyncio import Condition
from typing import TYPE_CHECKING, MutableSequence, Optional

from streamflow.core.data import LOCAL_LOCATION
from streamflow.core.scheduling import LocationAllocation, JobAllocation, Scheduler, Hardware
from streamflow.core.workflow import Status
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.scheduling import Location
    from streamflow.core.workflow import Job
    from streamflow.scheduling.policy import Policy
    from typing import MutableMapping


class DefaultScheduler(Scheduler):

    def __init__(self,
                 context: StreamFlowContext,
                 default_policy: Policy,
                 retry_delay: Optional[int] = None) -> None:
        super().__init__()
        self.allocation_groups: MutableMapping[str, MutableSequence[Job]] = {}
        self.scheduling_groups: MutableMapping[str, MutableSequence[str]] = {}
        self.context: StreamFlowContext = context
        self.default_policy: Policy = default_policy
        self.retry_interval: Optional[int] = retry_delay
        self.wait_queue: Condition = Condition()

    def _allocate_job(self,
                      job: Job,
                      deployment_name: str,
                      selected_locations: MutableSequence[str]):
        if len(selected_locations) == 1:
            logger.info(
                "Job {name} allocated {location}".format(
                    name=job.name,
                    location=("locally" if selected_locations[0] == LOCAL_LOCATION else
                              "on location {loc}".format(loc=selected_locations[0]))))
        else:
            logger.info(
                "Job {name} allocated on locations {locations}".format(
                    name=job.name,
                    locations=', '.join(selected_locations)))
        self.job_allocations[job.name] = JobAllocation(
            job=job,
            locations=selected_locations,
            status=Status.RUNNING,
            hardware=job.hardware or Hardware())
        for selected_location in selected_locations:
            if selected_location not in self.location_allocations:
                self.location_allocations[selected_location] = LocationAllocation(selected_location, deployment_name)
            self.location_allocations[selected_location].jobs.append(job.name)

    def _deallocate_job(self, job: Job):
        job_allocation = self.job_allocations.pop(job.name)
        for selected_location in job_allocation.locations:
            self.location_allocations[selected_location].jobs.remove(job.name)
        if len(job_allocation.locations) == 1:
            logger.info(
                "Job {name} deallocated {location}".format(
                    name=job.name,
                    location=("from local location" if job_allocation.locations[0] == LOCAL_LOCATION else
                              "from location {loc}".format(loc=job_allocation.locations[0]))))
        else:
            logger.info(
                "Job {name} deallocated from locations {locations}".format(
                    name=job.name,
                    locations=', '.join(job_allocation.locations)))

    def _get_locations(self,
                       job: Job,
                       scheduling_policy: Policy,
                       available_locations: MutableMapping[str, Location]) -> Optional[MutableSequence[str]]:
        selected_locations = []
        for i in range(job.step.target.locations):
            selected_location = (scheduling_policy or self.default_policy).get_location(
                job, available_locations, self.job_allocations, self.location_allocations)
            if selected_location is not None:
                selected_locations.append(selected_location)
                available_locations.pop(selected_location)
            else:
                return None
        return selected_locations

    async def notify_status(self, job_name: str, status: Status) -> None:
        async with self.wait_queue:
            if job_name in self.job_allocations:
                if status != self.job_allocations[job_name].status:
                    self.job_allocations[job_name].status = status
                    logger.info(
                        "Job {name} changed status to {status}".format(name=job_name, status=status.name))
                if status in [Status.COMPLETED, Status.FAILED]:
                    self.wait_queue.notify_all()

    async def schedule(self,
                       job: Job,
                       scheduling_policy: Policy = None) -> None:
        async with self.wait_queue:
            deployment_name = job.step.target.deployment.name
            connector = self.context.deployment_manager.get_connector(deployment_name)
            while True:
                if job.name in self.job_allocations:
                    return
                available_locations = dict(await connector.get_available_locations(job.step.target.service))
                if job.step.scheduling_group is not None:
                    if job.step.scheduling_group not in self.allocation_groups:
                        self.allocation_groups[job.step.scheduling_group] = []
                    self.allocation_groups[job.step.scheduling_group].append(job)
                    group_size = len(self.scheduling_groups[job.step.scheduling_group])
                    if len(self.allocation_groups.get(job.step.scheduling_group, [])) == group_size:
                        allocated_jobs = []
                        for j in self.allocation_groups[job.step.scheduling_group]:
                            selected_locations = self._get_locations(j, scheduling_policy, available_locations)
                            if selected_locations is None:
                                break
                            self._allocate_job(j, deployment_name, selected_locations)
                            allocated_jobs.append(j)
                        if len(allocated_jobs) < group_size:
                            for j in allocated_jobs:
                                self._deallocate_job(j)
                        else:
                            return
                else:
                    selected_locations = self._get_locations(job, scheduling_policy, available_locations)
                    if selected_locations is not None:
                        self._allocate_job(job, deployment_name, selected_locations)
                        return
                try:
                    await asyncio.wait_for(self.wait_queue.wait(), timeout=self.retry_interval)
                except asyncio.TimeoutError:
                    pass
