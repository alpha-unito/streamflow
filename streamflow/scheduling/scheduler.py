from __future__ import annotations

import asyncio
from asyncio import Condition
from typing import TYPE_CHECKING, MutableSequence, Optional

from streamflow.core.data import LOCAL_RESOURCE
from streamflow.core.scheduling import ResourceAllocation, JobAllocation, Scheduler, Hardware
from streamflow.core.workflow import Status
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.scheduling import Resource
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
                      model_name: str,
                      selected_resources: MutableSequence[str]):
        if len(selected_resources) == 1:
            logger.info(
                "Job {name} allocated {resource}".format(
                    name=job.name,
                    resource=("locally" if selected_resources[0] == LOCAL_RESOURCE else
                              "on resource {res}".format(res=selected_resources[0]))))
        else:
            logger.info(
                "Job {name} allocated on resources {resources}".format(
                    name=job.name,
                    resources=', '.join(selected_resources)))
        self.job_allocations[job.name] = JobAllocation(
            job=job,
            resources=selected_resources,
            status=Status.RUNNING,
            hardware=job.hardware or Hardware())
        for selected_resource in selected_resources:
            if selected_resource not in self.resource_allocations:
                self.resource_allocations[selected_resource] = ResourceAllocation(selected_resource, model_name)
            self.resource_allocations[selected_resource].jobs.append(job.name)

    def _deallocate_job(self, job: Job):
        job_allocation = self.job_allocations.pop(job.name)
        for selected_resource in job_allocation.resources:
            self.resource_allocations[selected_resource].jobs.remove(job.name)
        if len(job_allocation.resources) == 1:
            logger.info(
                "Job {name} deallocated {resource}".format(
                    name=job.name,
                    resource=("from local resource" if job_allocation.resources[0] == LOCAL_RESOURCE else
                              "from resource {res}".format(res=job_allocation.resources[0]))))
        else:
            logger.info(
                "Job {name} deallocated from resources {resources}".format(
                    name=job.name,
                    resources=', '.join(job_allocation.resources)))

    def _get_resources(self,
                       job: Job,
                       scheduling_policy: Policy,
                       available_resources: MutableMapping[str, Resource]) -> Optional[MutableSequence[str]]:
        selected_resources = []
        for i in range(job.step.target.resources):
            selected_resource = (scheduling_policy or self.default_policy).get_resource(
                job, available_resources, self.job_allocations, self.resource_allocations)
            if selected_resource is not None:
                selected_resources.append(selected_resource)
                available_resources.pop(selected_resource)
            else:
                return None
        return selected_resources

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
            model_name = job.step.target.model.name
            connector = self.context.deployment_manager.get_connector(model_name)
            while True:
                if job.name in self.job_allocations:
                    return
                available_resources = dict(await connector.get_available_resources(job.step.target.service))
                if job.step.scheduling_group is not None:
                    if job.step.scheduling_group not in self.allocation_groups:
                        self.allocation_groups[job.step.scheduling_group] = []
                    self.allocation_groups[job.step.scheduling_group].append(job)
                    group_size = len(self.scheduling_groups[job.step.scheduling_group])
                    if len(self.allocation_groups.get(job.step.scheduling_group, [])) == group_size:
                        allocated_jobs = []
                        for j in self.allocation_groups[job.step.scheduling_group]:
                            selected_resources = self._get_resources(j, scheduling_policy, available_resources)
                            if selected_resources is None:
                                break
                            self._allocate_job(j, model_name, selected_resources)
                            allocated_jobs.append(j)
                        if len(allocated_jobs) < group_size:
                            for j in allocated_jobs:
                                self._deallocate_job(j)
                        else:
                            return
                else:
                    selected_resources = self._get_resources(job, scheduling_policy, available_resources)
                    if selected_resources is not None:
                        self._allocate_job(job, model_name, selected_resources)
                        return
                try:
                    await asyncio.wait_for(self.wait_queue.wait(), timeout=self.retry_interval)
                except asyncio.TimeoutError:
                    pass
