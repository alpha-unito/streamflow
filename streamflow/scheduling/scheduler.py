from __future__ import annotations

from asyncio import Condition
from typing import TYPE_CHECKING, List, Optional

from streamflow.core.scheduling import ResourceAllocation, JobAllocation, Scheduler, JobStatus
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from streamflow.core.context import StreamflowContext
    from streamflow.core.scheduling import Resource
    from streamflow.core.workflow import Job
    from streamflow.scheduling.policy import Policy
    from typing import MutableMapping
    from typing_extensions import Text


class DefaultScheduler(Scheduler):

    def __init__(self,
                 context: StreamflowContext,
                 default_policy: Policy) -> None:
        super().__init__()
        self.context: StreamflowContext = context
        self.default_policy: Policy = default_policy
        self.wait_queue: Condition = Condition()

    async def _get_resources(self,
                             job: Job,
                             scheduling_policy: Policy,
                             available_resources: MutableMapping[Text, Resource]) -> Optional[List[Text]]:
        selected_resources = []
        for i in range(job.task.target.resources):
            selected_resource = (scheduling_policy or self.default_policy).get_resource(
                job, available_resources, self.job_allocations, self.resource_allocations)
            if selected_resource is not None:
                selected_resources.append(selected_resource)
                available_resources.pop(selected_resource)
            else:
                return None
        return selected_resources

    async def notify_status(self, job_name: str, status: JobStatus) -> None:
        async with self.wait_queue:
            if job_name in self.job_allocations:
                self.job_allocations[job_name].status = status
                logger.info(
                    "Job {name} changed status to {status}".format(name=job_name, status=status.name))
                if status in [JobStatus.COMPLETED, JobStatus.FAILED]:
                    self.wait_queue.notify_all()

    async def schedule(self,
                       job: Job,
                       scheduling_policy: Policy = None) -> None:
        async with self.wait_queue:
            model_name = job.task.target.model.name
            connector = self.context.deployment_manager.get_connector(model_name)
            available_resources = await connector.get_available_resources(job.task.target.service)
            while True:
                selected_resources = await self._get_resources(job, scheduling_policy, available_resources)
                if selected_resources is not None:
                    break
                await self.wait_queue.wait()
        if len(selected_resources) == 1:
            logger.info(
                "Job {name} allocated on resource {resource}".format(name=job.name, resource=selected_resources[0]))
        else:
            logger.info(
                "Job {name} allocated on resources {resources}".format(
                    name=job.name,
                    resources=', '.join(selected_resources)))
        self.job_allocations[job.name] = JobAllocation(job, selected_resources, JobStatus.RUNNING)
        for selected_resource in selected_resources:
            if selected_resource not in self.resource_allocations:
                self.resource_allocations[selected_resource] = ResourceAllocation(selected_resource, model_name)
            self.resource_allocations[selected_resource].jobs.append(job.name)
