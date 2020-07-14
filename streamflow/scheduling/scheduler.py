from __future__ import annotations

from asyncio import Condition
from typing import TYPE_CHECKING

from streamflow.core.scheduling import ResourceAllocation, JobAllocation, Scheduler, JobStatus
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from streamflow.core.context import StreamflowContext
    from streamflow.core.workflow import Job
    from streamflow.scheduling.policy import Policy


class DefaultScheduler(Scheduler):

    def __init__(self,
                 context: StreamflowContext,
                 default_policy: Policy) -> None:
        super().__init__()
        self.context: StreamflowContext = context
        self.default_policy: Policy = default_policy
        self.wait_queue: Condition = Condition()

    async def notify_status(self, job_name: str, status: JobStatus) -> None:
        async with self.wait_queue:
            self.jobs[job_name].status = status
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
                selected_resource = \
                    (scheduling_policy or self.default_policy).get_resource(job,
                                                                            available_resources,
                                                                            self.jobs,
                                                                            self.resources)
                if selected_resource is not None:
                    break
                await self.wait_queue.wait()
        logger.info(
            "Job {name} allocated on resource {resource}".format(name=job.name, resource=selected_resource))
        self.jobs[job.name] = JobAllocation(job, selected_resource, JobStatus.RUNNING)
        if selected_resource not in self.resources:
            self.resources[selected_resource] = ResourceAllocation(selected_resource, model_name)
        self.resources[selected_resource].jobs.append(job.name)
