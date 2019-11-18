from threading import RLock, Condition
from typing import List, MutableMapping

from streamflow.data.data_manager import RemotePath
from streamflow.log_handler import _logger
from streamflow.scheduling.policy import Policy, DataLocalityPolicy
from streamflow.scheduling.utils import ResourceAllocation, JobAllocation, JobStatus, TaskDescription


class Scheduler(object):

    def __init__(self) -> None:
        super().__init__()
        self.resources: MutableMapping[str, ResourceAllocation] = {}
        self.jobs: MutableMapping[str, JobAllocation] = {}
        self.remote_paths: MutableMapping[str, List[RemotePath]] = {}
        self.default_policy: Policy = DataLocalityPolicy()
        self.lock: RLock = RLock()
        self.wait_queue: Condition = Condition(self.lock)

    def get_resource(self,
                     resource_name: str) -> ResourceAllocation:
        return self.resources[resource_name]

    def get_service(self,
                    job_name: str) -> str:
        return self.jobs[job_name].service

    def notify_status(self,
                      job_name: str,
                      status: JobStatus) -> None:
        with self.wait_queue:
            self.jobs[job_name].status = status
            self.wait_queue.notify_all()

    def schedule(self,
                 task_description: TaskDescription,
                 model_name: str,
                 target_service: str,
                 available_resources: List[str],
                 remote_paths: MutableMapping[str, List[RemotePath]],
                 scheduling_policy: Policy = None) -> str:
        with self.wait_queue:
            job_name = task_description.name
            while True:
                selected_resource = \
                    (scheduling_policy or self.default_policy).get_resource(task_description,
                                                                            available_resources,
                                                                            remote_paths,
                                                                            self.jobs,
                                                                            self.resources)
                if selected_resource is not None:
                    break
                self.wait_queue.wait()
            _logger.info(
                "Task {name} allocated on resource {resource}".format(name=job_name, resource=selected_resource))
            self.jobs[job_name] = JobAllocation(job_name, task_description, target_service, selected_resource,
                                                JobStatus.running)
            if selected_resource not in self.resources:
                self.resources[selected_resource] = ResourceAllocation(selected_resource, model_name)
            self.resources[selected_resource].jobs.append(job_name)
            self.resources[selected_resource].services.add(target_service)
            return selected_resource
