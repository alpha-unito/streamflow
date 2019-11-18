from abc import abstractmethod, ABCMeta
from typing import List, MutableMapping, Optional

from streamflow.data.utils import RemotePath
from streamflow.scheduling.utils import JobAllocation, ResourceAllocation, TaskDescription, JobStatus


class Policy(object, metaclass=ABCMeta):

    @abstractmethod
    def get_resource(self,
                     task_description: TaskDescription,
                     available_resources: List[str],
                     remote_paths: MutableMapping[str, List[RemotePath]],
                     jobs: MutableMapping[str, JobAllocation],
                     resources: MutableMapping[str, ResourceAllocation]) -> Optional[str]: ...


class DataLocalityPolicy(Policy):

    def __init__(self) -> None:
        super().__init__()

    def get_resource(self,
                     task_description: TaskDescription,
                     available_resources: List[str],
                     remote_paths: MutableMapping[str, List[RemotePath]],
                     jobs: MutableMapping[str, JobAllocation],
                     resources: MutableMapping[str, ResourceAllocation]) -> Optional[str]:
        valid_resources = list(available_resources)
        for dep in task_description.dependencies:
            for job in jobs.values():
                if dep in job.description.outputs and job.resource in valid_resources:
                    running_jobs = list(
                        filter(lambda x: jobs[x].status == JobStatus.running, resources[job.resource].jobs))
                    if len(running_jobs) == 0:
                        return job.resource
                    valid_resources.remove(job.resource)
        for resource in valid_resources:
            if resource not in resources:
                return resource
            running_jobs = list(
                filter(lambda x: jobs[x].status == JobStatus.running, resources[resource].jobs))
            if len(running_jobs) == 0:
                return resource
        return None
