from __future__ import annotations

from abc import abstractmethod, ABC
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from streamflow.core.workflow import Job
    from typing import List, MutableMapping, Optional
    from typing_extensions import Text


class JobAllocation(object):
    __slots__ = ('job', 'resources', 'status')

    def __init__(self,
                 job: Job,
                 resources: List[Text],
                 status: JobStatus):
        self.job: Job = job
        self.resources: List[Text] = resources
        self.status: JobStatus = status


class JobStatus(Enum):
    RUNNING = 1
    SKIPPED = 2
    COMPLETED = 3
    FAILED = 4
    CANCELLED = 5


class Policy(ABC):

    @abstractmethod
    def get_resource(self,
                     job: Job,
                     available_resources: MutableMapping[Text, Resource],
                     jobs: MutableMapping[Text, JobAllocation],
                     resources: MutableMapping[Text, ResourceAllocation]) -> Optional[Text]: ...


class Resource(object):

    def __init__(self,
                 name: Text,
                 hostname: Text):
        self.name: Text = name
        self.hostname: Text = hostname


class ResourceAllocation(object):
    __slots__ = ('name', 'model', 'jobs')

    def __init__(self,
                 name: Text,
                 model: Text):
        self.name: Text = name
        self.model: Text = model
        self.jobs: List[Text] = []


class Scheduler(ABC):

    def __init__(self):
        self.job_allocations: MutableMapping[Text, JobAllocation] = {}
        self.resource_allocations: MutableMapping[Text, ResourceAllocation] = {}

    def get_job(self, job_name: Text) -> Optional[Job]:
        job = self.job_allocations.get(job_name, None)
        return job.job if job is not None else None

    def get_resources(self, job_name: Text) -> List[Text]:
        job = self.job_allocations.get(job_name, None)
        return job.resources if job is not None else []

    @abstractmethod
    async def notify_status(self,
                            job_name: Text,
                            status: JobStatus):
        ...

    @abstractmethod
    async def schedule(self,
                       job: Job,
                       scheduling_policy: Policy = None):
        ...
