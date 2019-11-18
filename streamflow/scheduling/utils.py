from enum import Enum
from typing import List, Set, MutableMapping, Any


class JobStatus(Enum):
    running = 1
    completed = 2
    failed = 3


class TaskDescription(object):

    def __init__(self, name, kwargs=None) -> None:
        super().__init__()
        self.name: str = name
        self.requirements: MutableMapping[str, Any] = {}
        self.dependencies: List[str] = []
        self.outputs: List[str] = []
        if kwargs:
            for k, v in kwargs.items():
                if hasattr(self, k):
                    setattr(self, k, v)

    def add_dependency(self,
                       path: str):
        self.dependencies.append(path)

    def add_output(self,
                   path: str):
        self.outputs.append(path)

    def add_requirement(self,
                        resource: str,
                        value: int):
        self.requirements[resource] = value


class JobAllocation(object):

    def __init__(self,
                 name: str,
                 description: TaskDescription,
                 service: str,
                 resource: str,
                 status: JobStatus
                 ):
        self.name: str = name
        self.description = description
        self.service: str = service
        self.resource: str = resource
        self.status: JobStatus = status


class ResourceAllocation(object):

    def __init__(self,
                 name: str,
                 model: str):
        self.name: str = name
        self.model: str = model
        self.services: Set[str] = set()
        self.jobs: List[str] = []
