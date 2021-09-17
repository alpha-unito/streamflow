from __future__ import annotations

from typing import TYPE_CHECKING, MutableSequence

from streamflow.core.scheduling import Policy, Hardware
from streamflow.core.workflow import Status

if TYPE_CHECKING:
    from streamflow.core.scheduling import JobAllocation, Resource, ResourceAllocation
    from streamflow.core.workflow import Job
    from typing import MutableMapping, Optional


def _is_valid(current_resource: str,
              job: Job,
              available_resources: MutableMapping[str, Resource],
              jobs: MutableMapping[str, JobAllocation],
              resources: MutableMapping[str, ResourceAllocation]) -> bool:
    resource_obj = available_resources[current_resource]
    running_jobs = list(
        filter(lambda x: jobs[x].status == Status.RUNNING,
               resources[current_resource].jobs)) if current_resource in resources else []
    # If resource is segmentable and job provides requirements, compute the used amount of resources
    if resource_obj.hardware is not None and job.hardware is not None:
        used_hardware = sum((jobs[j].hardware for j in running_jobs), start=Hardware())
        if (resource_obj.hardware - used_hardware) >= job.hardware:
            return True
        else:
            return False
    # If resource is segmentable but job does not provide requirements, treat it as null-weighted
    elif resource_obj.hardware is not None:
        return True
    # Otherwise, simply compute the number of allocated slots
    else:
        if len(running_jobs) < available_resources[current_resource].slots:
            return True
        else:
            return False


class DataLocalityPolicy(Policy):

    def get_resource(self,
                     job: Job,
                     available_resources: MutableMapping[str, Resource],
                     jobs: MutableMapping[str, JobAllocation],
                     resources: MutableMapping[str, ResourceAllocation]) -> Optional[str]:
        valid_resources = list(available_resources.keys())
        # For each input token sorted by weight
        for token in sorted(job.inputs, key=lambda x: x.weight, reverse=True):
            # Get related resources
            related_resources = set()
            for j in (token.job if isinstance(token.job, MutableSequence) else [token.job]):
                if j in jobs:
                    related_resources.update(r for r in jobs[j].resources)
            token_processor = job.step.input_ports[token.name].token_processor
            related_resources.update(token_processor.get_related_resources(token))
            # Check if one of the related resources is free
            for current_resource in related_resources:
                if current_resource in valid_resources:
                    if _is_valid(
                            current_resource=current_resource,
                            job=job,
                            available_resources=available_resources,
                            jobs=jobs,
                            resources=resources):
                        return current_resource
                    else:
                        valid_resources.remove(current_resource)
        # If a data-related allocation is not possible, assign a resource among the remaining free ones
        for resource in valid_resources:
            if _is_valid(
                    current_resource=resource,
                    job=job,
                    available_resources=available_resources,
                    jobs=jobs,
                    resources=resources):
                return resource
        # If there are no available resources, return None
        return None
