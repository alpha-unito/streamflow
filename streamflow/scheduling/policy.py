from __future__ import annotations

from typing import TYPE_CHECKING, MutableSequence

from streamflow.core.scheduling import Policy
from streamflow.core.workflow import Status

if TYPE_CHECKING:
    from streamflow.core.scheduling import JobAllocation, Resource, ResourceAllocation
    from streamflow.core.workflow import Job
    from typing import MutableMapping, Optional


class DataLocalityPolicy(Policy):

    def get_resource(self,
                     job: Job,
                     available_resources: MutableMapping[str, Resource],
                     jobs: MutableMapping[str, JobAllocation],
                     resources: MutableMapping[str, ResourceAllocation]) -> Optional[str]:
        valid_resources = list(available_resources.keys())
        inputs = []
        for token in job.inputs:
            # If the token is actually an aggregate of multiple tokens, consider each token separately
            if isinstance(token.job, MutableSequence):
                inputs.extend(token.value)
            else:
                inputs.append(token)
        # Sort inputs by weight
        inputs = sorted(inputs, key=lambda x: x.weight, reverse=True)
        # For each input token
        for token in inputs:
            # Skip the input if the related job was executed locally
            if token.job not in jobs:
                continue
            # Get related resources
            related_resources = set(jobs[token.job].resources)
            if token.name in job.step.input_ports:
                token_processor = job.step.input_ports[token.name].token_processor
            else:
                token_processor = jobs[token.job].job.step.output_ports[token.name].token_processor
            related_resources.update(token_processor.get_related_resources(token))
            # Check if one of the related resources is free
            for current_resource in related_resources:
                if current_resource in valid_resources:
                    resource_obj = available_resources[current_resource]
                    running_jobs = list(
                        filter(lambda x: jobs[x].status == Status.RUNNING, resources[current_resource].jobs))
                    # If resource is segmentable and job provides requirements, compute the used amount of resources
                    if resource_obj.hardware is not None and job.hardware is not None:
                        used_hardware = sum(jobs[j].hardware for j in running_jobs)
                        if (resource_obj.hardware - used_hardware) >= job.hardware:
                            return current_resource
                        else:
                            valid_resources.remove(current_resource)
                    # Otherwise, simply compute the number of allocated slots
                    else:
                        if len(running_jobs) < available_resources[current_resource].slots:
                            return current_resource
                        else:
                            valid_resources.remove(current_resource)
        # If a data-related allocation is not possible, assign a resource among the remaining free ones
        for resource in valid_resources:
            if resource not in resources:
                return resource
            running_jobs = list(
                filter(lambda x: jobs[x].status == Status.RUNNING, resources[resource].jobs))
            if len(running_jobs) == 0:
                return resource
        # If there are no available resources, return None
        return None
