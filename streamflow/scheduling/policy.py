from __future__ import annotations

from typing import TYPE_CHECKING

from streamflow.core.scheduling import Policy, JobStatus

if TYPE_CHECKING:
    from streamflow.core.scheduling import JobAllocation, Resource, ResourceAllocation
    from streamflow.core.workflow import Job
    from typing import MutableMapping, Optional
    from typing_extensions import Text


class DataLocalityPolicy(Policy):

    def get_resource(self,
                     job: Job,
                     available_resources: MutableMapping[Text, Resource],
                     jobs: MutableMapping[Text, JobAllocation],
                     resources: MutableMapping[Text, ResourceAllocation]) -> Optional[Text]:
        valid_resources = list(available_resources.keys())
        inputs = []
        for token in job.inputs:
            # If the token is actually an aggregate of multiple tokens, consider each token separately
            if isinstance(token.job, list):
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
            # If related job was executed on a remote resource, check if it's free
            for current_resource in jobs[token.job].resources:
                if current_resource in valid_resources:
                    running_jobs = list(
                        filter(lambda x: jobs[x].status == JobStatus.RUNNING, resources[current_resource].jobs))
                    # If the resource is free use it for the current job, otherwise discard it
                    if len(running_jobs) == 0:
                        return current_resource
                    else:
                        valid_resources.remove(current_resource)
        # If a data-related allocation is not possible, assign a resource among the remaining free ones
        for resource in valid_resources:
            if resource not in resources:
                return resource
            running_jobs = list(
                filter(lambda x: jobs[x].status == JobStatus.RUNNING, resources[resource].jobs))
            if len(running_jobs) == 0:
                return resource
        # If there are no available resources, return None
        return None
