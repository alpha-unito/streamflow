from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING

import pkg_resources

from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataType
from streamflow.core.scheduling import Hardware, JobAllocation, Policy
from streamflow.core.workflow import Status
from streamflow.workflow.token import FileToken

if TYPE_CHECKING:
    from streamflow.core.scheduling import Location, LocationAllocation
    from streamflow.core.workflow import Job
    from typing import MutableMapping, Optional


def _is_valid(current_location: str,
              hardware_requirement: Hardware,
              available_locations: MutableMapping[str, Location],
              jobs: MutableMapping[str, JobAllocation],
              locations: MutableMapping[str, LocationAllocation]) -> bool:
    location_obj = available_locations[current_location]
    running_jobs = list(
        filter(lambda x: jobs[x].status == Status.RUNNING or jobs[x].status == Status.FIREABLE,
               locations[current_location].jobs)) if current_location in locations else []
    # If location is segmentable and job provides requirements, compute the used amount of locations
    if location_obj.hardware is not None and hardware_requirement is not None:
        used_hardware = sum((jobs[j].hardware for j in running_jobs), start=hardware_requirement.__class__())
        if (location_obj.hardware - used_hardware) >= hardware_requirement:
            return True
        else:
            return False
    # If location is segmentable but job does not provide requirements, treat it as null-weighted
    elif location_obj.hardware is not None:
        return True
    # Otherwise, simply compute the number of allocated slots
    else:
        return len(running_jobs) < available_locations[current_location].slots


class DataLocalityPolicy(Policy):

    async def get_location(self,
                           context: StreamFlowContext,
                           job: Job,
                           deployment: str,
                           hardware_requirement: Hardware,
                           available_locations: MutableMapping[str, Location],
                           jobs: MutableMapping[str, JobAllocation],
                           locations: MutableMapping[str, LocationAllocation]) -> Optional[str]:
        valid_locations = list(available_locations.keys())
        # For each input token sorted by weight
        weights = {k: v for k, v in zip(job.inputs, await asyncio.gather(*(
            asyncio.create_task(t.get_weight(context)) for t in job.inputs.values())))}
        for name, token in sorted(job.inputs.items(), key=lambda item: weights[item[0]], reverse=True):
            related_locations = set()
            # For FileTokens, retrieve related locations
            if isinstance(token, FileToken):
                for path in await token.get_paths(context):
                    related_locations.update([loc.location for loc in context.data_manager.get_data_locations(
                        path=path,
                        deployment=deployment,
                        location_type=DataType.PRIMARY)])
            # Check if one of the related locations is free
            for current_location in related_locations:
                if current_location in valid_locations:
                    if _is_valid(
                            current_location=current_location,
                            hardware_requirement=hardware_requirement,
                            available_locations=available_locations,
                            jobs=jobs,
                            locations=locations):
                        return current_location
                    else:
                        valid_locations.remove(current_location)
        # If a data-related allocation is not possible, assign a location among the remaining free ones
        for location in valid_locations:
            if _is_valid(
                    current_location=location,
                    hardware_requirement=hardware_requirement,
                    available_locations=available_locations,
                    jobs=jobs,
                    locations=locations):
                return location
        # If there are no available locations, return None
        return None

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join('schemas', 'data_locality.json'))
