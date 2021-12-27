from __future__ import annotations

from typing import TYPE_CHECKING, MutableSequence

from streamflow.core.scheduling import Policy, Hardware
from streamflow.core.workflow import Status

if TYPE_CHECKING:
    from streamflow.core.scheduling import JobAllocation, Location, LocationAllocation
    from streamflow.core.workflow import Job
    from typing import MutableMapping, Optional


def _is_valid(current_location: str,
              job: Job,
              available_locations: MutableMapping[str, Location],
              jobs: MutableMapping[str, JobAllocation],
              locations: MutableMapping[str, LocationAllocation]) -> bool:
    location_obj = available_locations[current_location]
    running_jobs = list(
        filter(lambda x: jobs[x].status == Status.RUNNING,
               locations[current_location].jobs)) if current_location in locations else []
    # If location is segmentable and job provides requirements, compute the used amount of locations
    if location_obj.hardware is not None and job.hardware is not None:
        used_hardware = sum((jobs[j].hardware for j in running_jobs), start=Hardware())
        if (location_obj.hardware - used_hardware) >= job.hardware:
            return True
        else:
            return False
    # If location is segmentable but job does not provide requirements, treat it as null-weighted
    elif location_obj.hardware is not None:
        return True
    # Otherwise, simply compute the number of allocated slots
    else:
        if len(running_jobs) < available_locations[current_location].slots:
            return True
        else:
            return False


class DataLocalityPolicy(Policy):

    def get_location(self,
                     job: Job,
                     available_locations: MutableMapping[str, Location],
                     jobs: MutableMapping[str, JobAllocation],
                     locations: MutableMapping[str, LocationAllocation]) -> Optional[str]:
        valid_locations = list(available_locations.keys())
        # For each input token sorted by weight
        for token in sorted(job.inputs, key=lambda x: x.weight, reverse=True):
            # Get related locations
            related_locations = set()
            for j in (token.job if isinstance(token.job, MutableSequence) else [token.job]):
                if j in jobs:
                    related_locations.update(r for r in jobs[j].locations)
            token_processor = job.step.input_ports[token.name].token_processor
            related_locations.update(token_processor.get_related_locations(token))
            # Check if one of the related locations is free
            for current_location in related_locations:
                if current_location in valid_locations:
                    if _is_valid(
                            current_location=current_location,
                            job=job,
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
                    job=job,
                    available_locations=available_locations,
                    jobs=jobs,
                    locations=locations):
                return location
        # If there are no available locations, return None
        return None
