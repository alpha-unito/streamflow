from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, MutableSequence

from importlib_resources import files

from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataType
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.scheduling import (
    JobAllocation,
    Policy,
    Hardware,
)
from streamflow.core.workflow import Status, Job
from streamflow.workflow.token import FileToken

if TYPE_CHECKING:
    from streamflow.core.scheduling import AvailableLocation, LocationAllocation
    from typing import MutableMapping


class DataLocalityPolicy(Policy):

    def _is_valid(
        self,
        location: AvailableLocation,
        hardware_requirement: Hardware | None,
        scheduled_jobs,
    ) -> bool:
        if location.name in scheduled_jobs.get(location.deployment, {}):
            running_jobs = list(
                filter(
                    lambda x: (
                        scheduled_jobs.status == Status.RUNNING
                        or scheduled_jobs.status == Status.FIREABLE
                    ),
                    scheduled_jobs[location.deployment][location.name].jobs,
                )
            )
        else:
            running_jobs = []
        # If location is segmentable and job provides requirements, compute the used amount of locations
        if location.hardware is not None and hardware_requirement is not None:
            used_hardware = sum(
                (scheduled_jobs.hardware for j in running_jobs),
                start=hardware_requirement.__class__(0, 0, {}),
            )
            available_hardware = location.hardware - used_hardware
            return available_hardware >= hardware_requirement
        # If location is segmentable but job does not provide requirements, treat it as null-weighted
        elif location.hardware is not None:
            return True
        # Otherwise, simply compute the number of allocated slots
        else:
            return len(running_jobs) < location.slots

    async def get_location(
        self,
        context: StreamFlowContext,
        pending_jobs: MutableSequence[Job],
        hardware_requirements: MutableMapping[str, Hardware],
        available_locations: MutableMapping[str, AvailableLocation],
        scheduled_jobs: MutableMapping[str, JobAllocation],
        locations: MutableMapping[str, MutableMapping[str, LocationAllocation]],
    ) -> MutableMapping[str, AvailableLocation]:
        job_candidates = {}
        for job in pending_jobs:
            if job_candidates:
                # todo: tmp solution.
                #  It return just one job to schedule. It is necessary to consider
                #  the hardware_req to check which other jobs are possible to schedule
                break
            locations = {}
            for k, loc in available_locations.items():
                if self._is_valid(loc, hardware_requirements[job.name], scheduled_jobs):
                    locations[k] = loc
            valid_locations = list(locations.keys())
            deployments = {loc.deployment for loc in locations.values()}
            if len(deployments) > 1:
                raise WorkflowExecutionException(
                    f"Available locations coming from multiple deployments: {deployments}"
                )
            # For each input token sorted by weight
            weights = {
                k: v
                for k, v in zip(
                    job.inputs,
                    await asyncio.gather(
                        *(
                            asyncio.create_task(t.get_weight(context))
                            for t in job.inputs.values()
                        )
                    ),
                )
            }
            for _, token in sorted(
                job.inputs.items(), key=lambda item: weights[item[0]], reverse=True
            ):
                related_locations = set()
                # For FileTokens, retrieve related locations
                if isinstance(token, FileToken):
                    for path in await token.get_paths(context):
                        related_locations.update(
                            [
                                loc.name
                                for loc in context.data_manager.get_data_locations(
                                    path=path,
                                    deployment=next(iter(deployments)),
                                    data_type=DataType.PRIMARY,
                                )
                            ]
                        )
                # Check if one of the related locations is free
                for current_location in related_locations:
                    if current_location in valid_locations:
                        job_candidates[job.name] = available_locations[current_location]
                        break
                if job.name in job_candidates:
                    break
            # If a data-related allocation is not possible, assign a location among the remaining free ones
            for location in valid_locations:
                job_candidates[job.name] = available_locations[location]
                break
            # If there are no available locations, return None
        return job_candidates

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("data_locality.json")
            .read_text("utf-8")
        )
