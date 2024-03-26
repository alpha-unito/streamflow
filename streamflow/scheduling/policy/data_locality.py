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
    JobContext,
    sum_job_req,
    diff_hw,
    greater_eq_hw,
    JobHardwareRequirement,
)
from streamflow.core.workflow import Status
from streamflow.workflow.token import FileToken

if TYPE_CHECKING:
    from streamflow.core.scheduling import AvailableLocation, LocationAllocation
    from typing import MutableMapping


class DataLocalityPolicy(Policy):

    def _is_valid(
        self,
        location: AvailableLocation,
        job_context: JobContext,
        used_hardware: JobHardwareRequirement,
        num_scheduled_jobs: int,
    ) -> bool:
        # If location is segmentable and job provides requirements, compute the used amount of locations
        if (
            location.hardware is not None
            and job_context.hardware_requirement is not None
        ):
            available_hardware = diff_hw(location.hardware, used_hardware)
            return greater_eq_hw(available_hardware, used_hardware)
            # If location is segmentable but job does not provide requirements, treat it as null-weighted
        elif location.hardware is not None:
            return True
        # Otherwise, simply compute the number of allocated slots
        else:
            return num_scheduled_jobs < location.slots

    async def get_location(
        self,
        context: StreamFlowContext,
        pending_jobs: MutableSequence[JobContext],
        available_locations: MutableMapping[str, AvailableLocation],
        scheduled_jobs: MutableSequence[JobAllocation],
        locations: MutableMapping[str, MutableMapping[str, LocationAllocation]],
    ) -> MutableMapping[str, AvailableLocation]:
        job_candidates = {}
        running_jobs = list(
            filter(
                lambda x: (x.status == Status.RUNNING or x.status == Status.FIREABLE),
                scheduled_jobs,
            )
        )
        used_hardware = sum_job_req(j.hardware for j in running_jobs if j.hardware)
        num_scheduled_jobs = len(running_jobs)
        for job_context in pending_jobs:
            job = job_context.job
            locations = {}
            for k, loc in available_locations.items():
                if self._is_valid(loc, job_context, used_hardware, num_scheduled_jobs):
                    locations[k] = loc
                    num_scheduled_jobs += 1
                    if job_context.hardware_requirement:
                        used_hardware = sum_job_req(
                            [used_hardware, job_context.hardware_requirement]
                        )
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
