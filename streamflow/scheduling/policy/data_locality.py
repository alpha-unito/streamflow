from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING

import pkg_resources

from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataType
from streamflow.core.deployment import Location
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.scheduling import Hardware, JobAllocation, Policy
from streamflow.workflow.token import FileToken

if TYPE_CHECKING:
    from streamflow.core.scheduling import AvailableLocation, LocationAllocation
    from streamflow.core.workflow import Job
    from typing import MutableMapping


class DataLocalityPolicy(Policy):
    async def get_location(
        self,
        context: StreamFlowContext,
        job: Job,
        hardware_requirement: Hardware,
        available_locations: MutableMapping[str, AvailableLocation],
        jobs: MutableMapping[str, JobAllocation],
        locations: MutableMapping[str, MutableMapping[str, LocationAllocation]],
    ) -> Location | None:
        valid_locations = list(available_locations.keys())
        deployments = {loc.deployment for loc in available_locations.values()}
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
                    return available_locations[current_location]
        # If a data-related allocation is not possible, assign a location among the remaining free ones
        for location in valid_locations:
            return available_locations[location]
        # If there are no available locations, return None
        return None

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "data_locality.json")
        )
