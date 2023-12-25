import asyncio
import logging
from typing import Any, MutableSequence

from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataType
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.workflow import Token
from streamflow.cwl import utils
from streamflow.data import remotepath
from streamflow.log_handler import logger
from streamflow.workflow.token import FileToken


async def _get_file_token_weight(context: StreamFlowContext, value: Any):
    weight = 0
    if "size" in value:
        weight = value["size"]
    else:
        if path := utils.get_path_from_token(value):
            data_locations = context.data_manager.get_data_locations(
                path=path, data_type=DataType.PRIMARY
            )
            if data_locations:
                location = list(data_locations)[0]
                connector = context.deployment_manager.get_connector(
                    location.deployment
                )
                real_path = await remotepath.follow_symlink(
                    context, connector, location, location.path
                )
                weight = await remotepath.size(connector, location, real_path)
    if "secondaryFiles" in value:
        weight += sum(
            await asyncio.gather(
                *(
                    asyncio.create_task(
                        _get_file_token_weight(context=context, value=sf)
                    )
                    for sf in value["secondaryFiles"]
                )
            )
        )
    return weight


async def _is_file_token_available(context: StreamFlowContext, value: Any) -> bool:
    if path := utils.get_path_from_token(value):
        if not (data_locs := context.data_manager.get_data_locations(path)):
            return False
        is_available = False
        for data_loc in data_locs:
            connector = context.deployment_manager.get_connector(data_loc.deployment)
            if await remotepath.exists(connector, data_loc, data_loc.path):
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Location {data_loc.deployment} has valid data {data_loc.path}"
                    )
                # It does not immediately return True, because it is necessary to check
                # all the locations and invalidate when data is no longer available.
                is_available = True
            else:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Invalidated location {data_loc.deployment} (Lost data {data_loc.path})"
                    )
                root_data_loc = context.data_manager.get_data_locations(
                    "/", data_loc.deployment
                )[0]
                context.data_manager.invalidate_location(
                    root_data_loc, root_data_loc.path
                )
        return is_available
    raise WorkflowExecutionException(
        f"It is not possible to verify the data {value} availability"
    )


class CWLFileToken(FileToken):
    async def get_paths(self, context: StreamFlowContext) -> MutableSequence[str]:
        paths = []
        if isinstance(self.value, MutableSequence):
            for value in self.value:
                if path := utils.get_path_from_token(value):
                    paths.append(path)
                for sf in value.get("secondaryFiles", []):
                    if path := utils.get_path_from_token(sf):
                        paths.append(path)
        elif self.value and (path := utils.get_path_from_token(self.value)):
            paths.append(path)
            for sf in self.value.get("secondaryFiles", []):
                if path := utils.get_path_from_token(sf):
                    paths.append(path)
        return paths

    async def get_weight(self, context):
        if isinstance(self.value, Token):
            return await self.value.get_weight(context)
        else:
            return await _get_file_token_weight(context, self.value)

    async def is_available(self, context: StreamFlowContext):
        if isinstance(self.value, Token):
            return await self.value.is_available(context)
        else:
            return await _is_file_token_available(context, self.value)
