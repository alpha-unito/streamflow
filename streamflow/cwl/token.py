import asyncio
from typing import Any, MutableSequence

from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataType
from streamflow.core.workflow import Token
from streamflow.cwl import utils
from streamflow.data import remotepath
from streamflow.log_handler import logger
from streamflow.workflow.token import FileToken

import time


async def _get_file_token_weight(context: StreamFlowContext, value: Any):
    weight = 0
    if "size" in value:
        weight = value["size"]
    else:
        if path := utils.get_path_from_token(value):
            data_locations = context.data_manager.get_data_locations(
                path=path, location_type=DataType.PRIMARY
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


async def data_location_exists(data_location, context, path):
    connector = context.deployment_manager.get_connector(data_location.deployment)
    # location_allocation = context.scheduler.location_allocations[data_loc.deployment][data_loc.name]
    # available_locations = context.scheduler.get_locations(location_allocation.jobs[0])
    return await remotepath.exists(connector, data_location, path)


def _get_data_location(path, context):
    data_locs = context.data_manager.get_data_locations(path)
    # print(
    #     f"Path {path} in data_locs {[ str(dl.name) + ' ' + str(dl.deployment) + ' ' + str(dl.service) + ' ' + str(dl.path) for dl in data_locs]}"
    # )
    for data_loc in data_locs:
        if data_loc.path == path:
            # print(f"Path {path} return data_loc {data_loc.deployment}")
            return data_loc
    return None


async def _is_file_token_available(context: StreamFlowContext, value: Any) -> bool:
    if path := utils.get_path_from_token(value):
        if not (data_locs := context.data_manager.get_data_locations(path)):
            return False
        # exception only for debug
        if [dl for dl in data_locs if dl.data_type == DataType.INVALID]:
            raise Exception(
                f"file {path} è già contrassegnato come non disponibile ma è stato recuperato il data location",
            )
        for data_loc in data_locs:
            if await data_location_exists(data_loc, context, path):
                return True
            logger.debug(
                f"Invalidated location {data_loc.deployment} (Losted path {path})"
            )
            start = time.time()
            context.data_manager.invalidate_location(data_loc, "/")
            print(f"Tempo per invalidare location {path}: {time.time() - start}")
        return False
    raise Exception(
        f"Non è stato possibile verificare se il file {value} è disponibile"
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

    def __str__(self):
        return self.value["path"]
