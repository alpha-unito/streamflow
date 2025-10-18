import asyncio
from collections.abc import MutableSequence
from typing import Any

from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataType
from streamflow.cwl import utils
from streamflow.data.remotepath import StreamFlowPath
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
                data_location = next(iter(data_locations))
                path = StreamFlowPath(
                    data_location.path, context=context, location=data_location.location
                )
                weight = await (await path.resolve()).size()
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


class CWLFileToken(FileToken):
    __slots__ = ()

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
        return await _get_file_token_weight(context, self.value)
