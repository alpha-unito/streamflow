import asyncio
from typing import MutableSequence, Any

from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataType
from streamflow.core.workflow import Token
from streamflow.cwl import utils
from streamflow.data import remotepath
from streamflow.workflow.token import FileToken


async def _get_file_token_weight(context: StreamFlowContext,
                                 value: Any):
    weight = 0
    if 'size' in value:
        weight = value['size']
    else:
        if path := utils.get_path_from_token(value):
            data_locations = context.data_manager.get_data_locations(
                path=path,
                location_type=DataType.PRIMARY)
            if data_locations:
                connector = context.deployment_manager.get_connector(list(data_locations)[0].deployment)
                location = list(data_locations)[0].location
                real_path = await remotepath.follow_symlink(connector, location, path)
                weight = await remotepath.size(connector, location, real_path)
    if 'secondaryFiles' in value:
        weight += sum(await asyncio.gather(*(asyncio.create_task(
            _get_file_token_weight(
                context=context,
                value=sf
            )) for sf in value['secondaryFiles'])))
    return weight


class CWLFileToken(FileToken):

    async def get_paths(self, context: StreamFlowContext) -> MutableSequence[str]:
        paths = []
        if isinstance(self.value, MutableSequence):
            for value in self.value:
                if path := utils.get_path_from_token(value):
                    paths.append(path)
                for sf in value.get('secondaryFiles', []):
                    if path := utils.get_path_from_token(sf):
                        paths.append(path)
        elif self.value and (path := utils.get_path_from_token(self.value)):
            paths.append(path)
            for sf in self.value.get('secondaryFiles', []):
                if path := utils.get_path_from_token(sf):
                    paths.append(path)
        return paths

    async def get_weight(self, context):
        if isinstance(self.value, Token):
            return await self.value.get_weight(context)
        else:
            return await _get_file_token_weight(context, self.value)
