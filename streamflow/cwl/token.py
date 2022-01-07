import asyncio
import sys
from typing import MutableSequence, Any, MutableMapping

from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataType
from streamflow.core.workflow import Token
from streamflow.cwl import utils
from streamflow.data import remotepath
from streamflow.workflow.token import FileToken


async def _get_file_token_weight(context: StreamFlowContext,
                                 value: Any):
    if 'size' in value:
        return value['size']
    else:
        path = utils.get_path_from_token(value)
        if path:
            data_locations = context.data_manager.get_data_locations(
                path=utils.get_path_from_token(value),
                location_type=DataType.PRIMARY)
            if data_locations:
                connector = context.deployment_manager.get_connector(list(data_locations)[0].deployment)
                location = list(data_locations)[0].location
                path = utils.get_path_from_token(value)
                real_path = await remotepath.follow_symlink(connector, location, path)
                return value.get('size', await remotepath.size(connector, location, real_path))
    return 0


class CWLFileToken(FileToken):

    async def get_paths(self, context: StreamFlowContext) -> MutableSequence[str]:
        paths = []
        if isinstance(self.value, MutableSequence):
            for value in self.value:
                if path := utils.get_path_from_token(value):
                    paths.append(path)
        elif self.value and (path := utils.get_path_from_token(self.value)):
            paths.append(path)
        return paths

    async def get_weight(self, context):
        if isinstance(self.value, Token):
            return await self.value.get_weight(context)
        else:
            return await _get_file_token_weight(context, self.value)
