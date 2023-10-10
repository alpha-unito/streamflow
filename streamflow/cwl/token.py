import asyncio
import logging
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


async def _is_file_token_available(context: StreamFlowContext, value: Any) -> bool:
    if path := utils.get_path_from_token(value):
        # todo: controllare anche i secondaryfiles
        if not (data_locs := context.data_manager.get_data_locations(path)):
            return False
        # exception only for debug
        if [dl for dl in data_locs if dl.data_type == DataType.INVALID]:
            raise Exception(
                f"file {path} è già contrassegnato come non disponibile ma è stato recuperato il data location",
            )
        is_available = False
        for data_loc in data_locs:
            connector = context.deployment_manager.get_connector(data_loc.deployment)
            if await remotepath.exists(connector, data_loc, data_loc.path):
                # todo:
                #  - opz1: controllare le location finché non si trova un file available
                #    Problema [loop deadlock] -> possibile esecuzione:
                #      - file era su loc1 e loc2, ma su loc2 si è perso.
                #      - Il file viene segnato disponibile perché viene trovato su loc1 (ma non viene controllato loc2).
                #      - Se il job viene assegnato a loc2 viene creato un link-simbolico con il file perso.
                #  - opz2: controllare tutte le location, per aggiornare il data manager
                #    Attuale implementazione
                #  - opz3: controllare la location fallita, se file non available, controllare le altre location finché non c'è un file available

                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Location {data_loc.deployment} has valid file {data_loc.path}"
                    )
                is_available = True
            else:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Invalidated location {data_loc.deployment} (Losted path {data_loc.path})"
                    )
                context.data_manager.invalidate_location(data_loc, "/")
        return is_available
    raise Exception(f"It is not possible to verify the file {value} availability")


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
