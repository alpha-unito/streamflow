import json
from collections.abc import MutableSequence
from pathlib import Path

import tests
from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataLocation, DataManager, DataType
from streamflow.core.deployment import ExecutionLocation


def get_data_path(*args: str) -> Path:
    path = Path(tests.__file__).parent.joinpath("data")
    for arg in args:
        path = path.joinpath(arg)
    return path


class CustomDataManager(DataManager):
    def __init__(self, context: StreamFlowContext, custom_arg: int) -> None:
        super().__init__(context)
        self.custom_arg: int = custom_arg

    async def close(self) -> None:
        raise NotImplementedError

    @classmethod
    def get_schema(cls) -> str:
        return json.dumps(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$id": "https://streamflow.di.unito.it/schemas/tests/utils/data/custom_data_manager.json",
                "type": "object",
                "properties": {
                    "custom_arg": {"type": "integer", "description": "No description"}
                },
                "additionalProperties": False,
            }
        )

    def get_data_locations(
        self,
        path: str,
        deployment: str | None = None,
        location_name: str | None = None,
        data_type: DataType | None = None,
    ) -> MutableSequence[DataLocation]:
        raise NotImplementedError

    async def get_source_location(
        self, path: str, dst_deployment: str
    ) -> DataLocation | None:
        raise NotImplementedError

    def invalidate_location(self, location: ExecutionLocation, path: str) -> None: ...

    def register_path(
        self,
        location: ExecutionLocation,
        path: str,
        relpath: str | None = None,
        data_type: DataType = DataType.PRIMARY,
    ) -> DataLocation:
        raise NotImplementedError

    def register_relation(
        self, src_location: DataLocation, dst_location: DataLocation
    ) -> None:
        raise NotImplementedError

    async def transfer_data(
        self,
        src_location: ExecutionLocation,
        src_path: str,
        dst_locations: MutableSequence[ExecutionLocation],
        dst_path: str,
        writable: bool = False,
    ) -> None:
        raise NotImplementedError
