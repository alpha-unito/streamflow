from __future__ import annotations

import asyncio
from collections.abc import MutableMapping, MutableSet
from typing import TYPE_CHECKING, Any, AsyncContextManager, cast

from rdflib import Graph
from typing_extensions import Self

from streamflow.core.data import DataLocation
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.utils import random_name
from streamflow.core.workflow import Workflow
from streamflow.data.remotepath import StreamFlowPath

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext


class CWLOutputPathContextManager(AsyncContextManager[StreamFlowPath]):

    def __init__(self, path: StreamFlowPath, event: asyncio.Event | None = None):
        self.event: asyncio.Event | None = event
        self.path: StreamFlowPath = path

    async def __aenter__(self):
        return self.path

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Notify that the operation has been completed
        if self.event is not None:
            self.event.set()


class CWLOutputPathContextFactory:
    def __init__(self, path: StreamFlowPath):
        self.path: StreamFlowPath = path
        self._event: asyncio.Event = asyncio.Event()
        self._output_keys: MutableSet[tuple[str, str, str]] = set()

    async def get(
        self, deployment: str, location: str, path: str
    ) -> CWLOutputPathContextManager:
        key = (deployment, location, path)
        # If the exact same file has already been transferred
        if key in self._output_keys:
            # Wait for the original file to be processed and throw exception
            await self._event.wait()
            raise FileExistsError(f"File exists: {self.path}")
        # Otherwise
        else:
            # Register the new key
            self._output_keys.add(key)
            # If multiple replicas are present, generate a unique file name by appending a counter to the file
            if (idx := len(self._output_keys) - 1) > 0:
                return CWLOutputPathContextManager(
                    path=(
                        self.path.parent
                        / f"{self.path.stem}-{idx}{f'{self.path.suffix}' if self.path.suffix else ''}"
                    )
                )
            # Otherwise, simply use the original path and notify when it has been processed
            else:
                return CWLOutputPathContextManager(path=self.path, event=self._event)


class CWLWorkflow(Workflow):
    def __init__(
        self,
        context: StreamFlowContext,
        cwl_version: str,
        config: MutableMapping[str, Any],
        name: str | None = None,
        format_graph: Graph | None = None,
    ):
        super().__init__(context, config, name)
        self.cwl_version: str = cwl_version
        self.format_graph: Graph | None = format_graph
        self.type: str = "cwl"
        self._output_data: MutableMapping[str, CWLOutputPathContextFactory] = {}
        self._output_lock: asyncio.Lock = asyncio.Lock()

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "cwl_version": self.cwl_version,
            "format_graph": (
                self.format_graph.serialize() if self.format_graph is not None else None
            ),
        }

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        params = row["params"]
        return cls(
            context=context,
            config=params["config"],
            cwl_version=params["cwl_version"],
            name=row["name"],
            format_graph=(
                Graph().parse(data=params["format_graph"])
                if params["format_graph"] is not None
                else None
            ),
        )

    async def get_output_path(
        self,
        unique: bool,
        path: StreamFlowPath,
        src_location: DataLocation | None = None,
    ) -> CWLOutputPathContextManager:
        # If the path should not be unique, just generate a new context
        if not unique:
            return CWLOutputPathContextManager(path=path)
        # Verify if the output path has already been registered
        async with self._output_lock:
            if str(path) not in self._output_data:
                self._output_data[str(path)] = CWLOutputPathContextFactory(path=path)
        # If a source location exists, use the deployment name, location name, and path as the key
        if src_location is not None:
            return await self._output_data[str(path)].get(
                src_location.deployment, src_location.name, src_location.path
            )
        # Otherwise, since literal files should always be created, generate random values to prevent collisions
        else:
            return await self._output_data[str(path)].get(
                random_name(), random_name(), random_name()
            )
