from __future__ import annotations

import json
from collections.abc import MutableMapping, MutableSet
from typing import TYPE_CHECKING, Any, cast

from rdflib import Graph

from streamflow.core.data import DataLocation
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.utils import random_name
from streamflow.core.workflow import Workflow
from streamflow.data.remotepath import StreamFlowPath

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext


class CWLWorkflow(Workflow):
    def __init__(
        self,
        context: StreamFlowContext,
        cwl_version: str,
        config: MutableMapping[str, Any],
        name: str = None,
        format_graph: Graph | None = None,
    ):
        super().__init__(context, config, name)
        self.cwl_version: str = cwl_version
        self.format_graph: Graph | None = format_graph
        self.type: str = "cwl"
        self._output_data: MutableMapping[str, MutableSet[tuple[str, str, str]]] = {}

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
    ) -> CWLWorkflow:
        params = json.loads(row["params"])
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

    def get_unique_output_path(
        self, path: StreamFlowPath, src_location: DataLocation | None = None
    ) -> StreamFlowPath:
        # If a source location exists, use the deployment name, location name, and path as the key
        # Otherwise, since literal files should always be created, generate random values to prevent collisions
        key = (
            (src_location.deployment, src_location.name, src_location.path)
            if src_location is not None
            else (random_name(), random_name(), random_name())
        )
        # Verify if the output path has already been registered
        if str(path) in self._output_data:
            # If the exact same file has already been transferred, throw exception
            if key in self._output_data[str(path)]:
                raise FileExistsError(f"File exists: {path}")
            # Otherwise
            else:
                # Register the new key
                self._output_data[str(path)].add(key)
                # Generate a unique file name by appending a counter to the file
                idx = len(self._output_data[str(path)]) - 1
                return (
                    path.parent
                    / f"{path.stem}-{idx}{f'{path.suffix}' if path.suffix else ''}"
                )
        else:
            # If the output has never been transferred before, simply register it
            self._output_data[str(path)] = {key}
        return path
