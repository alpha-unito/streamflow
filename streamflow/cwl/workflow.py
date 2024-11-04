from __future__ import annotations

import json
from collections.abc import MutableMapping
from typing import TYPE_CHECKING

from rdflib import Graph

from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.workflow import Workflow

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from typing import Any


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
        self.type: str | None = "cwl"

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return await super()._save_additional_params(context) | {
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
