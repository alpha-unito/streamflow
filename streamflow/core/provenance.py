from __future__ import annotations

from abc import abstractmethod
from typing import MutableMapping, MutableSequence, TYPE_CHECKING

from streamflow.core.persistence import DatabaseLoadingContext

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.workflow import Workflow


class ProvenanceManager:
    def __init__(
        self,
        context: StreamFlowContext,
        db_context: DatabaseLoadingContext,
        workflows: MutableSequence[Workflow],
    ) -> None:
        self.context: StreamFlowContext = context
        self.db_context: DatabaseLoadingContext = db_context
        self.workflows: MutableSequence[Workflow] = workflows

    @abstractmethod
    async def create_archive(
        self,
        outdir: str,
        filename: str | None,
        config: str | None,
        additional_files: MutableSequence[MutableMapping[str, str]] | None,
        additional_properties: MutableSequence[MutableMapping[str, str]] | None,
    ):
        ...
