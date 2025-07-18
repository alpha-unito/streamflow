from __future__ import annotations

import math
import os
from collections.abc import MutableMapping, MutableSequence
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from streamflow.core.context import StreamFlowContext
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.scheduling import Hardware, HardwareRequirement, Storage
from streamflow.cwl.utils import eval_expression
from streamflow.workflow.utils import get_token_value

if TYPE_CHECKING:
    from streamflow.core.workflow import Job


class CWLHardwareRequirement(HardwareRequirement):
    def __init__(
        self,
        cwl_version: str,
        cores: str | float | None = None,
        memory: str | float | None = None,
        tmpdir: str | float | None = None,
        outdir: str | float | None = None,
        full_js: bool = False,
        expression_lib: MutableSequence[str] | None = None,
    ):
        self.cores: str | float = cores if cores is not None else 1
        self.memory: str | float = (
            memory if memory is not None else (1024 if cwl_version == "v1.0" else 256)
        )
        self.tmpdir: str | float = tmpdir if tmpdir is not None else 1024
        self.outdir: str | float = outdir if outdir is not None else 1024
        self.full_js: bool = full_js
        self.expression_lib: MutableSequence[str] | None = expression_lib

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        return cls(
            cwl_version="",
            cores=row["cores"],
            memory=row["memory"],
            tmpdir=row["tmpdir"],
            outdir=row["outdir"],
            full_js=row["full_js"],
            expression_lib=row["expression_lib"],
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return {
            "cores": self.cores,
            "memory": self.memory,
            "tmpdir": self.tmpdir,
            "outdir": self.outdir,
            "full_js": self.full_js,
            "expression_lib": self.expression_lib,
        }

    def _process_requirement(
        self, requirement: str | float, context: MutableMapping[str, Any]
    ) -> float:
        return math.ceil(
            eval_expression(
                expression=requirement,
                context=context,
                full_js=self.full_js,
                expression_lib=self.expression_lib,
            )
        )

    def get_cores(self, context: MutableMapping[str, Any]) -> float:
        return self._process_requirement(self.cores, context)

    def get_memory(self, context: MutableMapping[str, Any]) -> float:
        return self._process_requirement(self.memory, context)

    def get_outdir_size(self, context: MutableMapping[str, Any]) -> float:
        return self._process_requirement(self.outdir, context)

    def get_tmpdir_size(self, context: MutableMapping[str, Any]) -> float:
        return self._process_requirement(self.tmpdir, context)

    def eval(self, job: Job) -> Hardware:
        context = {
            "inputs": {name: get_token_value(t) for name, t in job.inputs.items()}
        }
        return Hardware(
            cores=self.get_cores(context),
            memory=self.get_memory(context),
            storage={
                "__outdir__": Storage(
                    os.sep, self.get_outdir_size(context), {job.output_directory}
                ),
                "__tmpdir__": Storage(
                    os.sep, self.get_tmpdir_size(context), {job.tmp_directory}
                ),
            },
        )
