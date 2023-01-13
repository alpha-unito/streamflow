from __future__ import annotations

import math
from typing import Any, MutableMapping, MutableSequence

from streamflow.core.context import StreamFlowContext
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.scheduling import Hardware, HardwareRequirement
from streamflow.core.workflow import Token
from streamflow.cwl.utils import eval_expression
from streamflow.workflow.utils import get_token_value


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
    ) -> CWLHardwareRequirement:
        return cls(
            cwl_version="",
            cores=row["cores"],
            memory=row["memory"],
            tmpdir=row["tmpdir"],
            outdir=row["outdir"],
            full_js=row["full_js"],
            expression_lib=row["expression_lib"],
        )

    async def _save_additional_params(self, context: StreamFlowContext):
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

    def eval(self, inputs: MutableMapping[str, Token]) -> Hardware:
        context = {"inputs": {name: get_token_value(t) for name, t in inputs.items()}}
        return Hardware(
            cores=self._process_requirement(self.cores, context),
            memory=self._process_requirement(self.memory, context),
            tmp_directory=self._process_requirement(self.tmpdir, context),
            output_directory=self._process_requirement(self.outdir, context),
        )
