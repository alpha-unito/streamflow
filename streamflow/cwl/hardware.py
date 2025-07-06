from __future__ import annotations

import math
import os
from collections.abc import MutableMapping, MutableSequence
from typing import TYPE_CHECKING, Any

from streamflow.core.context import StreamFlowContext
from streamflow.core.hardware import Hardware, HardwareRequirement, Storage
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.cwl.utils import eval_expression
from streamflow.hardware.device import NVIDIAGPUDevice
from streamflow.workflow.utils import get_token_value

if TYPE_CHECKING:
    from streamflow.core.workflow import Job


class CWLHardwareRequirement(HardwareRequirement):
    def __init__(
        self,
        cwl_version: str,
        compute_capabilities: float | MutableSequence[float] | None = None,
        cores: str | float | None = None,
        cuda_version: float | None = None,
        gpus: str | int | None = None,
        memory: str | float | None = None,
        tmpdir: str | float | None = None,
        outdir: str | float | None = None,
        full_js: bool = False,
        expression_lib: MutableSequence[str] | None = None,
    ):
        self.compute_capabilities: MutableSequence[float] | None = (
            compute_capabilities
            if isinstance(compute_capabilities, MutableSequence)
            else [compute_capabilities]
        )
        self.cores: str | float = cores if cores is not None else 1.0
        self.cuda_version: float | None = cuda_version
        self.gpus: str | int | None = gpus if gpus is not None else 0
        self.memory: str | float = (
            memory
            if memory is not None
            else (1024.0 if cwl_version == "v1.0" else 256.0)
        )
        self.tmpdir: str | float = tmpdir if tmpdir is not None else 1024.0
        self.outdir: str | float = outdir if outdir is not None else 1024.0
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
            compute_capabilities=row["compute_capabilities"],
            cores=row["cores"],
            cuda_version=row["cuda_version"],
            gpus=row["gpus"],
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
            "compute_capabilities": self.compute_capabilities,
            "cores": self.cores,
            "cuda_version": self.cuda_version,
            "gpus": self.gpus,
            "memory": self.memory,
            "tmpdir": self.tmpdir,
            "outdir": self.outdir,
            "full_js": self.full_js,
            "expression_lib": self.expression_lib,
        }

    def _process_requirement(
        self, requirement: str | float, context: MutableMapping[str, Any]
    ) -> int:
        return math.ceil(
            eval_expression(
                expression=requirement,
                context=context,
                full_js=self.full_js,
                expression_lib=self.expression_lib,
            )
        )

    def _get_cores(self, context) -> float:
        return self._process_requirement(self.cores, context)

    def _get_devices(self, context):
        return [
            NVIDIAGPUDevice(
                compute_capability=min(self.compute_capabilities),
                cuda_version=self.cuda_version,
            )
            for _ in range(self._process_requirement(self.gpus, context))
        ]

    def _get_memory(self, context) -> float:
        return self._process_requirement(self.memory, context)

    def _get_outdir_size(self, context) -> float:
        return self._process_requirement(self.outdir, context)

    def _get_tmpdir_size(self, context) -> float:
        return self._process_requirement(self.tmpdir, context)

    def eval(self, job: Job) -> Hardware:
        context = {
            "inputs": {name: get_token_value(t) for name, t in job.inputs.items()}
        }
        return Hardware(
            cores=self._get_cores(context),
            memory=self._get_memory(context),
            storage={
                "__outdir__": Storage(
                    os.sep, self._get_outdir_size(context), {job.output_directory}
                ),
                "__tmpdir__": Storage(
                    os.sep, self._get_tmpdir_size(context), {job.tmp_directory}
                ),
            },
            devices=self._get_devices(context),
        )
