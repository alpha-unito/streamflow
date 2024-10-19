from __future__ import annotations

from typing import MutableMapping

from streamflow.core.workflow import Job, Step, Token


class JobVersion:
    __slots__ = ("job", "outputs", "step", "version")

    def __init__(
        self,
        job: Job,
        outputs: MutableMapping[str, Token] | None,
        step: Step,
        version: int = 1,
    ):
        self.job: Job = job
        self.outputs: MutableMapping[str, Token] | None = outputs
        self.step: Step = step
        self.version: int = version
