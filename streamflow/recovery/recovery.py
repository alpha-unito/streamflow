from typing import MutableMapping, Optional

from streamflow.core.workflow import Job, Step, Token


class JobVersion(object):
    __slots__ = ('job', 'outputs', 'step', 'version')

    def __init__(self,
                 job: Job,
                 outputs: Optional[MutableMapping[str, Token]],
                 step: Step,
                 version: int = 1):
        self.job: Job = job
        self.outputs: Optional[MutableMapping[str, Token]] = outputs
        self.step: Step = step
        self.version: int = version
