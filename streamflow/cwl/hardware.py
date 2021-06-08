from typing import MutableSequence, Union, Optional

from streamflow.core.scheduling import Hardware
from streamflow.core.workflow import HardwareRequirement, Token


class CWLHardwareRequirement(HardwareRequirement):

    def __init__(self,
                 cores: Union[str, float] = 1,
                 memory: Union[str, float] = 256,
                 tmpdir: Union[str, float] = 1,
                 outdir: Union[str, float] = 1,
                 full_js: bool = False,
                 expression_lib: Optional[MutableSequence[str]] = None,):
        self.cores: Union[str, float] = cores
        self.memory: Union[str, float] = memory
        self.tmpdir: Union[str, float] = tmpdir
        self.outdir: Union[str, float] = outdir

    def eval(self, inputs: MutableSequence[Token]) -> Hardware:
        pass