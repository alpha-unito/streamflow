import math
from typing import MutableSequence, Union, Optional, MutableMapping, Any

from streamflow.core.scheduling import Hardware
from streamflow.core.utils import get_token_value
from streamflow.core.workflow import HardwareRequirement, Token
from streamflow.cwl.utils import eval_expression


class CWLHardware(Hardware):
    __slots__ = ('cores', 'memory', 'disk', 'tmpdir', 'outdir')

    def __init__(self,
                 cores: float = 0.0,
                 memory: float = 0.0,
                 tmpdir: float = 0.0,
                 outdir: float = 0.0):
        super().__init__(cores, memory, math.ceil((tmpdir + outdir) / 1024))
        self.tmpdir: float = tmpdir
        self.outdir: float = outdir


class CWLHardwareRequirement(HardwareRequirement):

    def __init__(self,
                 cores: Union[str, float] = 1,
                 memory: Union[str, float] = 256,
                 tmpdir: Union[str, float] = 1024,
                 outdir: Union[str, float] = 1024,
                 full_js: bool = False,
                 expression_lib: Optional[MutableSequence[str]] = None, ):
        self.cores: Union[str, float] = cores
        self.memory: Union[str, float] = memory
        self.tmpdir: Union[str, float] = tmpdir
        self.outdir: Union[str, float] = outdir
        self.full_js: bool = full_js
        self.expression_lib: Optional[MutableSequence[str]] = expression_lib

    def _process_requirement(self,
                             requirement: Union[str, float],
                             context: MutableMapping[str, Any]) -> float:
        return math.ceil(eval_expression(
            expression=requirement,
            context=context,
            full_js=self.full_js,
            expression_lib=self.expression_lib))

    def eval(self, inputs: MutableSequence[Token]) -> Hardware:
        context = {
            'inputs': {t.name: get_token_value(t) for t in inputs}
        }
        return CWLHardware(
            cores=self._process_requirement(self.cores, context),
            memory=self._process_requirement(self.memory, context),
            tmpdir=self._process_requirement(self.tmpdir, context),
            outdir=self._process_requirement(self.outdir, context))
