from typing import Optional, MutableSequence

from streamflow.core.exception import WorkflowDefinitionException
from streamflow.core.workflow import Condition, Job, Step
from streamflow.cwl import utils


class CWLCondition(Condition):

    def __init__(self,
                 step: Step,
                 expression: str,
                 full_js: bool = False,
                 expression_lib: Optional[MutableSequence[str]] = None):
        super().__init__(step)
        self.expression: str = expression
        self.full_js: bool = full_js
        self.expression_lib: Optional[MutableSequence[str]] = expression_lib

    async def eval(self, job: Job) -> bool:
        context = utils.build_context(job)
        condition = utils.eval_expression(
            expression=self.expression,
            context=context,
            full_js=self.full_js,
            expression_lib=self.expression_lib)
        if condition is True or condition is False:
            return condition
        else:
            raise WorkflowDefinitionException("Conditional 'when' must evaluate to 'true' or 'false'")
