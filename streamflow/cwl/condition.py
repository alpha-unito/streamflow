from typing import Text, Optional, List

import cwltool.expression

from streamflow.core.workflow import Condition, Job
from streamflow.cwl import utils
from streamflow.workflow.exception import WorkflowDefinitionException


class CWLCondition(Condition):

    def __init__(self,
                 when_expression: Text,
                 expression_lib: Optional[List[Text]] = None,
                 full_js: bool = False):
        super().__init__()
        self.when_expression: Text = when_expression
        self.expression_lib: Optional[List[Text]] = expression_lib
        self.full_js: bool = full_js

    async def evaluate(self, job: Job) -> bool:
        context = utils.build_context(job)
        condition = cwltool.expression.interpolate(
            self.when_expression,
            context,
            fullJS=self.full_js,
            jslib=cwltool.expression.jshead(
                self.expression_lib or [], context) if self.full_js else "")
        if condition is True or condition is False:
            return condition
        else:
            raise WorkflowDefinitionException("Conditional 'when' must evaluate to 'true' or 'false'")
