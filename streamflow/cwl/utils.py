from __future__ import annotations

from collections import MutableMapping
from typing import TYPE_CHECKING, MutableSequence

import cwltool.expression
from typing_extensions import Text

from streamflow.core.exception import WorkflowDefinitionException
from streamflow.core.utils import get_token_value

if TYPE_CHECKING:
    from streamflow.core.workflow import Job
    from typing import Any, Optional


def build_context(job: Job) -> MutableMapping[Text, Any]:
    context = {
        'inputs': {},
        'self': None,
        'runtime': {}
    }
    for token in job.inputs:
        context['inputs'][token.name] = get_token_value(token)
    context['runtime']['outdir'] = job.output_directory
    context['runtime']['tmpdir'] = job.tmp_directory
    # TODO: populate these fields with the right values
    context['runtime']['cores'] = 1
    context['runtime']['ram'] = 1
    context['runtime']['outdirSize'] = 1024
    context['runtime']['tmpdirSize'] = 1024
    return context


def eval_expression(expression: Text,
                    context: MutableMapping[Text, Any],
                    full_js: bool = False,
                    expression_lib: Optional[MutableSequence[Text]] = None,
                    timeout: Optional[int] = None,
                    strip_whitespace: bool = True):
    if isinstance(expression, Text) and ('$(' in expression or '${' in expression):
        # The default cwltool timeout of 20 seconds is too low: raise it to 10 minutes
        timeout = timeout or 600
        # Call the CWL JavaScript evaluation stack
        return cwltool.expression.interpolate(
            expression,
            context,
            fullJS=full_js,
            jslib=cwltool.expression.jshead(expression_lib or [], context) if full_js else "",
            strip_whitespace=strip_whitespace,
            timeout=timeout)
    else:
        return expression


def get_path_from_token(token_value: MutableMapping[Text, Any]) -> Optional[Text]:
    path = token_value.get('path') or token_value.get('location')
    return path[7:] if path is not None and path.startswith('file://') else path


def infer_type_from_token(token_value: Any) -> Text:
    if token_value is None:
        raise WorkflowDefinitionException('Inputs of type `Any` cannot be null')
    if isinstance(token_value, MutableMapping):
        if 'class' in token_value:
            return token_value['class']
    elif isinstance(token_value, Text):
        return 'string'
    elif isinstance(token_value, int):
        return 'long'
    elif isinstance(token_value, float):
        return 'double'
    elif isinstance(token_value, bool):
        return 'boolean'
    else:
        # Could not infer token type: mark as Any
        return 'Any'
