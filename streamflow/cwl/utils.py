from __future__ import annotations

from collections import MutableMapping
from typing import TYPE_CHECKING, MutableSequence, Set

import cwltool.expression

from streamflow.core.exception import WorkflowDefinitionException
from streamflow.core.utils import get_token_value
from streamflow.cwl.expression import interpolate

if TYPE_CHECKING:
    from streamflow.core.workflow import Job
    from typing import Any, Optional


def build_context(job: Job) -> MutableMapping[str, Any]:
    context = {
        'inputs': {},
        'self': None,
        'runtime': {}
    }
    for token in job.inputs:
        context['inputs'][token.name] = get_token_value(token)
    context['runtime']['outdir'] = job.output_directory
    context['runtime']['tmpdir'] = job.tmp_directory
    if job.hardware:
        context['runtime']['cores'] = job.hardware.cores
        context['runtime']['ram'] = job.hardware.memory
        # noinspection PyUnresolvedReferences
        context['runtime']['tmpdirSize'] = job.hardware.tmpdir
        # noinspection PyUnresolvedReferences
        context['runtime']['outdirSize'] = job.hardware.outdir
    return context


def eval_expression(expression: str,
                    context: MutableMapping[str, Any],
                    full_js: bool = False,
                    expression_lib: Optional[MutableSequence[str]] = None,
                    timeout: Optional[int] = None,
                    strip_whitespace: bool = True) -> Any:
    if isinstance(expression, str) and ('$(' in expression or '${' in expression):
        return interpolate(
            expression,
            context,
            full_js=full_js,
            jslib=cwltool.expression.jshead(expression_lib or [], context) if full_js else "",
            strip_whitespace=strip_whitespace,
            timeout=timeout)
    else:
        return expression


def get_path_from_token(token_value: MutableMapping[str, Any]) -> Optional[str]:
    path = token_value.get('path', token_value.get('location'))
    return path[7:] if path is not None and path.startswith('file://') else path


def infer_type_from_token(token_value: Any) -> str:
    if token_value is None:
        raise WorkflowDefinitionException('Inputs of type `Any` cannot be null')
    if isinstance(token_value, MutableMapping):
        if 'class' in token_value:
            return token_value['class']
        else:
            return 'record'
    elif isinstance(token_value, MutableSequence):
        return 'array'
    elif isinstance(token_value, str):
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


def resolve_dependencies(expression: str,
                         full_js: bool = False,
                         expression_lib: Optional[MutableSequence[str]] = None,
                         timeout: Optional[int] = None,
                         strip_whitespace: bool = True) -> Set[str]:
    if isinstance(expression, str) and ('$(' in expression or '${' in expression):
        context = {'inputs': {}, 'self': {}, 'runtime': {}}
        return interpolate(
            expression,
            context,
            full_js=full_js,
            jslib=cwltool.expression.jshead(expression_lib or [], context) if full_js else "",
            strip_whitespace=strip_whitespace,
            timeout=timeout,
            resolve_dependencies=True)
    else:
        return set()
