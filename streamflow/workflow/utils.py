from __future__ import annotations

import logging
import posixpath
from collections.abc import Iterable, MutableMapping, MutableSequence
from typing import Any

from streamflow.config.config import WorkflowConfig
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.workflow import Token, Workflow
from streamflow.log_handler import logger
from streamflow.workflow.token import (
    IterationTerminationToken,
    JobToken,
    ListToken,
    ObjectToken,
    TerminationToken,
)


def _check_bindings(
    current_node: MutableMapping[str, Any], workflow: Workflow, path: str
) -> None:
    if "step" in current_node:
        if not any(step.startswith(path) for step in workflow.steps.keys()):
            # The `step` bind check if there is a step named /parent/step_name
            #   (it includes also sub-workflow bindings)
            logger.warning(
                f"Binding {path}, defined in the StreamFlow file, does not match any steps in the workflow"
            )
            return
    if "port" in current_node:
        if not (
            (
                # The port is an input port of the workflow
                posixpath.dirname(path) == posixpath.sep
                and posixpath.basename(path) in workflow.input_ports.keys()
            )
            or (
                # The port is an output port of a step
                posixpath.dirname(path) in workflow.steps
                and posixpath.basename(path)
                in workflow.steps[posixpath.dirname(path)].output_ports
            )
        ):
            logger.warning(
                f"Binding {path}, defined in the StreamFlow file, does not match any ports in the workflow"
            )
    for key, node in current_node["children"].items():
        _check_bindings(node, workflow, posixpath.join(path, key))


def check_bindings(workflow_config: WorkflowConfig, workflow: Workflow) -> None:
    if (
        logger.isEnabledFor(logging.WARNING)
        and len(node := workflow_config.filesystem["children"]) == 1
    ):
        _check_bindings(node[posixpath.sep], workflow, posixpath.sep)


def check_iteration_termination(inputs: Token | Iterable[Token]) -> bool:
    return check_token_class(inputs, IterationTerminationToken)


def check_termination(inputs: Token | Iterable[Token]) -> bool:
    return check_token_class(inputs, TerminationToken)


def check_token_class(inputs: Token | Iterable[Token], cls: type[Token]) -> bool:
    if isinstance(inputs, Token):
        return isinstance(inputs, cls)
    else:
        for token in inputs:
            if isinstance(token, MutableSequence):
                if check_token_class(token, cls):
                    return True
            elif isinstance(token, cls):
                return True
        return False


def get_token_value(token: Token) -> Any:
    if isinstance(token, ListToken):
        return [get_token_value(t) for t in token.value]
    elif isinstance(token, ObjectToken):
        return {k: get_token_value(v) for k, v in token.value.items()}
    else:
        return token.value


def get_job_token(job_name: str, token_list: MutableSequence[Token]) -> JobToken:
    for token in token_list:
        if isinstance(token, JobToken) and token.value.name == job_name:
            return token
    raise WorkflowExecutionException(
        f"Impossible to find job {job_name} in token_list {token_list}"
    )
