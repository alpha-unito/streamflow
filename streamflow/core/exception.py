from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from streamflow.core.workflow import Token


class WorkflowException(Exception):
    ...


class WorkflowDefinitionException(WorkflowException):
    ...


class WorkflowExecutionException(WorkflowException):
    ...


class WorkflowProvenanceException(WorkflowException):
    ...


class FailureHandlingException(WorkflowException):
    ...


class InvalidPluginException(Exception):
    ...


class UnrecoverableTokenException(WorkflowException):
    def __init__(self, message: str, token: Token | None = None):
        super().__init__(message)
        self.token: Token = token
