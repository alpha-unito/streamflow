from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from streamflow.core.workflow import Token


class WorkflowException(Exception):
    pass


class WorkflowDefinitionException(WorkflowException):
    pass


class WorkflowExecutionException(WorkflowException):
    pass


class WorkflowProvenanceException(WorkflowException):
    pass


class FailureHandlingException(WorkflowException):
    pass


class InvalidPluginException(Exception):
    pass


class UnrecoverableTokenException(WorkflowException):
    def __init__(self, message: str, token: Token | None = None):
        super().__init__(message)
        self.token: Token = token
