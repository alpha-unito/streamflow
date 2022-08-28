from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from streamflow.core.workflow import Token
    from typing import Optional


class WorkflowException(Exception):
    ...


class WorkflowDefinitionException(WorkflowException):
    ...


class WorkflowExecutionException(WorkflowException):
    ...


class FailureHandlingException(WorkflowException):
    ...


class InvalidPluginException(Exception):
    ...


class UnrecoverableTokenException(WorkflowException):

    def __init__(self, message: str, token: Optional[Token] = None):
        super().__init__(message)
        self.token: Token = token
