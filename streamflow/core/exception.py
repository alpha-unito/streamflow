from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from streamflow.core.workflow import Token
    from typing_extensions import Text, Optional


class WorkflowException(Exception):
    ...


class WorkflowDefinitionException(WorkflowException):
    ...


class WorkflowExecutionException(WorkflowException):
    ...


class FailureHandlingException(WorkflowException):
    ...


class UnrecoverableTokenException(WorkflowException):

    def __init__(self, message: Text, token: Optional[Token] = None):
        super().__init__(message)
        self.token: Token = token
