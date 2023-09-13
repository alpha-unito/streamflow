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


class WorkflowTransferException(WorkflowException):
    ...


class FailureHandlingException(WorkflowException):
    ...


class InvalidPluginException(Exception):
    ...
