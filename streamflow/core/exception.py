from __future__ import annotations


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
