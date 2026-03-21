from __future__ import annotations


class InvalidPluginException(Exception):
    pass


class ProcessorTypeError(Exception):
    pass


class WorkflowException(Exception):
    pass


class UnrecoverableWorkflowException(WorkflowException):
    pass


class FailureHandlingException(UnrecoverableWorkflowException):
    pass


class WorkflowDefinitionException(UnrecoverableWorkflowException):
    pass


class WorkflowExecutionException(WorkflowException):
    pass


class WorkflowProvenanceException(WorkflowException):
    pass
