from __future__ import annotations


class ProcessorTypeError(Exception):
    pass


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
