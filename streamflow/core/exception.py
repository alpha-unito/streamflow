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


# todo: WorkflowFatalException per poterlo usare in giro su streamflow e non essere legato alla fault tolerance?
class FailureHandlingException(WorkflowException):
    ...


class InvalidPluginException(Exception):
    ...
