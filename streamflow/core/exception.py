from __future__ import annotations


class WorkflowException(Exception):
    pass


class WorkflowDefinitionException(WorkflowException):
    pass


class WorkflowExecutionException(WorkflowException):
    pass


class WorkflowProvenanceException(WorkflowException):
    pass


class WorkflowTransferException(WorkflowException):
    pass


# todo: WorkflowFatalException per poterlo usare in giro su streamflow e non essere legato alla fault tolerance?
class FailureHandlingException(WorkflowException):
    pass


class InvalidPluginException(Exception):
    pass
