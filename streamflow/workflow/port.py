from __future__ import annotations

import logging
from collections.abc import Callable, MutableSequence
from enum import Flag, auto

from streamflow.core.deployment import Connector
from streamflow.core.workflow import Job, Port, Status, Token, Workflow
from streamflow.log_handler import logger
from streamflow.workflow.token import TerminationToken


class BoundaryAction(Flag):
    PROPAGATE = auto()
    TERMINATE = auto()


class BoundaryRule:
    __slots__ = ("action", "port", "tags")

    def __init__(
        self, action: BoundaryAction, port: Port, tags: MutableSequence[str]
    ) -> None:
        self.action: BoundaryAction = action
        self.port: Port = port
        self.tags: MutableSequence[str] = tags

    def is_satisfied(self) -> bool:
        return len(self.tags) == 0

    def remove_tag(self, tag: str) -> None:
        if tag in self.tags:
            self.tags.remove(tag)


class ConnectorPort(Port):
    async def get_connector(self, consumer: str) -> Connector:
        token = await self.get(consumer)
        return self.workflow.context.deployment_manager.get_connector(token.value)

    def put_connector(self, connector_name: str) -> None:
        self.put(Token(value=connector_name))


class JobPort(Port):
    async def get_job(self, consumer: str) -> Job | None:
        token = await self.get(consumer)
        if isinstance(token, TerminationToken):
            return None
        else:
            return token.value

    def put_job(self, job: Job) -> None:
        self.put(Token(value=job))


class FilterTokenPort(Port):
    def __init__(
        self,
        workflow: Workflow,
        name: str,
        filter_function: Callable[[Token], bool] | None = None,
    ):
        super().__init__(workflow, name)
        self.filter_function: Callable[[Token], bool] = filter_function or (
            lambda _: True
        )

    def put(self, token: Token) -> None:
        if isinstance(token, TerminationToken) or self.filter_function(token):
            super().put(token)
        elif logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Port {self.name} skips {token.tag}")


class InterWorkflowPort(Port):
    def __init__(self, workflow: Workflow, name: str):
        super().__init__(workflow, name)
        self.boundaries: MutableSequence[BoundaryRule] = []

    def _execute_boundary_action(self, boundary: BoundaryRule, token: Token) -> None:
        target = boundary.port if boundary.port is not self else super()

        if BoundaryAction.PROPAGATE in boundary.action:
            target.put(token)
        if BoundaryAction.TERMINATE in boundary.action:
            target.put(TerminationToken(Status.RECOVERED))

    def add_inter_port(
        self,
        port: Port,
        boundary_tags: MutableSequence[str],
        boundary_action: BoundaryAction,
    ) -> None:
        # Deep copy of `boundary_tags` because it will be manipulated
        boundary = BoundaryRule(
            port=port, action=boundary_action, tags=list(boundary_tags)
        )
        self.boundaries.append(boundary)

        # Create a copy of `token_list` because the list can be modified
        # within `_execute_boundary_action` method, e.g. adding a termination token
        for token in [
            t for t in self.token_list if not isinstance(t, TerminationToken)
        ]:
            boundary.remove_tag(token.tag)
            if boundary.is_satisfied():
                self._execute_boundary_action(boundary, token)

    def put(self, token: Token) -> None:
        if isinstance(token, TerminationToken):
            super().put(token)
        else:
            matched_self = False
            for boundary in self.boundaries:
                boundary.remove_tag(token.tag)
                if boundary.is_satisfied():
                    self._execute_boundary_action(boundary, token)
                    if boundary.port is self:
                        matched_self = True
            if not matched_self:
                super().put(token)


class InterWorkflowJobPort(InterWorkflowPort, JobPort):
    pass
