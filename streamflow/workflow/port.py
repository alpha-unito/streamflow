from __future__ import annotations

import logging
from collections.abc import Callable, MutableSequence
from enum import Flag, auto
from typing import NamedTuple

from streamflow.core.deployment import Connector
from streamflow.core.workflow import Job, Port, Status, Token, Workflow
from streamflow.log_handler import logger
from streamflow.workflow.token import TerminationToken


class BoundaryAction(Flag):
    PROPAGATE = auto()
    TERMINATE = auto()


class BoundaryRule(NamedTuple):
    port: Port
    action: BoundaryAction
    tags: MutableSequence[str]


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

    def _handle_boundary(self, boundary: BoundaryRule, token: Token) -> bool:
        if token.tag in boundary.tags:
            boundary.tags.remove(token.tag)
            if len(boundary.tags) == 0:
                if BoundaryAction.PROPAGATE in boundary.action:
                    if boundary.port is self:
                        super().put(token)
                    else:
                        boundary.port.put(token)
                if BoundaryAction.TERMINATE in boundary.action:
                    if boundary.port is self:
                        super().put(TerminationToken(Status.RECOVERED))
                    else:
                        boundary.port.put(TerminationToken(Status.RECOVERED))
                return True
            else:
                return False
        else:
            # Duplicated tokens. e.g. size port of a gather step.
            # first token is injected by the recovery system,
            # second token is injected by the scatter size output
            return False

    def add_inter_port(
        self,
        port: Port,
        boundary_tags: MutableSequence[str],
        boundary_action: BoundaryAction,
    ) -> None:
        boundary = BoundaryRule(port=port, action=boundary_action, tags=boundary_tags)
        self.boundaries.append(boundary)

        # Create a copy of `token_list` because the list can be modified within `_handle_self_boundary` method
        for token in list(self.token_list):
            self._handle_boundary(boundary, token)

    def put(self, token: Token) -> None:
        if isinstance(token, TerminationToken):
            super().put(token)
        else:
            self_rule = False
            for boundary in self.boundaries:
                if self._handle_boundary(boundary, token) and boundary.port is self:
                    self_rule = True
            if not self_rule:
                super().put(token)


class InterWorkflowJobPort(InterWorkflowPort, JobPort):
    pass
