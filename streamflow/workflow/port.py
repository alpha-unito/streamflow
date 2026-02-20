from __future__ import annotations

import logging
from collections.abc import Callable, MutableMapping, MutableSequence
from enum import Flag, auto
from typing import NamedTuple

from streamflow.core.deployment import Connector
from streamflow.core.workflow import Job, Port, Status, Token, Workflow
from streamflow.log_handler import logger
from streamflow.workflow.token import TerminationToken


class BoundaryRule(NamedTuple):
    port: Port
    termination_type: TerminationType


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


class TerminationType(Flag):
    PROPAGATE = auto()
    TERMINATE = auto()


class InterWorkflowPort(Port):
    def __init__(self, workflow: Workflow, name: str):
        super().__init__(workflow, name)
        self.boundaries: MutableMapping[str, MutableSequence[BoundaryRule]] = {}

    def _handle_boundary(self, boundary: BoundaryRule, token: Token) -> None:
        if TerminationType.PROPAGATE in boundary.termination_type:
            if boundary.port is self:
                super().put(token)
            else:
                boundary.port.put(token)
        if TerminationType.TERMINATE in boundary.termination_type:
            if boundary.port is self:
                # super().put(TerminationToken(Status.RECOVERED))
                super().put(TerminationToken())
            else:
                # boundary.port.put(TerminationToken(Status.RECOVERED))
                boundary.port.put(TerminationToken())

    def add_inter_port(
        self,
        port: Port,
        boundary_tag: str,
        termination_type: TerminationType,
    ) -> None:
        boundary = BoundaryRule(port, termination_type)
        self.boundaries.setdefault(boundary_tag, []).append(boundary)

        # Create a copy of `token_list` because the list can be modified within `_handle_self_boundary` method
        for token in list(self.token_list):
            if token.tag == boundary_tag:
                self._handle_boundary(boundary, token)

    def put(self, token: Token) -> None:
        if (
            isinstance(token, TerminationToken)
            or token.tag not in self.boundaries.keys()
        ):
            super().put(token)
        else:
            for boundary in self.boundaries[token.tag]:
                self._handle_boundary(boundary, token)


class InterWorkflowJobPort(InterWorkflowPort, JobPort):
    pass
