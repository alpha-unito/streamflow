from __future__ import annotations

import logging
from collections.abc import Callable, MutableMapping, MutableSequence
from typing import NamedTuple

from streamflow.core.deployment import Connector
from streamflow.core.workflow import Job, Port, Status, Token, Workflow
from streamflow.log_handler import logger
from streamflow.workflow.token import TerminationToken


class ConnectorPort(Port):
    async def get_connector(self, consumer: str) -> Connector:
        token = await self.get(consumer)
        return self.workflow.context.deployment_manager.get_connector(token.value)

    def put_connector(self, connector_name: str):
        self.put(Token(value=connector_name))


class JobPort(Port):
    async def get_job(self, consumer: str) -> Job | None:
        token = await self.get(consumer)
        if isinstance(token, TerminationToken):
            return None
        else:
            return token.value

    def put_job(self, job: Job):
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


class BoundaryRule(NamedTuple):
    port: Port
    inter_terminate: bool = False
    intra_terminate: bool = False


class InterWorkflowPort(Port):
    def __init__(self, workflow: Workflow, name: str):
        super().__init__(workflow, name)
        self.boundaries: MutableMapping[str, MutableSequence[BoundaryRule]] = {}

    def _handle_boundary(self, token: Token, boundary: BoundaryRule) -> None:
        logger.info(f"Port {self.name} ({id(self)}) passing token {token.tag}")
        boundary.port.put(token)
        if boundary.inter_terminate:
            logger.info(
                f"Port {self.name} ({id(self)}) passing inter termination token"
            )
            boundary.port.put(TerminationToken(Status.RECOVERED))
        if boundary.intra_terminate:
            logger.info(
                f"Port {self.name} ({id(self)}) passing intra termination token"
            )
            super().put(TerminationToken(Status.RECOVERED))

    def add_inter_port(
        self,
        port: Port,
        boundary_tag: str,
        inter_terminate: bool = False,
        intra_terminate: bool = False,
    ) -> None:
        boundary = BoundaryRule(
            port=port, inter_terminate=inter_terminate, intra_terminate=intra_terminate
        )
        self.boundaries.setdefault(boundary_tag, []).append(boundary)
        logger.info(f"Port {self.name} ({id(self)}) inter port {boundary_tag}")
        for token in self.token_list:
            if token.tag == boundary_tag:
                self._handle_boundary(token, boundary)

    def put(self, token: Token) -> None:
        if not isinstance(token, TerminationToken):
            for route in self.boundaries.get(token.tag, ()):
                self._handle_boundary(token, route)
        super().put(token)


class InterWorkflowJobPort(InterWorkflowPort, JobPort):
    pass
