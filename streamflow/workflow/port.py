from __future__ import annotations

import logging
from collections.abc import Callable, MutableMapping, MutableSequence

from streamflow.core.deployment import Connector
from streamflow.core.workflow import Job, Port, Token, Workflow
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


class InterWorkflowPort(Port):
    def __init__(self, workflow: Workflow, name: str):
        super().__init__(workflow, name)
        self.boundaries: MutableMapping[str, MutableSequence[tuple[Port, bool]]] = {}

    def add_inter_port(
        self,
        port: Port,
        boundary_tag: str,
        terminate: bool,
    ) -> None:
        self.boundaries.setdefault(boundary_tag, []).append((port, terminate))
        for token in self.token_list:
            if token.tag == boundary_tag:
                port.put(token)
                if terminate:
                    port.put(TerminationToken())

    def put(self, token: Token) -> None:
        can_continue = True
        if not isinstance(token, TerminationToken):
            for boundary in self.boundaries.get(token.tag, ()):
                if boundary[0] is not self:
                    boundary[0].put(token)
                if boundary[1]:
                    can_continue = can_continue and boundary[0] is not self
                    boundary[0].put(TerminationToken())
        # If the self port injected a termination token, no need to propagate current token
        if can_continue:
            super().put(token)


class InterWorkflowJobPort(InterWorkflowPort, JobPort):
    pass
