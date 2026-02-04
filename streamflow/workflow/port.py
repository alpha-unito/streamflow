from __future__ import annotations

import logging
from collections.abc import Callable, MutableMapping, MutableSequence

from streamflow.core.deployment import Connector
from streamflow.core.exception import WorkflowDefinitionException
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


class InterWorkflowPort(Port):
    def __init__(self, workflow: Workflow, name: str):
        super().__init__(workflow, name)
        self.boundaries: MutableMapping[str, MutableSequence[tuple[Port, bool]]] = {}

    def _handle_self_boundary(self, token: Token) -> None:
        if next(
            (
                boundary
                for boundary in self.boundaries.get(token.tag, ())
                if boundary[0] is self
            ),
            None,
        ) is not None:
            logger.info(f"Port {self.name} add inter_port. self termination token after {token.tag} (ignored input token {type(token)})")
            super().put(TerminationToken(Status.RECOVERED))
        else:
            logger.info(f"Port {self.name} add inter_port. self put token after {token.tag} ({type(token)})")
            super().put(token)


    def add_inter_port(
        self,
        port: Port,
        boundary_tag: str,
        terminate: bool = True,
    ) -> None:
        if port is self and not terminate:
            raise WorkflowDefinitionException(
                f"Impossible to add self boundary without termination in the port {self.name}"
            )
        self.boundaries.setdefault(boundary_tag, []).append((port, terminate))
        for token in list(self.token_list): # hard copy because the list can be increased in self._handle_self_boundary
            if token.tag == boundary_tag:
                if port is not self:
                    logger.info(f"Port {self.name} add inter_port. put token {token.tag} ({type(token)})")
                    port.put(token)
                    if terminate:
                        logger.info(f"Port {self.name} add inter_port. put termination token after {token.tag} ({type(token)})")
                        port.put(TerminationToken(Status.RECOVERED))
                else:
                    self._handle_self_boundary(token)

    def put(self, token: Token) -> None:
        if not isinstance(token, TerminationToken):
            for boundary in self.boundaries.get(token.tag, ()):
                if boundary[0] is not self:
                    boundary[0].put(token)
                    if boundary[1]:
                        boundary[0].put(TerminationToken(Status.RECOVERED))
        self._handle_self_boundary(token)


class InterWorkflowJobPort(InterWorkflowPort, JobPort):
    pass
