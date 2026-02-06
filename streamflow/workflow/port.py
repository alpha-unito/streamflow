from __future__ import annotations

import logging
from collections.abc import Callable, MutableMapping, MutableSequence
from enum import Enum

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


class TerminationType(Enum):
    PROPAGATE_AND_TERMINATE = 0
    TERMINATE = 1
    PROPAGATE = 2


class InterWorkflowPort(Port):
    def __init__(self, workflow: Workflow, name: str):
        super().__init__(workflow, name)
        self.boundaries: MutableMapping[
            str, MutableSequence[tuple[Port, TerminationType]]
        ] = {}

    def _handle_boundary(
        self, port: Port, token: Token, termination_type: TerminationType, msg: str
    ) -> None:
        if termination_type in (
            TerminationType.PROPAGATE,
            TerminationType.PROPAGATE_AND_TERMINATE,
        ):
            if port is self:
                logger.info(
                    f"Port {self.name} {msg}. self boundary propagating token {token.tag} {type(token)}"
                )
                super().put(token)
            else:
                logger.info(
                    f"Port {self.name} {msg}. boundary propagating token {token.tag} {type(token)}"
                )
                port.put(token)
        if termination_type in (
            TerminationType.TERMINATE,
            TerminationType.PROPAGATE_AND_TERMINATE,
        ):
            if port is self:
                logger.info(
                    f"Port {self.name} {msg}. self boundary termination token  (input token {token.tag} {type(token)})"
                )
                super().put(TerminationToken(Status.RECOVERED))
            else:
                logger.info(
                    f"Port {self.name} {msg}. boundary termination token  (input token {token.tag} {type(token)})"
                )
                port.put(TerminationToken(Status.RECOVERED))

    def add_inter_port(
        self,
        port: Port,
        boundary_tag: str,
        termination_type: TerminationType = TerminationType.TERMINATE,
    ) -> None:
        self.boundaries.setdefault(boundary_tag, []).append((port, termination_type))

        # Range of tag necessary before to propagate the boundary tag
        # NOTE. currently the range start from 0 to boundary tag
        #  it is better pass as argument the min of the range.
        #  It can be necessary for the loop (?)
        # levels = boundary_tag.split(".")
        # a = not bool(
        #     {".".join((*levels[:-1], str(i))) for i in range(int(levels[-1]))}
        #     - {t.tag for t in self.token_list if not isinstance(t, TerminationToken)}
        # )

        # hard copy because the list can be increased in self._handle_self_boundary
        for token in list(self.token_list):
            if token.tag == boundary_tag:
                self._handle_boundary(port, token, termination_type, "add_inter_port")

    def put(self, token: Token) -> None:
        if (
            isinstance(token, TerminationToken)
            or token.tag not in self.boundaries.keys()
        ):
            super().put(token)
        else:
            for boundary in self.boundaries[token.tag]:
                self._handle_boundary(boundary[0], token, boundary[1], "put")


class InterWorkflowJobPort(InterWorkflowPort, JobPort):
    pass
