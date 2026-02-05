from __future__ import annotations

import logging
from collections.abc import Callable, MutableMapping, MutableSequence
from enum import Enum

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

    def _handle_self_boundary(self, token: Token) -> None:
        if (
            boundary := next(
                (
                    boundary
                    for boundary in self.boundaries.get(token.tag, ())
                    if boundary[0] is self
                ),
                None,
            )
        ) is not None:
            if boundary[1] in (
                TerminationType.PROPAGATE,
                TerminationType.PROPAGATE_AND_TERMINATE,
            ):
                if token.tag not in (
                    t.tag for t in self.token_list
                ):  # DEBUG check. remove it
                    logger.info(
                        f"Port {self.name} _handle_self_boundary. propagating token {token.tag} {type(token)}"
                    )
                    super().put(token)
                else:
                    logger.info(
                        f"Port {self.name} _handle_self_boundary. skips propagation token {token.tag} {type(token)}"
                    )
            if boundary[1] in (
                TerminationType.TERMINATE,
                TerminationType.PROPAGATE_AND_TERMINATE,
            ):
                logger.info(
                    f"Port {self.name} _handle_self_boundary. termination token (input token {token.tag} {type(token)})"
                )
                super().put(TerminationToken(Status.RECOVERED))
        else:
            # logger.info(
            #     f"Port {self.name} add inter_port. self put token {token.tag} {type(token)}"
            # )
            super().put(token)

    def add_inter_port(
        self,
        port: Port,
        boundary_tag: str,
        termination_type: TerminationType = TerminationType.TERMINATE,
    ) -> None:
        if port is self and termination_type not in (
            TerminationType.TERMINATE,
            TerminationType.PROPAGATE_AND_TERMINATE,
        ):
            raise WorkflowDefinitionException(
                f"Impossible to add self boundary without termination in the port {self.name}"
            )
        if port is self and termination_type == TerminationType.PROPAGATE_AND_TERMINATE:
            pass
        self.boundaries.setdefault(boundary_tag, []).append((port, termination_type))
        # hard copy because the list can be increased in self._handle_self_boundary
        for token in list(self.token_list):
            if token.tag == boundary_tag:
                if port is not self:
                    if termination_type in (
                        TerminationType.PROPAGATE,
                        TerminationType.PROPAGATE_AND_TERMINATE,
                    ):
                        logger.info(
                            f"Port {self.name} add_inter_port. boundary propagating token {token.tag} {type(token)}"
                        )
                        port.put(token)
                    if termination_type in (
                        TerminationType.TERMINATE,
                        TerminationType.PROPAGATE_AND_TERMINATE,
                    ):
                        logger.info(
                            f"Port {self.name} add_inter_port. boundary termination token  (input token {token.tag} {type(token)})"
                        )
                        port.put(TerminationToken(Status.RECOVERED))
                else:
                    self._handle_self_boundary(token)

    def put(self, token: Token) -> None:
        if not isinstance(token, TerminationToken):
            for boundary in self.boundaries.get(token.tag, ()):
                if boundary[0] is not self:
                    if boundary[1] in (
                        TerminationType.PROPAGATE,
                        TerminationType.PROPAGATE_AND_TERMINATE,
                    ):
                        logger.info(
                            f"Port {self.name} put. boundary propagating token {token.tag} {type(token)}"
                        )
                        boundary[0].put(token)
                    if boundary[1] in (
                        TerminationType.TERMINATE,
                        TerminationType.PROPAGATE_AND_TERMINATE,
                    ):
                        logger.info(
                            f"Port {self.name} put. boundary termination token  (input token {token.tag} {type(token)})"
                        )
                        boundary[0].put(TerminationToken(Status.RECOVERED))
        self._handle_self_boundary(token)


class InterWorkflowJobPort(InterWorkflowPort, JobPort):
    pass
