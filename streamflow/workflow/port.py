from __future__ import annotations

import logging
from collections.abc import MutableMapping, MutableSequence
from typing import Callable

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


class InterWorkflowPort(Port):
    def __init__(self, workflow: Workflow, name: str):
        super().__init__(workflow, name)
        self.inter_ports: MutableMapping[
            str, MutableSequence[MutableMapping[str, Port | bool]]
        ] = {}

    def add_inter_port(
        self,
        port: Port,
        boundary_tag: str,
        inter_terminate: bool = False,
        intra_terminate: bool = False,
    ) -> None:
        self.inter_ports.setdefault(boundary_tag, []).append(
            {
                "port": port,
                "inter_terminate": inter_terminate,
                "intra_terminate": intra_terminate,
            }
        )
        for token in self.token_list:
            if boundary_tag == token.tag:
                port.put(token)
                if inter_terminate:
                    port.put(TerminationToken(Status.SKIPPED))
                if intra_terminate:
                    super().put(TerminationToken(Status.SKIPPED))

    def put(self, token: Token) -> None:
        if not isinstance(token, TerminationToken):
            for inter_port in self.inter_ports.get(token.tag, []):
                inter_port["port"].put(token)
                if inter_port["inter_terminate"]:
                    inter_port["port"].put(TerminationToken(Status.SKIPPED))
                if inter_port["intra_terminate"]:
                    super().put(TerminationToken(Status.SKIPPED))
        super().put(token)


class InterWorkflowJobPort(InterWorkflowPort, JobPort):
    pass
