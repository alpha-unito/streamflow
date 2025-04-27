from __future__ import annotations

import logging
from collections.abc import MutableSequence
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
        filter_function: Callable | None = None,
    ):
        super().__init__(workflow, name)
        self.filter_function: Callable = filter_function or (lambda _: True)

    def put(self, token: Token):
        if isinstance(token, TerminationToken) or self.filter_function(token):
            super().put(token)
        elif logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Port {self.name} skips {token.tag}")


class InterWorkflowPort(Port):
    def __init__(self, workflow: Workflow, name: str, interrupt: bool = False):
        super().__init__(workflow, name)
        self.inter_ports: MutableSequence[tuple[Port, str | None]] = []
        self.interrupt: bool = interrupt

    def add_inter_port(self, port: Port, border_tag: str | None = None) -> None:
        self.inter_ports.append((port, border_tag))
        for token in self.token_list:
            if border_tag is None or border_tag == token.tag:
                port.put(token)
                if self.interrupt:
                    super().put(TerminationToken(value=Status.SKIPPED))
                    return

    def put(self, token: Token) -> None:
        if not isinstance(token, TerminationToken):
            for port, border_tag in self.inter_ports:
                if border_tag is None or border_tag == token.tag:
                    port.put(token)
                    if self.interrupt:
                        super().put(TerminationToken(value=Status.SKIPPED))
                        return
        super().put(token)


class InterWorkflowJobPort(JobPort):
    def __init__(self, workflow: Workflow, name: str, interrupt: bool = False):
        super().__init__(workflow, name)
        self.inter_ports: MutableSequence[tuple[Port, str | None]] = []
        self.interrupt: bool = interrupt

    def add_inter_port(self, port: Port, border_tag: str | None = None) -> None:
        self.inter_ports.append((port, border_tag))
        for token in self.token_list:
            if border_tag is None or border_tag == token.tag:
                port.put(token)
                if self.interrupt:
                    super().put(TerminationToken(value=Status.SKIPPED))
                    return

    def put(self, token: Token) -> None:
        if not isinstance(token, TerminationToken):
            for port, border_tag in self.inter_ports:
                if border_tag is None or border_tag == token.tag:
                    port.put(token)
                    if self.interrupt:
                        super().put(TerminationToken(value=Status.SKIPPED))
                        return
        super().put(token)

    async def get_job(self, consumer: str) -> Job | None:
        return await super().get_job(consumer)

    def put_job(self, job: Job):
        super().put_job(job)
