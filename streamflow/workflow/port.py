from __future__ import annotations

from typing import MutableSequence

from streamflow.core.context import StreamFlowContext
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
        stop_tags: MutableSequence[str],
        port: Port | None = None,
    ):
        super().__init__(workflow, name)
        self.port = port
        self.stop_tags = stop_tags

    def put(self, token: Token):
        if token.tag in self.stop_tags:
            if self.port and not isinstance(token, TerminationToken):
                logger.info(
                    f"Port {self.name} of wf {self.workflow.name} received token {token.tag} (type: {type(token)}). Token is also putted in the same port of wf {self.port.workflow.name}"
                )
                self.port.put(token)
            logger.info(
                f"Port {self.name} of wf {self.workflow.name} received token {token.tag} but it will replace with TerminationToken"
            )
            super().put(TerminationToken())
        else:
            super().put(token)

    async def save(self, context: StreamFlowContext) -> None:
        async with self.persistence_lock:
            if not self.persistent_id:
                self.persistent_id = await context.database.add_port(
                    name=self.name,
                    workflow_id=self.workflow.persistent_id,
                    type=Port,
                    params=await self._save_additional_params(context),
                )
