from __future__ import annotations

from typing import MutableSequence, Tuple

from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import Connector
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.workflow import Job, Port, Token, Workflow, Step
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
    ):
        super().__init__(workflow, name)
        self.stop_tags = stop_tags

    def put(self, token: Token):
        if token.tag in self.stop_tags:
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


class InterWorkflowPort:
    def __init__(self, intra_port):
        self.intra_port: Port = intra_port
        self.inter_ports: MutableSequence[Tuple[Port, str]] = []

    @property
    def persistent_id(self):
        return self.intra_port.persistent_id

    @persistent_id.setter
    def persistent_id(self, value):
        self.intra_port.persistent_id = value

    @property
    def queues(self):
        return self.intra_port.queues

    @queues.setter
    def queues(self, value):
        self.intra_port.queues = value

    @property
    def name(self):
        return self.intra_port.name

    @name.setter
    def name(self, value):
        self.intra_port.name = value

    @property
    def token_list(self):
        return self.intra_port.token_list

    @token_list.setter
    def token_list(self, value):
        self.intra_port.token_list = value

    @property
    def workflow(self):
        return self.intra_port.workflow

    @workflow.setter
    def workflow(self, value):
        self.intra_port.workflow = value

    def add_inter_port(self, port, border_tag):
        self.inter_ports.append((port, border_tag))

    def close(self, consumer: str):
        self.intra_port.close(consumer)

    def empty(self) -> bool:
        return not self.intra_port.token_list

    async def get(self, consumer: str) -> Token:
        return await self.intra_port.get(consumer)

    def get_input_steps(self) -> MutableSequence[Step]:
        return self.intra_port.get_input_steps()

    def get_output_steps(self) -> MutableSequence[Step]:
        return self.intra_port.get_output_steps()

    @classmethod
    async def load(
        cls,
        context: StreamFlowContext,
        persistent_id: int,
        loading_context: DatabaseLoadingContext,
    ) -> Port:
        return await loading_context.load_port(context, persistent_id)

    def put(self, token: Token):
        if not isinstance(token, TerminationToken):
            for port, border_tag in self.inter_ports:
                if border_tag == token.tag:
                    port.put(token)
        self.intra_port.put(token)

    async def save(self, context: StreamFlowContext) -> None:
        await self.intra_port.save(context)
