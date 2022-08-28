from typing import Optional

from streamflow.core.deployment import Connector
from streamflow.core.workflow import Job, Port, Token
from streamflow.workflow.token import TerminationToken


class ConnectorPort(Port):

    async def get_connector(self, consumer: str) -> Connector:
        token = await self.get(consumer)
        return self.workflow.context.deployment_manager.get_connector(token.value)

    def put_connector(self, connector_name: str):
        self.put(Token(value=connector_name))


class JobPort(Port):

    async def get_job(self, consumer: str) -> Optional[Job]:
        token = await self.get(consumer)
        if isinstance(token, TerminationToken):
            return None
        else:
            return token.value

    def put_job(self, job: Job):
        self.put(Token(value=job))
