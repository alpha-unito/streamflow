from typing import MutableMapping

from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import DeploymentConfig, Target
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.workflow import Port, Step, Token, Workflow


class DefaultDatabaseLoadingContext(DatabaseLoadingContext):

    def __init__(self):
        super().__init__()
        self._deployment_configs: MutableMapping[int, DeploymentConfig] = {}
        self._ports: MutableMapping[int, Port] = {}
        self._steps: MutableMapping[int, Step] = {}
        self._targets: MutableMapping[int, Target] = {}
        self._tokens: MutableMapping[int, Token] = {}
        self._workflows: MutableMapping[int, Workflow] = {}

    def add_deployment(self,
                       persistent_id: int,
                       deployment: DeploymentConfig):
        self._deployment_configs[persistent_id] = deployment

    def add_port(self,
                 persistent_id: int,
                 port: Port):
        self._ports[persistent_id] = port

    def add_step(self,
                 persistent_id: int,
                 step: Step):
        self._steps[persistent_id] = step

    def add_target(self,
                   persistent_id: int,
                   target: Target):
        self._targets[persistent_id] = target

    def add_token(self,
                  persistent_id: int,
                  token: Token):
        self._tokens[persistent_id] = token

    def add_workflow(self,
                     persistent_id: int,
                     workflow: Workflow):
        self._workflows[persistent_id] = workflow

    async def load_deployment(self,
                              context: StreamFlowContext,
                              persistent_id: int):
        return self._deployment_configs.get(persistent_id) or await DeploymentConfig.load(context, persistent_id, self)

    async def load_port(self,
                        context: StreamFlowContext,
                        persistent_id: int):
        return self._ports.get(persistent_id) or await Port.load(context, persistent_id, self)

    async def load_step(self,
                        context: StreamFlowContext,
                        persistent_id: int):
        return self._steps.get(persistent_id) or await Step.load(context, persistent_id, self)

    async def load_target(self,
                          context: StreamFlowContext,
                          persistent_id: int):
        return self._targets.get(persistent_id) or await Target.load(context, persistent_id, self)

    async def load_token(self,
                         context: StreamFlowContext,
                         persistent_id: int):
        return self._tokens.get(persistent_id) or await Token.load(context, persistent_id, self)

    async def load_workflow(self,
                            context: StreamFlowContext,
                            persistent_id: int):
        return self._workflows.get(persistent_id) or await Workflow.load(context, persistent_id, self)
