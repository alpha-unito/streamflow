from __future__ import annotations
from typing import MutableMapping

from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import DeploymentConfig, Target, FilterConfig
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.workflow import Port, Step, Token, Workflow


class DefaultDatabaseLoadingContext(DatabaseLoadingContext):
    def __init__(self):
        super().__init__()
        self._deployment_configs: MutableMapping[int, DeploymentConfig] = {}
        self._ports: MutableMapping[int, Port] = {}
        self._steps: MutableMapping[int, Step] = {}
        self._targets: MutableMapping[int, Target] = {}
        self._filter_configs: MutableMapping[int, FilterConfig] = {}
        self._tokens: MutableMapping[int, Token] = {}
        self._workflows: MutableMapping[int, Workflow] = {}

    def add_deployment(self, persistent_id: int, deployment: DeploymentConfig):
        self._deployment_configs[persistent_id] = deployment

    def add_filter(self, persistent_id: int, filter_config: FilterConfig):
        self._filter_configs[persistent_id] = filter_config

    def add_port(self, persistent_id: int, port: Port):
        self._ports[persistent_id] = port

    def add_step(self, persistent_id: int, step: Step):
        self._steps[persistent_id] = step

    def add_target(self, persistent_id: int, target: Target):
        self._targets[persistent_id] = target

    def add_token(self, persistent_id: int, token: Token):
        self._tokens[persistent_id] = token

    def add_workflow(self, persistent_id: int, workflow: Workflow):
        self._workflows[persistent_id] = workflow

    async def load_deployment(self, context: StreamFlowContext, persistent_id: int):
        return self._deployment_configs.get(
            persistent_id
        ) or await DeploymentConfig.load(context, persistent_id, self)

    async def load_filter(self, context: StreamFlowContext, persistent_id: int):
        return self._filter_configs.get(persistent_id) or await FilterConfig.load(
            context, persistent_id, self
        )

    async def load_port(self, context: StreamFlowContext, persistent_id: int):
        return self._ports.get(persistent_id) or await Port.load(
            context, persistent_id, self
        )

    async def load_step(self, context: StreamFlowContext, persistent_id: int):
        return self._steps.get(persistent_id) or await Step.load(
            context, persistent_id, self
        )

    async def load_target(self, context: StreamFlowContext, persistent_id: int):
        return self._targets.get(persistent_id) or await Target.load(
            context, persistent_id, self
        )

    async def load_token(self, context: StreamFlowContext, persistent_id: int):
        return self._tokens.get(persistent_id) or await Token.load(
            context, persistent_id, self
        )

    async def load_workflow(self, context: StreamFlowContext, persistent_id: int):
        return self._workflows.get(persistent_id) or await Workflow.load(
            context, persistent_id, self
        )

    def is_standard_loading(self) -> bool:
        return True


class WorkflowLoader(DatabaseLoadingContext):
    def __init__(self, workflow: Workflow):
        super().__init__()
        self.workflow: Workflow = workflow
        self._tokens: MutableMapping[int, Token] = {}

    def is_standard_loading(self) -> bool:
        return False

    def add_deployment(self, persistent_id: int, deployment: DeploymentConfig):
        ...

    def add_filter(self, persistent_id: int, filter_config: FilterConfig):
        ...

    def add_port(self, persistent_id: int, port: Port):
        ...

    def add_step(self, persistent_id: int, step: Step):
        ...

    def add_target(self, persistent_id: int, target: Target):
        ...

    def add_token(self, persistent_id: int, token: Token):
        self._tokens[persistent_id] = token

    def add_workflow(self, persistent_id: int, workflow: Workflow):
        ...

    async def load_deployment(self, context: StreamFlowContext, persistent_id: int):
        return await DeploymentConfig.load(context, persistent_id, self)

    async def load_filter(self, context: StreamFlowContext, persistent_id: int):
        return await FilterConfig.load(context, persistent_id, self)

    async def load_port(
        self,
        context: StreamFlowContext,
        persistent_id: int,
    ):
        port_row = await context.database.get_port(persistent_id)
        if port := self.workflow.ports.get(port_row["name"]):
            return port
        else:
            # If the port is not available in the new workflow, a new one must be created
            port = await Port.load(context, persistent_id, self)
            self.workflow.ports[port.name] = port
            return port

    async def load_step(self, context: StreamFlowContext, persistent_id: int):
        return await Step.load(context, persistent_id, self)

    async def load_target(self, context: StreamFlowContext, persistent_id: int):
        return await Target.load(context, persistent_id, self)

    async def load_token(self, context: StreamFlowContext, persistent_id: int):
        return self._tokens.get(persistent_id) or await Token.load(
            context, persistent_id, self
        )

    async def load_workflow(
        self,
        context: StreamFlowContext,
        persistent_id: int,
    ):
        return self.workflow
