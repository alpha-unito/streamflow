from collections.abc import MutableMapping

from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import DeploymentConfig, FilterConfig, Target
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.workflow import Port, Status, Step, Token, Workflow


class DefaultDatabaseLoadingContext(DatabaseLoadingContext):
    def __init__(self) -> None:
        super().__init__()
        self._deployment_configs: MutableMapping[int, DeploymentConfig] = {}
        self._ports: MutableMapping[int, Port] = {}
        self._steps: MutableMapping[int, Step] = {}
        self._targets: MutableMapping[int, Target] = {}
        self._filter_configs: MutableMapping[int, FilterConfig] = {}
        self._tokens: MutableMapping[int, Token] = {}
        self._workflows: MutableMapping[int, Workflow] = {}

    def add_deployment(self, persistent_id: int, deployment: DeploymentConfig) -> None:
        deployment.persistent_id = persistent_id
        self._deployment_configs[persistent_id] = deployment

    def add_filter(self, persistent_id: int, filter_config: FilterConfig) -> None:
        filter_config.persistent_id = persistent_id
        self._filter_configs[persistent_id] = filter_config

    def add_port(self, persistent_id: int, port: Port) -> None:
        port.persistent_id = persistent_id
        self._ports[persistent_id] = port

    def add_step(self, persistent_id: int, step: Step) -> None:
        step.persistent_id = persistent_id
        self._steps[persistent_id] = step

    def add_target(self, persistent_id: int, target: Target) -> None:
        target.persistent_id = persistent_id
        self._targets[persistent_id] = target

    def add_token(self, persistent_id: int, token: Token) -> None:
        token.persistent_id = persistent_id
        self._tokens[persistent_id] = token

    def add_workflow(self, persistent_id: int, workflow: Workflow) -> None:
        workflow.persistent_id = persistent_id
        self._workflows[persistent_id] = workflow

    async def load_deployment(
        self, context: StreamFlowContext, persistent_id: int
    ) -> DeploymentConfig:
        return self._deployment_configs.get(
            persistent_id
        ) or await DeploymentConfig.load(context, persistent_id, self)

    async def load_filter(
        self, context: StreamFlowContext, persistent_id: int
    ) -> FilterConfig:
        return self._filter_configs.get(persistent_id) or await FilterConfig.load(
            context, persistent_id, self
        )

    async def load_port(self, context: StreamFlowContext, persistent_id: int) -> Port:
        return self._ports.get(persistent_id) or await Port.load(
            context, persistent_id, self
        )

    async def load_step(self, context: StreamFlowContext, persistent_id: int) -> Step:
        return self._steps.get(persistent_id) or await Step.load(
            context, persistent_id, self
        )

    async def load_target(
        self, context: StreamFlowContext, persistent_id: int
    ) -> Target:
        return self._targets.get(persistent_id) or await Target.load(
            context, persistent_id, self
        )

    async def load_token(self, context: StreamFlowContext, persistent_id: int) -> Token:
        return self._tokens.get(persistent_id) or await Token.load(
            context, persistent_id, self
        )

    async def load_workflow(
        self, context: StreamFlowContext, persistent_id: int
    ) -> Workflow:
        return self._workflows.get(persistent_id) or await Workflow.load(
            context, persistent_id, self
        )


class WorkflowBuilder(DefaultDatabaseLoadingContext):
    def __init__(self, deep_copy: bool = True) -> None:
        super().__init__()
        self.deep_copy: bool = deep_copy
        self.workflow: Workflow | None = None

    def add_port(self, persistent_id: int, port: Port) -> None:
        self._ports[persistent_id] = port

    def add_step(self, persistent_id: int, step: Step) -> None:
        self._steps[persistent_id] = step

    def add_workflow(self, persistent_id: int, workflow: Workflow) -> None:
        self._workflows[persistent_id] = workflow
        if self.deep_copy:
            # Deep copy
            # The `persistent_id` value will be removed in the `load_workflow` method
            workflow.persistent_id = persistent_id
            self.workflow = workflow

    async def load_port(self, context: StreamFlowContext, persistent_id: int) -> Port:
        if persistent_id in self._ports.keys():
            return self._ports[persistent_id]
        else:
            port_row = await context.database.get_port(persistent_id)
            if (port := self.workflow.ports.get(port_row["name"])) is None:
                # If the port is not available in the new workflow, a new one must be created
                self.add_workflow(port_row["workflow"], self.workflow)
                port = await Port.load(context, persistent_id, self)
                self.workflow.ports[port.name] = port
            return port

    async def load_step(self, context: StreamFlowContext, persistent_id: int) -> Step:
        if persistent_id in self._steps.keys():
            return self._steps[persistent_id]
        else:
            step_row = await context.database.get_step(persistent_id)
            if (step := self.workflow.steps.get(step_row["name"])) is None:
                # If the step is not available in the new workflow, a new one must be created
                self.add_workflow(step_row["workflow"], self.workflow)
                step = await Step.load(context, persistent_id, self)

                # Restore initial step state
                step.status = Status.WAITING
                step.terminated = False

                self.workflow.steps[step.name] = step
            return step

    async def load_workflow(
        self, context: StreamFlowContext, persistent_id: int
    ) -> Workflow:
        if persistent_id not in self._workflows.keys():
            if self.deep_copy:
                # Deep copy
                self.workflow = await Workflow.load(context, persistent_id, self)
                self.workflow.persistent_id = None
            else:
                # Copy only workflow instance without steps and ports
                self.workflow = await Workflow.load(context, persistent_id, self)
        return self.workflow
