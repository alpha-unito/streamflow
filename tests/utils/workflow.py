from __future__ import annotations

import asyncio
import json
import os
import posixpath
from collections.abc import MutableMapping, MutableSequence, MutableSet
from typing import TYPE_CHECKING, Any, cast

from streamflow.core import utils
from streamflow.core.config import BindingConfig
from streamflow.core.data import DataLocation, DataType
from streamflow.core.deployment import (
    Connector,
    DeploymentConfig,
    ExecutionLocation,
    Target,
)
from streamflow.core.exception import (
    FailureHandlingException,
    WorkflowDefinitionException,
    WorkflowExecutionException,
)
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.scheduling import HardwareRequirement
from streamflow.core.utils import flatten_list, get_entity_ids, get_job_tag, get_tag
from streamflow.core.workflow import (
    Command,
    CommandOutput,
    Job,
    Port,
    Status,
    Step,
    Token,
    Workflow,
)
from streamflow.cwl.hardware import CWLHardwareRequirement
from streamflow.cwl.step import CWLScheduleStep
from streamflow.cwl.transformer import ForwardTransformer
from streamflow.cwl.utils import get_token_class, search_in_parent_locations
from streamflow.cwl.workflow import CWLWorkflow
from streamflow.data.remotepath import StreamFlowPath
from streamflow.deployment.utils import get_path_processor
from streamflow.log_handler import logger
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.workflow.combinator import (
    CartesianProductCombinator,
    DotProductCombinator,
    LoopCombinator,
    LoopTerminationCombinator,
)
from streamflow.workflow.port import ConnectorPort, JobPort
from streamflow.workflow.step import (
    CombinatorStep,
    ConditionalStep,
    DefaultCommandOutputProcessor,
    DeployStep,
    ExecuteStep,
    InputInjectorStep,
    LoopCombinatorStep,
    LoopOutputStep,
    ScheduleStep,
    TransferStep,
)
from streamflow.workflow.token import (
    FileToken,
    IterationTerminationToken,
    ListToken,
    ObjectToken,
)
from tests.utils.deployment import get_docker_deployment_config

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext

CWL_VERSION = "v1.2"


async def _invalidate_token(context: StreamFlowContext, job: Job, token: Token) -> None:
    if isinstance(token, FileToken):
        for loc in context.scheduler.get_locations(job.name):
            for path in await token.get_paths(context):
                context.data_manager.invalidate_location(loc, path)
    elif isinstance(token, ListToken):
        for t in token.value:
            await _invalidate_token(context, job, t)
    elif isinstance(token, ObjectToken):
        for t in token.value.value():
            await _invalidate_token(context, job, t)
    elif isinstance(token.value, Token):
        await _invalidate_token(context, job, token.value)


async def _register_path(
    context: StreamFlowContext,
    connector: Connector,
    location: ExecutionLocation,
    path: str,
    relpath: str,
    data_type: DataType = DataType.PRIMARY,
) -> DataLocation | None:
    path = StreamFlowPath(path, context=context, location=location)
    if real_path := await path.resolve():
        if real_path != path:
            if data_locations := context.data_manager.get_data_locations(
                path=str(real_path), deployment=connector.deployment_name
            ):
                data_location = next(iter(data_locations))
            else:
                base_path = StreamFlowPath(
                    str(path).removesuffix(str(relpath)),
                    context=context,
                    location=location,
                )
                if real_path.is_relative_to(base_path):
                    data_location = context.data_manager.register_path(
                        location=location,
                        path=str(real_path),
                        relpath=str(real_path.relative_to(base_path)),
                    )
                elif data_locations := await search_in_parent_locations(
                    context=context,
                    connector=connector,
                    path=str(real_path),
                    relpath=real_path.name,
                ):
                    data_location = data_locations[0]
                else:
                    return None
            link_location = context.data_manager.register_path(
                location=location,
                path=str(path),
                relpath=relpath,
                data_type=DataType.SYMBOLIC_LINK,
            )
            context.data_manager.register_relation(data_location, link_location)
            return data_location
        else:
            return context.data_manager.register_path(
                location=location, path=str(path), relpath=relpath, data_type=data_type
            )
    return None


def create_deploy_step(
    workflow: Workflow, deployment_config: DeploymentConfig | None = None
) -> DeployStep:
    connector_port = workflow.create_port(cls=ConnectorPort)
    if deployment_config is None:
        deployment_config = get_docker_deployment_config()
    return workflow.create_step(
        cls=DeployStep,
        name=posixpath.join("__deploy__", deployment_config.name),
        deployment_config=deployment_config,
        connector_port=connector_port,
    )


def create_schedule_step(
    workflow: Workflow,
    deploy_steps: MutableSequence[DeployStep],
    binding_config: BindingConfig = None,
    hardware_requirement: HardwareRequirement = None,
    name_prefix: str | None = None,
) -> ScheduleStep:
    # It is necessary to pass in the correct order biding_config.targets and deploy_steps for the mapping
    if not binding_config:
        binding_config = BindingConfig(
            targets=[
                Target(
                    deployment=deploy_step.deployment_config,
                )
                for deploy_step in deploy_steps
            ]
        )
    name_prefix = name_prefix or utils.random_name()
    return workflow.create_step(
        cls=(
            CWLScheduleStep
            if isinstance(hardware_requirement, CWLHardwareRequirement)
            else ScheduleStep
        ),
        name=posixpath.join(name_prefix, "__schedule__"),
        job_prefix=name_prefix,
        connector_ports={
            target.deployment.name: deploy_step.get_output_port()
            for target, deploy_step in zip(binding_config.targets, deploy_steps)
        },
        binding_config=binding_config,
        hardware_requirement=hardware_requirement,
    )


async def create_workflow(
    context: StreamFlowContext,
    num_port: int = 2,
    type_: str = "cwl",
    save: bool = True,
) -> tuple[Workflow, tuple[Port, ...]]:
    if type_ == "cwl":
        workflow = CWLWorkflow(
            context=context,
            name=utils.random_name(),
            config={},
            cwl_version=CWL_VERSION,
        )
    else:
        workflow = Workflow(context=context, name=utils.random_name(), config={})
    ports = []
    for _ in range(num_port):
        ports.append(workflow.create_port())
    if save:
        await workflow.save(context)
    return workflow, tuple(ports)


def get_cartesian_product_combinator(
    workflow: Workflow, name: str | None = None
) -> CartesianProductCombinator:
    return CartesianProductCombinator(
        name=name or utils.random_name(), workflow=workflow
    )


def get_combinator_step(workflow: Workflow, combinator_type: str) -> CombinatorStep:
    combinator_step_cls = CombinatorStep
    name = utils.random_name()
    if combinator_type == "cartesian_product_combinator":
        combinator = get_cartesian_product_combinator(workflow, name)
    elif combinator_type == "dot_combinator":
        combinator = get_dot_combinator(workflow, name)
    elif combinator_type == "loop_combinator":
        combinator_step_cls = LoopCombinatorStep
        combinator = get_loop_combinator(workflow, name)
    elif combinator_type == "loop_termination_combinator":
        combinator = get_loop_terminator_combinator(workflow, name)
    elif combinator_type == "nested_crossproduct":
        combinator = get_nested_crossproduct(workflow, name)
    else:
        raise ValueError(
            f"Invalid input combinator type: {combinator_type} is not supported"
        )
    return workflow.create_step(
        cls=combinator_step_cls,
        name=name + "-combinator",
        combinator=combinator,
    )


def get_dot_combinator(
    workflow: Workflow, name: str | None = None
) -> DotProductCombinator:
    return DotProductCombinator(name=name or utils.random_name(), workflow=workflow)


def get_loop_combinator(workflow: Workflow, name: str | None = None) -> LoopCombinator:
    return LoopCombinator(name=name or utils.random_name(), workflow=workflow)


def get_loop_terminator_combinator(
    workflow: Workflow, name: str | None = None
) -> LoopTerminationCombinator:
    c = LoopTerminationCombinator(name=name or utils.random_name(), workflow=workflow)
    c.add_output_item("test1")
    c.add_output_item("test2")
    return c


def get_nested_crossproduct(
    workflow: Workflow, name: str | None = None
) -> DotProductCombinator:
    combinator = DotProductCombinator(
        name=name or utils.random_name(), workflow=workflow
    )
    c1 = CartesianProductCombinator(name=name or utils.random_name(), workflow=workflow)
    c1.add_item("ext")
    c1.add_item("inn")
    items = c1.get_items(False)
    combinator.add_combinator(c1, items)
    return combinator


def random_job_name(step_name: str | None = None):
    step_name = step_name or utils.random_name()
    return os.path.join(posixpath.sep, step_name, "0.0")


async def build_token(job: Job, token_value: Any, context: StreamFlowContext) -> Token:
    if isinstance(token_value, MutableSequence):
        return ListToken(
            tag=get_tag(job.inputs.values()),
            value=[await build_token(job, v, context) for v in token_value],
        )
    elif isinstance(token_value, MutableMapping):
        if get_token_class(token_value).lower() in ["file", "directory"]:
            connector = context.scheduler.get_connector(job.name)
            locations = context.scheduler.get_locations(job.name)
            await _register_path(
                context,
                connector,
                next(iter(locations)),
                token_value["path"],
                token_value["path"],
            )
            return BaseFileToken(
                tag=get_tag(job.inputs.values()), value=token_value["path"]
            )
        else:
            return ObjectToken(
                tag=get_tag(job.inputs.values()),
                value={
                    k: await build_token(job, v, context)
                    for k, v in token_value.items()
                },
            )
    elif isinstance(token_value, FileToken):
        return token_value.update(token_value.value)
    elif isinstance(token_value, Token):
        return token_value.update(token_value.value)
    else:
        return Token(tag=get_tag(job.inputs.values()), value=token_value)


class BaseFileToken(FileToken):
    async def get_paths(self, context: StreamFlowContext) -> MutableSequence[str]:
        return [self.value]


class BaseInputInjectorStep(InputInjectorStep):
    def __init__(self, name: str, workflow: Workflow, job_port: JobPort):
        super().__init__(name, workflow, job_port)

    async def process_input(self, job: Job, token_value: Any) -> Token:
        return await build_token(
            job=job,
            token_value=token_value,
            context=self.workflow.context,
        )


class BaseLoopConditionalStep(ConditionalStep):
    def __init__(self, name: str, workflow: Workflow, condition: str):
        super().__init__(name, workflow)
        self.condition: str = condition
        self.skip_ports: MutableMapping[str, str] = {}

    async def _eval(self, inputs: MutableMapping[str, Token]):
        return eval(self.condition)(inputs)

    async def _on_true(self, inputs: MutableMapping[str, Token]) -> None:
        # Next iteration: propagate outputs to the loop
        for port_name, port in self.get_output_ports().items():
            port.put(
                await self._persist_token(
                    token=inputs[port_name].update(inputs[port_name].value),
                    port=port,
                    input_token_ids=get_entity_ids(inputs.values()),
                )
            )

    async def _on_false(self, inputs: MutableMapping[str, Token]) -> None:
        # Loop termination: propagate outputs outside the loop
        for port in self.get_skip_ports().values():
            port.put(IterationTerminationToken(tag=get_tag(inputs.values())))

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "skip_ports": {
                k: p.persistent_id for k, p in self.get_skip_ports().items()
            },
            "condition": self.condition,
        }

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> BaseLoopConditionalStep:
        params = json.loads(row["params"])
        step = cls(
            name=row["name"],
            workflow=cast(
                CWLWorkflow,
                await loading_context.load_workflow(context, row["workflow"]),
            ),
            condition=params["condition"],
        )
        for k, port in zip(
            params["skip_ports"].keys(),
            await asyncio.gather(
                *(
                    asyncio.create_task(loading_context.load_port(context, port_id))
                    for port_id in params["skip_ports"].values()
                )
            ),
        ):
            step.add_skip_port(k, port)
        return step

    def add_skip_port(self, name: str, port: Port) -> None:
        if port.name not in self.workflow.ports:
            self.workflow.ports[port.name] = port
        self.skip_ports[name] = port.name

    def get_skip_ports(self) -> MutableMapping[str, Port]:
        return {k: self.workflow.ports[v] for k, v in self.skip_ports.items()}


class BaseLoopOutputLastStep(LoopOutputStep):
    async def _process_output(self, tag: str) -> Token:
        return Token(
            tag=tag,
            value=sorted(
                self.token_map.get(tag, [Token(value=None)]),
                key=lambda t: int(t.tag.split(".")[-1]),
            )[-1],
        )


class EvalCommandOutputProcessor(DefaultCommandOutputProcessor):
    def __init__(
        self,
        name: str,
        workflow: Workflow,
        value_type: str,
        target: Target | None = None,
    ):
        super().__init__(name, workflow, target)
        self.value_type: str = value_type

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> EvalCommandOutputProcessor:
        return cls(
            name=row["name"],
            workflow=await loading_context.load_workflow(context, row["workflow"]),
            value_type=row["value_type"],
            target=(
                (await loading_context.load_target(context, row["workflow"]))
                if row["target"]
                else None
            ),
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        if self.target:
            await self.target.save(context)
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "value_type": self.value_type,
        }

    async def process(
        self,
        job: Job,
        command_output: CommandOutput,
        connector: Connector | None = None,
    ) -> Token | None:
        context = self.workflow.context
        value = command_output.value
        if self.value_type == "file":
            locations = context.scheduler.get_locations(job.name)
            await _register_path(
                context, connector, next(iter(locations)), value, value
            )
            return BaseFileToken(tag=get_tag(job.inputs.values()), value=value)
        elif self.value_type == "list":
            return ListToken(
                tag=get_tag(job.inputs.values()),
                value=[await build_token(job, v, context) for v in value],
            )
        elif self.value_type == "dict":
            return ObjectToken(
                tag=get_tag(job.inputs.values()),
                value={k: await build_token(job, v, context) for k, v in value.items()},
            )
        else:
            return Token(tag=get_tag(job.inputs.values()), value=value)


class InjectorFailureCommand(Command):
    SOFT_ERROR = "soft_error"
    FAIL_STOP = "fail_stop"
    INJECT_TOKEN = "inject_error"

    def __init__(
        self,
        step: Step,
        command: str,
        failure_tags: MutableMapping[str, int] | None = None,
        failure_type: str | None = None,
    ):
        super().__init__(step)
        self.command: str = command
        self.failure_tags: MutableMapping[str, int] = failure_tags or {}
        self.failure_type: str | None = failure_type
        if self.failure_tags and self.failure_type is None:
            raise WorkflowDefinitionException(
                f"Failure type does not defined. "
                f"Impossible to inject failures to the tags: {list(self.failure_tags.keys())}"
            )

    async def execute(self, job: Job) -> CommandOutput:
        # Counts all the execution of the step in the different workflows
        context = self.step.workflow.context
        loading_context = DefaultDatabaseLoadingContext()
        workflows = await asyncio.gather(
            *(
                asyncio.create_task(
                    Workflow.load(
                        context=context,
                        persistent_id=w["id"],
                        loading_context=loading_context,
                    )
                )
                for w in await context.database.get_workflows_by_name(
                    self.step.workflow.name
                )
            )
        )
        steps = [
            w.steps[self.step.name] for w in workflows if self.step.name in w.steps
        ]
        executions = await asyncio.gather(
            *(
                asyncio.create_task(
                    context.database.get_executions_by_step(s.persistent_id)
                )
                for s in steps
            )
        )
        num_executions = len(flatten_list(executions))

        tag = get_job_tag(job.name)
        if (
            max_failures := self.failure_tags.get(tag, None)
        ) is not None and num_executions < max_failures:
            if self.failure_type == InjectorFailureCommand.INJECT_TOKEN:
                context.failure_manager.get_request(job.name).output_tokens = {
                    k: t.update(t.value, recoverable=True)
                    for k, t in job.inputs.items()
                }
            elif self.failure_type == InjectorFailureCommand.FAIL_STOP:
                for t in job.inputs.values():
                    await _invalidate_token(context, job, t)
            cmd_out = CommandOutput("Injected failure", Status.FAILED)
        else:
            try:
                val = eval(self.command)(job.inputs)
                cmd_out = CommandOutput(val, Status.COMPLETED)
            except Exception as err:
                logger.error(f"Failed command evaluation: {err}")
                raise FailureHandlingException(err)
        await context.database.update_execution(
            await context.database.add_execution(
                self.step.persistent_id, tag, self.command
            ),
            {
                "status": cmd_out.status,
            },
        )
        return cmd_out

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "command": self.command,
            "failure_tags": json.dumps(self.failure_tags),
            "failure_type": self.failure_type,
        }

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
        step: Step,
    ) -> InjectorFailureCommand:
        return cls(
            step=step,
            command=row["command"],
            failure_tags=json.loads(row["failure_tags"]),
            failure_type=row["failure_type"],
        )


class InjectorFailureTransferStep(TransferStep):
    def __init__(
        self,
        name: str,
        workflow: Workflow,
        job_port: JobPort,
        failure_tags: MutableMapping[str, int] | None = None,
        failure_type: str | None = None,
    ):
        super().__init__(name, workflow, job_port)
        self.failure_tags: MutableMapping[str, int] = failure_tags or {}
        self.failure_type: str | None = failure_type
        if self.failure_tags and self.failure_type is None:
            raise WorkflowDefinitionException(
                f"Failure type does not defined. "
                f"Impossible to inject failures to the tags: {list(self.failure_tags.keys())}"
            )

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> InjectorFailureTransferStep:
        params = json.loads(row["params"])
        return cls(
            name=row["name"],
            workflow=await loading_context.load_workflow(context, row["workflow"]),
            job_port=cast(
                JobPort, await loading_context.load_port(context, params["job_port"])
            ),
            failure_tags=json.loads(params["failure_tags"]),
            failure_type=params["failure_type"],
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "failure_tags": json.dumps(self.failure_tags),
            "failure_type": self.failure_type,
        }

    async def _transfer_path(self, job: Job, path: str) -> str:
        dst_connector = self.workflow.context.scheduler.get_connector(job.name)
        dst_path_processor = get_path_processor(dst_connector)
        dst_locations = self.workflow.context.scheduler.get_locations(job.name)
        source_location = await self.workflow.context.data_manager.get_source_location(
            path=path, dst_deployment=dst_connector.deployment_name
        )
        dst_path = dst_path_processor.join(job.input_directory, source_location.relpath)
        await self.workflow.context.data_manager.transfer_data(
            src_location=source_location.location,
            src_path=source_location.path,
            dst_locations=dst_locations,
            dst_path=dst_path,
        )
        return dst_path

    async def transfer(self, job: Job, token: Token) -> Token:
        # Counts the number of rollback of the workflow
        loading_context = DefaultDatabaseLoadingContext()
        workflows = await asyncio.gather(
            *(
                asyncio.create_task(
                    Workflow.load(
                        context=self.workflow.context,
                        persistent_id=w["id"],
                        loading_context=loading_context,
                    )
                )
                for w in await self.workflow.context.database.get_workflows_by_name(
                    self.workflow.name
                )
            )
        )
        if len(
            [w.steps[self.name] for w in workflows if self.name in w.steps]
        ) - 1 < self.failure_tags.get(get_tag(job.inputs.values()), 0):
            if self.failure_type == InjectorFailureCommand.FAIL_STOP:
                for t in job.inputs.values():
                    await _invalidate_token(self.workflow.context, job, t)
            raise WorkflowExecutionException(f"Injected error into {self.name} step")
        # Execute the transfer
        if isinstance(token, ListToken):
            return token.update(
                await asyncio.gather(
                    *(asyncio.create_task(self.transfer(job, t)) for t in token.value)
                )
            )
        elif isinstance(token, ObjectToken):
            return token.update(
                {
                    k: v
                    for k, v in zip(
                        token.value.keys(),
                        await asyncio.gather(
                            *(
                                asyncio.create_task(self.transfer(job, t))
                                for t in token.value.values()
                            )
                        ),
                    )
                }
            )
        elif isinstance(token, FileToken):
            return token.update(await self._transfer_path(job, token.value))
        else:
            return token.update(token.value)


class RecoveryTranslator:
    def __init__(self, workflow: Workflow):
        self.deployment_configs: MutableMapping[str, DeploymentConfig] = {}
        self.workflow: Workflow = workflow

    def _get_deploy_step(self, deployment_name: str):
        step_name = posixpath.join("__deploy__", deployment_name)
        if step_name not in self.workflow.steps.keys():
            return self.workflow.create_step(
                cls=DeployStep,
                name=step_name,
                deployment_config=self.deployment_configs[deployment_name],
            )
        else:
            return self.workflow.steps[step_name]

    def get_base_injector_step(
        self,
        deployment_names: MutableSequence[str],
        port_name: str,
        step_name: str,
        workflow: Workflow,
        binding_config: BindingConfig | None = None,
    ) -> InputInjectorStep:
        step_name = f"{step_name}-injector"
        schedule_step = self.get_schedule_step(
            binding_config, deployment_names, step_name, workflow
        )
        step = workflow.create_step(
            cls=BaseInputInjectorStep,
            name=step_name,
            job_port=schedule_step.get_output_port(),
        )
        step.add_input_port(port_name, workflow.create_port())
        step.add_output_port(port_name, workflow.create_port())
        return step

    def get_execute_pipeline(
        self,
        command: str,
        deployment_names: MutableSequence[str],
        input_ports: MutableMapping[str, Port],
        outputs: MutableMapping[str, str],
        step_name: str,
        workflow: Workflow,
        binding_config: BindingConfig | None = None,
        failure_type: str | None = None,
        failure_step: str | None = None,
        failure_tags: MutableMapping[str, int] | None = None,
    ) -> ExecuteStep:
        schedule_step = self.get_schedule_step(
            binding_config, deployment_names, step_name, workflow
        )
        execute_step = workflow.create_step(
            ExecuteStep, name=step_name, job_port=schedule_step.get_output_port()
        )
        execute_step.command = InjectorFailureCommand(
            execute_step,
            command=command,
            failure_tags=failure_tags if failure_step == "execute" else None,
            failure_type=failure_type if failure_step == "execute" else None,
        )
        for key, port in input_ports.items():
            schedule_step.add_input_port(key, port)
            transfer_step = workflow.create_step(
                cls=InjectorFailureTransferStep,
                name=posixpath.join(step_name, "__transfer__", key),
                job_port=schedule_step.get_output_port(),
                failure_tags=failure_tags if failure_step == "transfer" else None,
                failure_type=failure_type if failure_step == "transfer" else None,
            )
            transfer_step.add_input_port(key, port)
            transfer_step.add_output_port(key, workflow.create_port())
            execute_step.add_input_port(key, transfer_step.get_output_port(key))
        for output, value_type in outputs.items():
            execute_step.add_output_port(
                output,
                workflow.create_port(),
                EvalCommandOutputProcessor(output, workflow, value_type),
            )
        return execute_step

    def get_input_loop(
        self,
        step_name: str,
        input_ports: MutableMapping[str, Port],
        condition_function: str,
    ) -> MutableMapping[str, Port]:
        loop_combinator = LoopCombinator(
            workflow=self.workflow, name=step_name + "-loop-combinator"
        )
        forward_ports = {}
        for port_name, port in input_ports.items():
            # Decouple loop ports through a forwarder
            loop_forwarder = self.workflow.create_step(
                cls=ForwardTransformer,
                name=os.path.join(step_name, port_name) + "-input-forward-transformer",
            )
            loop_forwarder.add_input_port(port_name, port)
            forward_ports[port_name] = self.workflow.create_port()
            loop_forwarder.add_output_port(port_name, forward_ports[port_name])
            # Add item to combinator
            loop_combinator.add_item(port_name)
        # Create a combinator step and add all inputs to it
        combinator_step = self.workflow.create_step(
            cls=LoopCombinatorStep,
            name=step_name + "-loop-combinator",
            combinator=loop_combinator,
        )
        for port_name, port in forward_ports.items():
            combinator_step.add_input_port(port_name, port)
            combinator_step.add_output_port(port_name, self.workflow.create_port())
        # Create loop conditional step
        loop_conditional_step = self.workflow.create_step(
            cls=BaseLoopConditionalStep,
            name=step_name + "-loop-when",
            condition=condition_function,
        )
        # Add inputs to conditional step
        output_ports = {}
        for port_name in input_ports:
            loop_conditional_step.add_input_port(
                port_name, combinator_step.get_output_port(port_name)
            )
            output_ports[port_name] = self.workflow.create_port()
            loop_conditional_step.add_output_port(port_name, output_ports[port_name])
        return output_ports

    def get_output_loop(
        self,
        step_name: str,
        loop_ports: MutableMapping[str, Port],
        output_ports: MutableSet[str],
    ) -> MutableMapping[str, Port]:
        """
        loop_ports are the output ports which are needed as input for the next iteration
        output_ports are the ports which produced the data which are the output of the loop
        """
        loop_conditional_step = cast(
            BaseLoopConditionalStep, self.workflow.steps[step_name + "-loop-when"]
        )
        combinator_step = self.workflow.steps[step_name + "-loop-combinator"]
        external_output_ports = {}
        internal_ports = dict(loop_ports)
        # internal_ports = { k : (p if p else loop_conditional_step.get_output_port(k)) for k, p in loop_ports.items() }
        # Create a loop termination combinator
        loop_terminator_combinator = LoopTerminationCombinator(
            workflow=self.workflow, name=step_name + "-loop-termination-combinator"
        )
        loop_terminator_step = self.workflow.create_step(
            cls=CombinatorStep,
            name=step_name + "-loop-terminator",
            combinator=loop_terminator_combinator,
        )
        for port_name, port in combinator_step.get_input_ports().items():
            loop_terminator_step.add_output_port(port_name, port)
            loop_terminator_combinator.add_output_item(port_name)
        for port_name in output_ports:
            # Create loop forwarder
            loop_forwarder = self.workflow.create_step(
                cls=ForwardTransformer,
                name=os.path.join(step_name, port_name) + "-output-forward-transformer",
            )
            loop_forwarder.add_input_port(port_name, loop_ports[port_name])
            loop_forwarder.add_output_port(port_name, self.workflow.create_port())
            internal_ports[port_name] = loop_forwarder.get_output_port(port_name)
            # Create loop output step
            loop_output_step = self.workflow.create_step(
                cls=BaseLoopOutputLastStep,
                name=os.path.join(step_name, port_name) + "-loop-output",
            )
            loop_output_step.add_input_port(port_name, loop_forwarder.get_output_port())
            loop_conditional_step.add_skip_port(
                port_name, loop_forwarder.get_output_port()
            )
            loop_output_step.add_output_port(port_name, self.workflow.create_port())
            loop_terminator_step.add_input_port(
                port_name, loop_output_step.get_output_port(port_name)
            )
            loop_terminator_combinator.add_item(port_name)
        for port_name in loop_ports:
            # Create loop output step
            loop_forwarder = self.workflow.create_step(
                cls=ForwardTransformer,
                name=os.path.join(step_name, port_name)
                + "-back-propagation-transformer",
            )
            loop_forwarder.add_input_port(port_name, internal_ports[port_name])
            loop_forwarder.add_output_port(
                port_name, combinator_step.get_input_port(port_name)
            )
        return external_output_ports

    def get_schedule_step(
        self,
        binding_config: BindingConfig,
        deployment_names: MutableSequence[str],
        step_name: str,
        workflow: Workflow,
    ) -> ScheduleStep:
        deploy_steps = {
            deployment: self._get_deploy_step(deployment)
            for deployment in deployment_names
        }
        return create_schedule_step(
            workflow=workflow,
            deploy_steps=[d for d in deploy_steps.values()],
            binding_config=binding_config,
            name_prefix=step_name,
        )
