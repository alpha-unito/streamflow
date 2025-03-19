from __future__ import annotations

import asyncio
import json
import os
import posixpath
from collections.abc import MutableMapping, MutableSequence
from typing import TYPE_CHECKING, Any, cast

from streamflow.core import utils
from streamflow.core.command import Command, CommandOutput, CommandOutputProcessor
from streamflow.core.config import BindingConfig
from streamflow.core.data import DataLocation, DataType
from streamflow.core.deployment import (
    Connector,
    DeploymentConfig,
    ExecutionLocation,
    Target,
)
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.recovery import RetryRequest
from streamflow.core.scheduling import HardwareRequirement
from streamflow.core.utils import flatten_list, get_job_tag, get_tag
from streamflow.core.workflow import Job, Port, Status, Step, Token, Workflow
from streamflow.cwl.hardware import CWLHardwareRequirement
from streamflow.cwl.step import CWLScheduleStep
from streamflow.cwl.utils import get_token_class, search_in_parent_locations
from streamflow.cwl.workflow import CWLWorkflow
from streamflow.data.remotepath import StreamFlowPath
from streamflow.deployment.utils import get_path_processor
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.recovery.failure_manager import DefaultFailureManager
from streamflow.workflow.combinator import (
    CartesianProductCombinator,
    DotProductCombinator,
    LoopCombinator,
    LoopTerminationCombinator,
)
from streamflow.workflow.port import ConnectorPort, JobPort
from streamflow.workflow.step import (
    CombinatorStep,
    DeployStep,
    ExecuteStep,
    InputInjectorStep,
    LoopCombinatorStep,
    ScheduleStep,
    TransferStep,
)
from streamflow.workflow.token import FileToken, ListToken, ObjectToken
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
        if get_token_class(token_value) in ["File", "Directory"]:
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


class ForwardFirstInputProcessor(CommandOutputProcessor):
    async def process(
        self,
        job: Job,
        command_output: CommandOutput,
        connector: Connector | None = None,
    ) -> Token | None:
        token = next(iter(job.inputs.values()))
        if isinstance(token, FileToken):
            return token.update(token.value)
        else:
            return await build_token(job, token.value, self.workflow.context)


class InjectorFailureCommand(Command):
    SOFT_ERROR = "soft_error"
    FAIL_STOP = "fail_stop"
    INJECT_TOKEN = "inject_error"

    def __init__(
        self,
        step: Step,
        inject_failure: MutableMapping[str, int] | None = None,
        failure_t: str | None = None,
    ):
        super().__init__(step)
        self.inject_failure: MutableMapping[str, int] = inject_failure or {}
        self.failure_t: int | None = failure_t

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
            max_failures := self.inject_failure.get(tag, None)
        ) and num_executions < max_failures:
            if self.failure_t == InjectorFailureCommand.INJECT_TOKEN:
                request = cast(
                    DefaultFailureManager, context.failure_manager
                ).retry_requests[job.name] = RetryRequest()
                request.output_tokens = {
                    k: t.update(t.value) for k, t in job.inputs.items()
                }
            elif self.failure_t == InjectorFailureCommand.FAIL_STOP:
                for t in job.inputs.values():
                    await _invalidate_token(context, job, t)
            cmd_out = CommandOutput("Injected failure", Status.FAILED)
        else:
            cmd_out = CommandOutput("Injected success", Status.COMPLETED)
        await context.database.update_execution(
            await context.database.add_execution(self.step.persistent_id, tag, "true"),
            {
                "status": cmd_out.status,
            },
        )
        return cmd_out

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "inject_failure": json.dumps(self.inject_failure)
        }

    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
        step: Step,
    ) -> InjectorFailureCommand:
        return cls(step=step, inject_failure=json.loads(row["inject_failure"]))


class InjectorFailureTransferStep(TransferStep):
    def __init__(
        self,
        name: str,
        workflow: Workflow,
        job_port: JobPort,
        num_failures: int = 0,
        failure_t: str | None = None,
    ):
        super().__init__(name, workflow, job_port)
        self.num_failures: int = num_failures
        self.failure_t: str | None = failure_t

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
            num_failures=params["num_failures"],
        )

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return cast(dict[str, Any], await super()._save_additional_params(context)) | {
            "num_failures": self.num_failures
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
        if (
            len([w.steps[self.name] for w in workflows if self.name in w.steps]) - 1
            < self.num_failures
        ):
            if self.failure_t == InjectorFailureCommand.FAIL_STOP:
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
        self.sub_workflows: MutableMapping[str, Workflow] = {}
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
        workflow: Workflow,
        step_name: str,
        deployment_names: MutableSequence[str],
        input_ports: MutableMapping[str, Port],
        outputs: MutableSequence[str],
        binding_config: BindingConfig | None = None,
        transfer_failures: int = 0,
        failure_t: str | None = None,
    ) -> ExecuteStep:
        schedule_step = self.get_schedule_step(
            binding_config, deployment_names, step_name, workflow
        )
        execute_step = workflow.create_step(
            ExecuteStep, name=step_name, job_port=schedule_step.get_output_port()
        )
        for key, port in input_ports.items():
            schedule_step.add_input_port(key, port)
            transfer_step = workflow.create_step(
                cls=InjectorFailureTransferStep,
                name=posixpath.join(step_name, "__transfer__", key),
                job_port=schedule_step.get_output_port(),
                num_failures=transfer_failures,
                failure_t=failure_t,
            )
            transfer_step.add_input_port(key, port)
            transfer_step.add_output_port(key, workflow.create_port())
            execute_step.add_input_port(key, transfer_step.get_output_port(key))
        for output in outputs:
            execute_step.add_output_port(
                output,
                workflow.create_port(),
                ForwardFirstInputProcessor(output, workflow),
            )
        return execute_step
