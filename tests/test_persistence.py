import posixpath
from collections.abc import MutableMapping
from typing import Any

import pytest
from typing_extensions import Self

from streamflow.core import utils
from streamflow.core.config import BindingConfig, Config
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import (
    DeploymentConfig,
    FilterConfig,
    LocalTarget,
    Target,
    WrapsConfig,
)
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.scheduling import Hardware, HardwareRequirement
from streamflow.core.workflow import Job, Port, Status, Token, Workflow
from streamflow.workflow.port import ConnectorPort, JobPort
from streamflow.workflow.step import (
    DeployStep,
    ExecuteStep,
    GatherStep,
    ScatterStep,
    ScheduleStep,
)
from streamflow.workflow.token import (
    IterationTerminationToken,
    JobToken,
    ListToken,
    ObjectToken,
    TerminationToken,
)
from tests.conftest import save_load_and_test
from tests.utils.deployment import get_docker_deployment_config
from tests.utils.utils import get_full_instantiation
from tests.utils.workflow import create_workflow, get_combinator_step


class DummyHardwareRequirement(HardwareRequirement):
    @classmethod
    async def _load(
        cls,
        context: StreamFlowContext,
        row: MutableMapping[str, Any],
        loading_context: DatabaseLoadingContext,
    ) -> Self:
        return DummyHardwareRequirement()

    async def _save_additional_params(
        self, context: StreamFlowContext
    ) -> MutableMapping[str, Any]:
        return {}

    def eval(self, job: Job) -> Hardware:
        return Hardware()


@pytest.mark.asyncio
async def test_workflow(context: StreamFlowContext):
    """Test saving and loading Workflow from database"""
    workflow = get_full_instantiation(
        cls_=Workflow,
        context=context,
        name=utils.random_name(),
        config={"key": "value"},
    )
    await save_load_and_test(workflow, context)


@pytest.mark.asyncio
async def test_port(context: StreamFlowContext):
    """Test saving and loading Port from database"""
    workflow = Workflow(context=context, name=utils.random_name(), config={})
    await workflow.save(context)
    port = get_full_instantiation(cls_=Port, workflow=workflow, name="my_port")
    workflow.ports[port.name] = port
    await save_load_and_test(port, context)


@pytest.mark.asyncio
async def test_job_port(context: StreamFlowContext):
    """Test saving and loading JobPort from database"""
    workflow = Workflow(context=context, name=utils.random_name(), config={})
    await workflow.save(context)
    port = get_full_instantiation(cls_=JobPort, workflow=workflow, name="my_port")
    workflow.ports[port.name] = port
    await save_load_and_test(port, context)


@pytest.mark.asyncio
async def test_connector_port(context: StreamFlowContext):
    """Test saving and loading ConnectorPort from database"""
    workflow = Workflow(context=context, name=utils.random_name(), config={})
    await workflow.save(context)
    port = get_full_instantiation(cls_=ConnectorPort, workflow=workflow, name="my_port")
    workflow.ports[port.name] = port
    await save_load_and_test(port, context)


@pytest.mark.asyncio
async def test_deploy_step(context: StreamFlowContext):
    """Test saving and loading DeployStep from database"""
    workflow = Workflow(context=context, name=utils.random_name(), config={})
    connector_port = workflow.create_port(cls=ConnectorPort)
    await workflow.save(context)

    deployment_config = get_docker_deployment_config()
    step = get_full_instantiation(
        cls_=DeployStep,
        name=posixpath.join(utils.random_name(), "__deploy__", deployment_config.name),
        deployment_config=deployment_config,
        connector_port=connector_port,
        workflow=workflow,
    )
    workflow.steps[step.name] = step
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_schedule_step(context: StreamFlowContext):
    """Test saving and loading ScheduleStep from database"""
    workflow, (job_port,) = await create_workflow(context, type_="default", num_port=1)
    binding_config = get_full_instantiation(
        BindingConfig,
        targets=[
            get_full_instantiation(cls_=LocalTarget, workdir=utils.random_name()),
            get_full_instantiation(
                cls_=Target,
                deployment=get_docker_deployment_config(),
                locations=2,
                service="my_service",
                workdir=utils.random_name(),
            ),
        ],
        filters=[
            get_full_instantiation(
                cls_=FilterConfig, config={}, name=utils.random_name(), type="shuffle"
            )
        ],
    )
    connector_ports = {
        target.deployment.name: workflow.create_port(ConnectorPort)
        for target in binding_config.targets
    }
    await workflow.save(context)

    step = get_full_instantiation(
        cls_=ScheduleStep,
        name=posixpath.join(utils.random_name(), "__schedule__"),
        workflow=workflow,
        binding_config=binding_config,
        connector_ports=connector_ports,
        job_port=job_port,
        job_prefix="something",
        hardware_requirement=DummyHardwareRequirement(),
        input_directory="/inputs",
        output_directory="/outputs",
        tmp_directory="/tmp",
    )
    workflow.steps[step.name] = step
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_execute_step(context: StreamFlowContext):
    """Test saving and loading ExecuteStep from database"""
    workflow, (job_port,) = await create_workflow(context, type_="default", num_port=1)
    await workflow.save(context)

    step = get_full_instantiation(
        cls_=ExecuteStep,
        name="/exec1",
        workflow=workflow,
        job_port=job_port,
    )
    workflow.steps[step.name] = step
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_gather_step(context: StreamFlowContext):
    """Test saving and loading GatherStep from database"""
    workflow, (port,) = await create_workflow(context, type_="default", num_port=1)
    await workflow.save(context)

    step = get_full_instantiation(
        cls_=GatherStep,
        name=f"{utils.random_name()}-gather",
        depth=2,
        size_port=port,
        workflow=workflow,
    )
    workflow.steps[step.name] = step
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_scatter_step(context: StreamFlowContext):
    """Test saving and loading ScatterStep from database"""
    workflow, (port,) = await create_workflow(context, type_="default", num_port=1)
    await workflow.save(context)

    step = get_full_instantiation(
        cls_=ScatterStep,
        name=f"{utils.random_name()}-scatter",
        size_port=port,
        workflow=workflow,
    )
    workflow.steps[step.name] = step
    await save_load_and_test(step, context)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "combinator_t",
    [
        "cartesian_product_combinator",
        "dot_combinator",
        "loop_combinator",
        "loop_termination_combinator",
    ],
)
async def test_combinator_step(context: StreamFlowContext, combinator_t: str):
    """
    Test saving and loading CombinatorStep and LoopCombinatorStep
    with appropriate Combinator classes from database
    """
    workflow = Workflow(context=context, name=utils.random_name(), config={})
    await workflow.save(context)
    step = get_combinator_step(workflow, combinator_t, inner_combinator=True)
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_target(context: StreamFlowContext):
    """Test saving and loading Target from database"""
    target = get_full_instantiation(
        cls_=Target,
        deployment=get_docker_deployment_config(),
        locations=2,
        service="test-persistence",
        workdir=utils.random_name(),
    )
    await save_load_and_test(target, context)


@pytest.mark.asyncio
async def test_local_target(context: StreamFlowContext):
    """Test saving and loading LocalTarget from database"""
    target = get_full_instantiation(cls_=LocalTarget, workdir=utils.random_name())
    await save_load_and_test(target, context)


@pytest.mark.asyncio
async def test_token(context: StreamFlowContext):
    """Test saving and loading Token from database"""
    token = get_full_instantiation(
        cls_=Token, value=["test", "token"], tag="0.0", recoverable=True
    )
    await save_load_and_test(token, context)


@pytest.mark.asyncio
async def test_job_token(context: StreamFlowContext):
    """Test saving and loading JobToken from database"""
    token = get_full_instantiation(
        cls_=JobToken,
        value=Job(
            workflow_id=0,
            name=utils.random_name(),
            inputs={"test": Token(value="job_token")},
            input_directory=utils.random_name(),
            output_directory=utils.random_name(),
            tmp_directory=utils.random_name(),
        ),
        tag="0.0",
        recoverable=True,
    )
    await save_load_and_test(token, context)


@pytest.mark.asyncio
async def test_list_token(context: StreamFlowContext):
    """Test saving and loading ListToken from database"""
    # The `ListToken` does not accept `recoverable=True` and set its internal attribute to `False`.
    # However, when using `get_full_instantiation`, passing the `recoverable` value is mandatory.
    # To handle this, `None` is passed, allowing the `get_full_instantiation` function to control the value.
    token = get_full_instantiation(
        cls_=ListToken,
        value=[Token("list", recoverable=True), Token("test", recoverable=True)],
        tag="0.0",
        recoverable=None,
    )
    await save_load_and_test(token, context)


@pytest.mark.asyncio
async def test_object_token(context: StreamFlowContext):
    """Test saving and loading ObjectToken from database"""
    # The `ObjectToken` does not accept `recoverable=True` and set its internal attribute to `False`.
    # However, when using `get_full_instantiation`, passing the `recoverable` value is mandatory.
    # To handle this, `None` is passed, allowing the `get_full_instantiation` function to control the value.
    token = get_full_instantiation(
        cls_=ObjectToken,
        value={"test": Token("object", recoverable=True)},
        tag="0.0",
        recoverable=None,
    )
    await save_load_and_test(token, context)


@pytest.mark.asyncio
async def test_termination_token(context: StreamFlowContext):
    """Test saving and loading TerminationToken from database"""
    token = get_full_instantiation(cls_=TerminationToken, value=Status.FAILED)
    await save_load_and_test(token, context)


@pytest.mark.asyncio
async def test_iteration_termination_token(context: StreamFlowContext):
    """Test saving and loading IterationTerminationToken from database"""
    token = get_full_instantiation(cls_=IterationTerminationToken, tag="0.0")
    await save_load_and_test(token, context)


@pytest.mark.asyncio
async def test_filter_config(context: StreamFlowContext):
    """Test saving and loading filter configuration from database"""
    config = get_full_instantiation(
        cls_=FilterConfig,
        config={"key": ["1"]},
        name=utils.random_name(),
        type="shuffle",
    )
    await save_load_and_test(config, context)


@pytest.mark.asyncio
async def test_deployment(context: StreamFlowContext):
    """Test saving and loading deployment configuration from database"""
    config = get_full_instantiation(
        cls_=DeploymentConfig,
        name="test",
        type="ssh",
        config={"nodes": ["localhost"]},
        external=True,
        lazy=False,
        scheduling_policy=Config(name="test", type="default", config={"key": 2}),
        workdir="/tmp",
        wraps=WrapsConfig(deployment="vm", service="test1"),
    )
    await save_load_and_test(config, context)
