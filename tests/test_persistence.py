import posixpath

import pytest

from tests.conftest import get_docker_deployment_config, save_load_and_test

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.config import BindingConfig
from streamflow.core.deployment import Target, LocalTarget
from streamflow.core.workflow import Job, Token, Workflow

from streamflow.workflow.port import JobPort, ConnectorPort
from streamflow.workflow.step import (
    CombinatorStep,
    DeployStep,
    ExecuteStep,
    GatherStep,
    ScheduleStep,
    ScatterStep,
    LoopCombinatorStep,
)
from streamflow.workflow.combinator import (
    CartesianProductCombinator,
    DotProductCombinator,
    LoopCombinator,
    LoopTerminationCombinator,
)
from streamflow.workflow.token import (
    JobToken,
    ListToken,
    ObjectToken,
    TerminationToken,
    IterationTerminationToken,
)


# Testing Workflow
@pytest.mark.asyncio
async def test_workflow(context: StreamFlowContext):
    """Test saving and loading Workflow from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    await save_load_and_test(workflow, context)


# Testing Port and its extension classes
@pytest.mark.asyncio
async def test_port(context: StreamFlowContext):
    """Test saving and loading Port from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    await workflow.save(context)
    port = workflow.create_port()
    await save_load_and_test(port, context)


@pytest.mark.asyncio
async def test_job_port(context: StreamFlowContext):
    """Test saving and loading JobPort from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    await workflow.save(context)
    port = workflow.create_port(JobPort)
    await save_load_and_test(port, context)


@pytest.mark.asyncio
async def test_connector_port(context: StreamFlowContext):
    """Test saving and loading ConnectorPort from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    await workflow.save(context)
    port = workflow.create_port(ConnectorPort)
    await save_load_and_test(port, context)


# Testing Step and its extension classes
@pytest.mark.asyncio
async def test_combinator_step(context: StreamFlowContext):
    """Test saving and loading CombinatorStep with CartesianProductCombinator from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    await workflow.save(context)
    name = utils.random_name()
    step = workflow.create_step(
        cls=CombinatorStep,
        name=name + "-combinator",
        combinator=CartesianProductCombinator(
            name=utils.random_name(), workflow=workflow, depth=1
        ),
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_loop_combinator_step(context: StreamFlowContext):
    """Test saving and loading LoopCombinatorStep from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    await workflow.save(context)

    name = utils.random_name()
    step = workflow.create_step(
        cls=LoopCombinatorStep,
        name=name + "-combinator",
        combinator=CartesianProductCombinator(
            name=utils.random_name(), workflow=workflow, depth=1
        ),
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_deploy_step(context: StreamFlowContext):
    """Test saving and loading DeployStep from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    connector_port = workflow.create_port(cls=ConnectorPort)
    await workflow.save(context)

    deployment_config = get_docker_deployment_config()
    step = workflow.create_step(
        cls=DeployStep,
        name=posixpath.join("__deploy__", deployment_config.name),
        deployment_config=deployment_config,
        connector_port=connector_port,
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_schedule_step(context: StreamFlowContext):
    """Test saving and loading ScheduleStep from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    await workflow.save(context)

    binding_config = BindingConfig(targets=[LocalTarget(workdir=utils.random_name())])
    schedule_step = workflow.create_step(
        cls=ScheduleStep,
        name=posixpath.join(utils.random_name() + "-injector", "__schedule__"),
        job_prefix="something",
        connector_ports={binding_config.targets[0].deployment.name: port},
        input_directory=binding_config.targets[0].workdir,
        output_directory=binding_config.targets[0].workdir,
        tmp_directory=binding_config.targets[0].workdir,
        binding_config=binding_config,
    )
    await save_load_and_test(schedule_step, context)


@pytest.mark.asyncio
async def test_execute_step(context: StreamFlowContext):
    """Test saving and loading ExecuteStep from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    await workflow.save(context)

    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=port
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_gather_step(context: StreamFlowContext):
    """Test saving and loading GatherStep from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    await workflow.save(context)

    step = workflow.create_step(
        cls=GatherStep, name=utils.random_name() + "-gather", depth=1
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_scatter_step(context: StreamFlowContext):
    """Test saving and loading ScatterStep from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    await workflow.save(context)

    step = workflow.create_step(cls=ScatterStep, name=utils.random_name() + "-scatter")
    await save_load_and_test(step, context)


# Subtest - Step param combinator
@pytest.mark.asyncio
async def test_dot_product_combinator(context: StreamFlowContext):
    """Test saving and loading CombinatorStep with DotProductCombinator from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    await workflow.save(context)

    name = utils.random_name()
    step = workflow.create_step(
        cls=CombinatorStep,
        name=name + "-combinator",
        combinator=DotProductCombinator(name=utils.random_name(), workflow=workflow),
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_loop_combinator(context: StreamFlowContext):
    """Test saving and loading CombinatorStep with LoopCombinator from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    await workflow.save(context)

    name = utils.random_name()
    step = workflow.create_step(
        cls=CombinatorStep,
        name=name + "-combinator",
        combinator=LoopCombinator(name=utils.random_name(), workflow=workflow),
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_loop_termination_combinator(context: StreamFlowContext):
    """Test saving and loading CombinatorStep with LoopTerminationCombinator from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    await workflow.save(context)

    name = utils.random_name()
    step = workflow.create_step(
        cls=CombinatorStep,
        name=name + "-combinator",
        combinator=LoopTerminationCombinator(
            name=utils.random_name(), workflow=workflow
        ),
    )
    await save_load_and_test(step, context)


# Testing the Target and its extension classes
@pytest.mark.asyncio
async def test_target(context: StreamFlowContext):
    """Test saving and loading Target from database"""
    target = Target(
        deployment=get_docker_deployment_config(),
        service="test-persistence",
        workdir=utils.random_name(),
    )
    await save_load_and_test(target, context)


@pytest.mark.asyncio
async def test_local_target(context: StreamFlowContext):
    """Test saving and loading LocalTarget from database"""
    target = LocalTarget(workdir=utils.random_name())
    await save_load_and_test(target, context)


# Testing the Token and its extension classes
@pytest.mark.asyncio
async def test_token(context: StreamFlowContext):
    """Test saving and loading Token from database"""
    token = Token(value=["test", "token"])
    await save_load_and_test(token, context)


@pytest.mark.asyncio
async def test_job_token(context: StreamFlowContext):
    """Test saving and loading JobToken from database"""
    token = JobToken(
        value=Job(
            workflow_id=0,
            name=utils.random_name(),
            inputs={"test": Token(value="jobtoken")},
            input_directory=utils.random_name(),
            output_directory=utils.random_name(),
            tmp_directory=utils.random_name(),
        ),
    )
    await save_load_and_test(token, context)


@pytest.mark.asyncio
async def test_list_token(context: StreamFlowContext):
    """Test saving and loading ListToken from database"""
    token = ListToken(value=[Token("list"), Token("test")])
    await save_load_and_test(token, context)


@pytest.mark.asyncio
async def test_object_token(context: StreamFlowContext):
    """Test saving and loading ObjectToken from database"""
    token = ObjectToken(value={"test": Token("object")})
    await save_load_and_test(token, context)


@pytest.mark.asyncio
async def test_termination_token(context: StreamFlowContext):
    """Test saving and loading IterationTerminationToken from database"""
    token = TerminationToken()
    await save_load_and_test(token, context)


@pytest.mark.asyncio
async def test_iteration_termination_token(context: StreamFlowContext):
    """Test saving and loading IterationTerminationToken from database"""
    token = IterationTerminationToken("1")
    await save_load_and_test(token, context)


# Deployment test
@pytest.mark.asyncio
async def test_deployment(context: StreamFlowContext):
    """Test saving and loading deployment configuration from database"""
    config = get_docker_deployment_config()
    await save_load_and_test(config, context)
