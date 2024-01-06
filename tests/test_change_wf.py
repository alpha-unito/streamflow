from typing import Type, cast, MutableSequence

import pytest

from streamflow.core import utils
from streamflow.core.config import BindingConfig
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import LocalTarget, FilterConfig
from streamflow.core.workflow import Workflow, Port, Step
from streamflow.cwl.command import CWLCommand, CWLCommandToken
from streamflow.cwl.translator import _create_command_output_processor_base
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.workflow.combinator import LoopCombinator
from streamflow.workflow.port import ConnectorPort, JobPort
from streamflow.workflow.step import (
    CombinatorStep,
    ExecuteStep,
    Combinator,
    LoopCombinatorStep,
    GatherStep,
    ScatterStep,
)
from tests.conftest import (
    are_equals,
    object_to_dict,
)
from tests.utils.workflow import (
    create_workflow,
    create_schedule_step,
    create_deploy_step,
    get_dot_combinator,
    get_cartesian_product_combinator,
    get_loop_terminator_combinator,
    get_nested_crossproduct,
)


async def _base_step_test_process(
    workflow, step_cls, kwargs_step, context, test_are_eq=True
):
    step = workflow.create_step(cls=step_cls, **kwargs_step)
    await workflow.save(context)
    new_workflow, new_step = await _clone_step(step, workflow, context)
    _persistent_id_test(workflow, new_workflow, step, new_step)
    if test_are_eq:
        _set_val_to_attributes(step, ["persistent_id", "workflow"], None)
        _set_val_to_attributes(new_step, ["persistent_id", "workflow"], None)
        assert are_equals(step, new_step)
    return step, new_workflow, new_step


def _persistent_id_test(original_workflow, new_workflow, original_elem, new_elem):
    assert original_workflow.persistent_id
    assert new_workflow.persistent_id
    assert original_workflow.persistent_id != new_workflow.persistent_id
    if isinstance(original_elem, Step):
        assert new_elem.name in new_workflow.steps.keys()
    if isinstance(original_elem, Port):
        assert new_elem.name in new_workflow.ports.keys()
    assert original_elem.persistent_id != new_elem.persistent_id
    assert new_elem.workflow.persistent_id == new_workflow.persistent_id


async def _general_test_port(context: StreamFlowContext, cls_port: Type[Port]):
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port(cls_port)
    await workflow.save(context)
    assert workflow.persistent_id
    assert port.persistent_id

    loading_context = DefaultDatabaseLoadingContext()
    new_workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    new_port = await Port.load(
        context, port.persistent_id, loading_context, new_workflow
    )
    new_workflow.add_port(new_port)
    await new_workflow.save(context)
    _persistent_id_test(workflow, new_workflow, port, new_port)
    port.persistent_id = None
    new_port.persistent_id = None
    port.workflow = None
    new_port.workflow = None
    assert are_equals(port, new_port)


def _set_val_to_attributes(elem, str_attributes: MutableSequence[str], val):
    attrs = object_to_dict(elem)
    for attr in str_attributes:
        if attr in attrs.keys():
            setattr(elem, attr, val)


def _workflow_in_combinator_test(original_combinator, new_combinator):
    assert (
        original_combinator.workflow.persistent_id
        != new_combinator.workflow.persistent_id
    )
    for original_inner, new_inner in zip(
        original_combinator.combinators.values(), new_combinator.combinators.values()
    ):
        _workflow_in_combinator_test(original_inner, new_inner)


def _set_workflow_in_combinator(combinator, workflow):
    combinator.workflow = workflow
    for c in combinator.combinators.values():
        _set_workflow_in_combinator(c, workflow)


async def _clone_step(step, workflow, context):
    new_workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    loading_context = DefaultDatabaseLoadingContext()
    new_step = await Step.load(
        context, step.persistent_id, loading_context, new_workflow
    )
    new_workflow.add_step(new_step)

    # ports are not loaded in new_workflow. It is necessary to do it manually
    for port in workflow.ports.values():
        new_workflow.add_port(
            await Port.load(context, port.persistent_id, loading_context, new_workflow)
        )
    await new_workflow.save(context)
    return new_workflow, new_step


@pytest.mark.asyncio
async def test_port(context: StreamFlowContext):
    """Test saving Port on database and re-load it in a new Workflow"""
    await _general_test_port(context, Port)


@pytest.mark.asyncio
async def test_job_port(context: StreamFlowContext):
    """Test saving JobPort on database and re-load it in a new Workflow"""
    await _general_test_port(context, JobPort)


@pytest.mark.asyncio
async def test_connection_port(context: StreamFlowContext):
    """Test saving ConnectorPort on database and re-load it in a new Workflow"""
    await _general_test_port(context, ConnectorPort)


@pytest.mark.asyncio
async def test_execute_step(context: StreamFlowContext):
    """Test saving ExecuteStep on database and re-load it in a new Workflow"""
    workflow, (job_port, in_port, out_port) = await create_workflow(context, num_port=3)

    in_port_name = "in-1"
    out_port_name = "out-1"
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=cast(JobPort, job_port)
    )
    step.command = CWLCommand(
        step=step,
        base_command=["echo"],
        command_tokens=[CWLCommandToken(name=in_port_name, value=None)],
    )
    step.add_output_port(
        out_port_name,
        out_port,
        _create_command_output_processor_base(
            out_port.name,
            workflow,
            None,
            "string",
            {},
            {"hints": {}, "requirements": {}},
        ),
    )
    step.add_input_port(in_port_name, in_port)
    await workflow.save(context)
    new_workflow, new_step = await _clone_step(step, workflow, context)
    _persistent_id_test(workflow, new_workflow, step, new_step)

    assert step.command.step.persistent_id != new_step.command.step.persistent_id
    step.command.step = None
    new_step.command.step = None
    for original_processor, new_processor in zip(
        step.output_processors.values(), new_step.output_processors.values()
    ):
        assert (
            original_processor.workflow.persistent_id
            != new_processor.workflow.persistent_id
        )
        _set_val_to_attributes(original_processor, ["workflow"], None)
        _set_val_to_attributes(new_processor, ["workflow"], None)
    _set_val_to_attributes(step, ["persistent_id", "workflow"], None)
    _set_val_to_attributes(new_step, ["persistent_id", "workflow"], None)
    assert are_equals(step, new_step)


@pytest.mark.asyncio
async def test_schedule_step(context: StreamFlowContext):
    """Test saving ScheduleStep on database and re-load it in a new Workflow"""
    workflow = (await create_workflow(context, num_port=0))[0]
    deploy_step = create_deploy_step(workflow)
    nof_deployments = 2
    step = create_schedule_step(
        workflow,
        [deploy_step for _ in range(nof_deployments)],
        BindingConfig(
            targets=[LocalTarget() for _ in range(nof_deployments)],
            filters=[
                FilterConfig(config={}, name=utils.random_name(), type="shuffle")
                for _ in range(nof_deployments)
            ],
        ),
    )
    await workflow.save(context)
    new_workflow, new_step = await _clone_step(step, workflow, context)
    _persistent_id_test(workflow, new_workflow, step, new_step)

    for original_filter, new_filter in zip(
        step.binding_config.filters, new_step.binding_config.filters
    ):
        # Config are read-only so workflows can share the same
        assert original_filter.persistent_id == new_filter.persistent_id
        _set_val_to_attributes(original_filter, ["persistent_id", "workflow"], None)
        _set_val_to_attributes(new_filter, ["persistent_id", "workflow"], None)
    _set_val_to_attributes(step, ["persistent_id", "workflow"], None)
    _set_val_to_attributes(new_step, ["persistent_id", "workflow"], None)
    assert are_equals(step, new_step)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "combinator",
    [
        get_dot_combinator(),
        get_cartesian_product_combinator(),
        get_loop_terminator_combinator(),
        get_nested_crossproduct(),
    ],
    ids=[
        "dot_combinator",
        "cartesian_product_combinator",
        "loop_termination_combinator",
        "nested_crossproduct",
    ],
)
async def test_combinator_step(context: StreamFlowContext, combinator: Combinator):
    """Test saving CombinatorStep on database and re-load it in a new Workflow"""
    workflow, (in_port, out_port, in_port_2, out_port_2) = await create_workflow(
        context, num_port=4
    )
    _set_workflow_in_combinator(combinator, workflow)
    step = workflow.create_step(
        cls=CombinatorStep,
        name=utils.random_name() + "-combinator",
        combinator=combinator,
    )
    port_name = "test"
    step.add_input_port(port_name, in_port)
    step.add_output_port(port_name, out_port)

    port_name_2 = f"{port_name}_2"
    step.add_input_port(port_name_2, in_port_2)
    step.add_output_port(port_name_2, out_port_2)

    await workflow.save(context)
    new_workflow, new_step = await _clone_step(step, workflow, context)
    _persistent_id_test(workflow, new_workflow, step, new_step)

    _set_val_to_attributes(step, ["persistent_id", "workflow"], None)
    _set_val_to_attributes(new_step, ["persistent_id", "workflow"], None)
    _workflow_in_combinator_test(step.combinator, new_step.combinator)
    _set_workflow_in_combinator(step.combinator, None)
    _set_workflow_in_combinator(new_step.combinator, None)
    assert are_equals(step, new_step)


@pytest.mark.asyncio
async def test_loop_combinator_step(context: StreamFlowContext):
    """Test saving LoopCombinatorStep on database and re-load it in a new Workflow"""
    workflow, (in_port, out_port, in_port_2, out_port_2) = await create_workflow(
        context, num_port=4
    )
    name = utils.random_name()
    step = workflow.create_step(
        cls=LoopCombinatorStep,
        name=name + "-combinator",
        combinator=LoopCombinator(name=name, workflow=workflow),
    )
    port_name = "test"
    step.add_input_port(port_name, in_port)
    step.add_output_port(port_name, out_port)

    port_name_2 = f"{port_name}_2"
    step.add_input_port(port_name_2, in_port_2)
    step.add_output_port(port_name_2, out_port_2)

    await workflow.save(context)
    new_workflow, new_step = await _clone_step(step, workflow, context)
    _persistent_id_test(workflow, new_workflow, step, new_step)

    _set_val_to_attributes(step, ["persistent_id", "workflow"], None)
    _set_val_to_attributes(new_step, ["persistent_id", "workflow"], None)
    _workflow_in_combinator_test(step.combinator, new_step.combinator)
    _set_workflow_in_combinator(step.combinator, None)
    _set_workflow_in_combinator(new_step.combinator, None)
    assert are_equals(step, new_step)


@pytest.mark.asyncio
async def test_deploy_step(context: StreamFlowContext):
    """Test saving DeployStep on database and re-load it in a new Workflow"""
    workflow = (await create_workflow(context, num_port=0))[0]
    step = create_deploy_step(workflow)
    await workflow.save(context)
    new_workflow, new_step = await _clone_step(step, workflow, context)
    _persistent_id_test(workflow, new_workflow, step, new_step)


@pytest.mark.asyncio
async def test_gather_step(context: StreamFlowContext):
    """Test saving GatherStep on database and re-load it in a new Workflow"""
    workflow = (await create_workflow(context, num_port=0))[0]
    await _base_step_test_process(
        workflow,
        GatherStep,
        {"name": utils.random_name() + "-gather", "depth": 1},
        context,
    )


@pytest.mark.asyncio
async def test_scatter_step(context: StreamFlowContext):
    """Test saving ScatterStep on database and re-load it in a new Workflow"""
    workflow = (await create_workflow(context, num_port=0))[0]
    await _base_step_test_process(
        workflow, ScatterStep, {"name": utils.random_name() + "-scatter"}, context
    )
