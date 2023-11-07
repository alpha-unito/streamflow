import posixpath

import pytest

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.cwl.combinator import ListMergeCombinator
from streamflow.cwl.processor import CWLTokenProcessor
from streamflow.cwl.step import (
    CWLConditionalStep,
    CWLTransferStep,
    CWLInputInjectorStep,
    CWLEmptyScatterConditionalStep,
    CWLLoopOutputAllStep,
)
from streamflow.cwl.transformer import (
    DefaultTransformer,
    DefaultRetagTransformer,
    CWLTokenTransformer,
    LoopValueFromTransformer,
)
from streamflow.workflow.step import CombinatorStep
from tests.conftest import (
    are_equals,
)
from tests.test_change_wf import (
    persistent_id_test,
    set_val_to_attributes,
    base_step_test_process,
    set_workflow_in_combinator, workflow_in_combinator_test,
)
from tests.utils.get_instances import create_workflow


@pytest.mark.asyncio
async def test_default_transformer(context: StreamFlowContext):
    """Test saving DefaultTransformer on database and re-load it in a new Workflow"""
    workflow, (port,) = await create_workflow(context, num_port=1)
    await base_step_test_process(
        workflow,
        DefaultTransformer,
        {"name": utils.random_name() + "-transformer", "default_port": port},
        context,
    )


@pytest.mark.asyncio
async def test_default_retag_transformer(context: StreamFlowContext):
    """Test saving DefaultRetagTransformer on database and re-load it in a new Workflow"""
    workflow, (port,) = await create_workflow(context, num_port=1)
    await base_step_test_process(
        workflow,
        DefaultRetagTransformer,
        {"name": utils.random_name() + "-transformer", "default_port": port},
        context,
    )


@pytest.mark.asyncio
async def test_cwl_token_transformer(context: StreamFlowContext):
    """Test saving CWLTokenProcessor on database and re-load it in a new Workflow"""
    workflow, (port,) = await create_workflow(context, num_port=1)
    step_name = utils.random_name()
    step, new_workflow, new_step = await base_step_test_process(
        workflow,
        CWLTokenTransformer,
        {
            "name": step_name + "-transformer",
            "port_name": "test",
            "processor": CWLTokenProcessor(
                name=step_name,
                workflow=workflow,
            ),
        },
        context,
        test_are_eq=False,
    )
    set_val_to_attributes(step, ["persistent_id", "workflow"], None)
    set_val_to_attributes(new_step, ["persistent_id", "workflow"], None)
    assert step.processor.workflow.persistent_id != new_step.processor.workflow.persistent_id
    set_val_to_attributes(step.processor, ["workflow"], None)
    set_val_to_attributes(new_step.processor, ["workflow"], None)
    assert are_equals(step, new_step)


@pytest.mark.asyncio
async def test_cwl_conditional_step(context: StreamFlowContext):
    """Test saving CWLConditionalStep on database and re-load it in a new Workflow"""
    workflow, (port,) = await create_workflow(context, num_port=1)
    await base_step_test_process(
        workflow,
        CWLConditionalStep,
        {
            "name": utils.random_name() + "-when",
            "expression": f"$(inputs.{utils.random_name()}.length == 1)",
            "full_js": True,
        },
        context,
    )


@pytest.mark.asyncio
async def test_cwl_transfer_step(context: StreamFlowContext):
    """Test saving CWLTransferStep on database and re-load it in a new Workflow"""
    workflow, (port,) = await create_workflow(context, num_port=1)
    await base_step_test_process(
        workflow,
        CWLTransferStep,
        {
            "name": posixpath.join(utils.random_name(), "__transfer__", "test"),
            "job_port": port,
        },
        context,
    )


@pytest.mark.asyncio
async def test_cwl_input_injector_step(context: StreamFlowContext):
    """Test saving CWLInputInjectorStep on database and re-load it in a new Workflow"""
    workflow, (port,) = await create_workflow(context, num_port=1)
    await base_step_test_process(
        workflow,
        CWLInputInjectorStep,
        {
            "name": utils.random_name() + "-injector",
            "job_port": port,
        },
        context,
    )


@pytest.mark.asyncio
async def test_empty_scatter_conditional_step(context: StreamFlowContext):
    """Test saving CWLEmptyScatterConditionalStep on database and re-load it in a new Workflow"""
    workflow, (port,) = await create_workflow(context, num_port=1)
    await base_step_test_process(
        workflow,
        CWLEmptyScatterConditionalStep,
        {
            "name": utils.random_name() + "-empty-scatter-condition",
            "scatter_method": "dotproduct",
        },
        context,
    )


@pytest.mark.asyncio
async def test_list_merge_combinator(context: StreamFlowContext):
    """Test saving ListMergeCombinator on database and re-load it in a new Workflow"""
    workflow, (port,) = await create_workflow(context, num_port=1)
    step, new_workflow, new_step = await base_step_test_process(
        workflow,
        CombinatorStep,
        {
            "name": utils.random_name() + "-combinator",
            "combinator": ListMergeCombinator(
                name=utils.random_name(),
                workflow=workflow,
                input_names=[port.name],
                output_name=port.name,
                flatten=False,
            ),
        },
        context,
        test_are_eq=False,
    )
    persistent_id_test(workflow, new_workflow, step, new_step)

    set_val_to_attributes(step, ["persistent_id", "workflow"], None)
    set_val_to_attributes(new_step, ["persistent_id", "workflow"], None)
    workflow_in_combinator_test(step.combinator, new_step.combinator)
    set_workflow_in_combinator(step.combinator, None)
    set_workflow_in_combinator(new_step.combinator, None)
    assert are_equals(step, new_step)


@pytest.mark.asyncio
async def test_loop_value_from_transformer(context: StreamFlowContext):
    """Test saving LoopValueFromTransformer on database and re-load it in a new Workflow"""
    workflow, (port,) = await create_workflow(context, num_port=1)
    step, new_workflow, new_step = await base_step_test_process(
        workflow,
        LoopValueFromTransformer,
        {
            "name": utils.random_name() + "-loop-value-from-transformer",
            "processor": CWLTokenProcessor(
                name=port.name,
                workflow=workflow,
            ),
            "port_name": port.name,
            "full_js": True,
            "value_from": f"$(inputs.{port.name} + 1)",
        },
        context,
        test_are_eq=False,
    )
    persistent_id_test(workflow, new_workflow, step, new_step)

    set_val_to_attributes(step, ["persistent_id", "workflow"], None)
    set_val_to_attributes(new_step, ["persistent_id", "workflow"], None)
    assert step.processor.workflow.persistent_id != new_step.processor.workflow.persistent_id
    set_val_to_attributes(step.processor, ["workflow"], None)
    set_val_to_attributes(new_step.processor, ["workflow"], None)
    assert are_equals(step, new_step)


@pytest.mark.asyncio
async def test_cwl_loop_output_all_step(context: StreamFlowContext):
    """Test saving CWLLoopOutputAllStep on database and re-load it in a new Workflow"""
    workflow, (port,) = await create_workflow(context, num_port=1)
    await base_step_test_process(
        workflow,
        CWLLoopOutputAllStep,
        {
            "name": utils.random_name() + "-loop-output",
        },
        context,
    )
