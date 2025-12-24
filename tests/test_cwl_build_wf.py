import posixpath
from typing import cast

import pytest

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Step
from streamflow.cwl.combinator import ListMergeCombinator
from streamflow.cwl.processor import CWLTokenProcessor
from streamflow.cwl.step import (
    CWLConditionalStep,
    CWLEmptyScatterConditionalStep,
    CWLInputInjectorStep,
    CWLLoopConditionalStep,
    CWLLoopOutputAllStep,
    CWLLoopOutputLastStep,
    CWLTransferStep,
)
from streamflow.cwl.transformer import (
    AllNonNullTransformer,
    CWLTokenTransformer,
    DefaultRetagTransformer,
    DefaultTransformer,
    FirstNonNullTransformer,
    ForwardTransformer,
    ListToElementTransformer,
    LoopValueFromTransformer,
    OnlyNonNullTransformer,
    ValueFromTransformer,
)
from streamflow.cwl.workflow import CWLWorkflow
from streamflow.workflow.step import CombinatorStep
from tests.conftest import are_equals
from tests.utils.utils import (
    check_combinators,
    check_persistent_id,
    duplicate_and_test,
    inject_workflow_combinator,
    set_attributes_to_none,
)
from tests.utils.workflow import create_workflow


@pytest.mark.asyncio
@pytest.mark.parametrize("step_cls", [CWLLoopOutputAllStep, CWLLoopOutputLastStep])
async def test_cwl_loop_output(context: StreamFlowContext, step_cls: type[Step]):
    """Test saving CWLLoopOutputAllStep on database and re-load it in a new Workflow"""
    workflow, _ = await create_workflow(context, num_port=0)
    await duplicate_and_test(
        workflow,
        step_cls,
        {
            "name": utils.random_name() + "-loop-output",
        },
        context,
    )


@pytest.mark.asyncio
async def test_default_retag_transformer(context: StreamFlowContext):
    """Test saving DefaultRetagTransformer on database and re-load it in a new Workflow"""
    workflow, (port,) = await create_workflow(context, num_port=1)
    await duplicate_and_test(
        workflow,
        DefaultRetagTransformer,
        {
            "name": utils.random_name() + "-transformer",
            "default_port": port,
            "primary_port": "pci",
        },
        context,
    )


@pytest.mark.asyncio
async def test_default_transformer(context: StreamFlowContext):
    """Test saving DefaultTransformer on database and re-load it in a new Workflow"""
    workflow, (port,) = await create_workflow(context, num_port=1)
    await duplicate_and_test(
        workflow,
        DefaultTransformer,
        {"name": utils.random_name() + "-transformer", "default_port": port},
        context,
    )


@pytest.mark.asyncio
async def test_forward_transformer(context: StreamFlowContext):
    """Test saving ForwardTransformer on database and re-load it in a new Workflow"""
    workflow, _ = await create_workflow(context, num_port=0)
    await duplicate_and_test(
        workflow,
        ForwardTransformer,
        {
            "name": utils.random_name() + "-transformer",
        },
        context,
    )


@pytest.mark.asyncio
async def test_list_merge_combinator(context: StreamFlowContext):
    """Test saving ListMergeCombinator on database and re-load it in a new Workflow"""
    workflow, (port,) = await create_workflow(context, num_port=1)
    step, new_workflow, new_step = await duplicate_and_test(
        workflow,
        CombinatorStep,
        {
            "name": utils.random_name() + "-combinator",
            "combinator": ListMergeCombinator(
                name=utils.random_name(),
                workflow=cast(CWLWorkflow, workflow),
                input_names=[port.name],
                output_name=port.name,
                flatten=False,
            ),
        },
        context,
        test_are_eq=False,
    )
    check_persistent_id(workflow, new_workflow, step, new_step)

    set_attributes_to_none(step, set_id=True, set_wf=True)
    set_attributes_to_none(new_step, set_id=True, set_wf=True)
    check_combinators(step.combinator, new_step.combinator)
    inject_workflow_combinator(step.combinator, None)
    inject_workflow_combinator(new_step.combinator, None)
    assert are_equals(step, new_step)


@pytest.mark.asyncio
async def test_list_to_element_transformer(context: StreamFlowContext):
    """Test saving ListToElementTransformer on database and re-load it in a new Workflow"""
    workflow, _ = await create_workflow(context, num_port=0)
    await duplicate_and_test(
        workflow,
        ListToElementTransformer,
        {
            "name": utils.random_name() + "-transformer",
        },
        context,
    )


@pytest.mark.asyncio
async def test_loop_value_from_transformer(context: StreamFlowContext):
    """Test saving LoopValueFromTransformer on database and re-load it in a new Workflow"""
    workflow, (port,) = await create_workflow(context, num_port=1)
    step, new_workflow, new_step = await duplicate_and_test(
        workflow,
        LoopValueFromTransformer,
        {
            "name": utils.random_name() + "-loop-value-from-transformer",
            "processor": CWLTokenProcessor(
                name=port.name,
                workflow=cast(CWLWorkflow, workflow),
            ),
            "port_name": port.name,
            "full_js": True,
            "value_from": f"$(inputs.{port.name} + 1)",
        },
        context,
        test_are_eq=False,
    )
    check_persistent_id(workflow, new_workflow, step, new_step)

    set_attributes_to_none(step, set_id=True, set_wf=True)
    set_attributes_to_none(new_step, set_id=True, set_wf=True)
    assert (
        step.processor.workflow.persistent_id
        != new_step.processor.workflow.persistent_id
    )
    set_attributes_to_none(step.processor, set_id=True, set_wf=True)
    set_attributes_to_none(new_step.processor, set_id=True, set_wf=True)
    assert are_equals(step, new_step)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "transformer_cls",
    [AllNonNullTransformer, FirstNonNullTransformer, OnlyNonNullTransformer],
)
async def test_non_null_transformer(
    context: StreamFlowContext, transformer_cls: type[Step]
):
    """Test saving All/First/Only NonNullTransformer on database and re-load it in a new Workflow"""
    workflow, _ = await create_workflow(context, num_port=0)
    await duplicate_and_test(
        workflow,
        transformer_cls,
        {
            "name": utils.random_name() + "-transformer",
        },
        context,
    )


@pytest.mark.asyncio
async def test_value_from_transformer(context: StreamFlowContext):
    """Test saving ValueFromTransformer on database and re-load it in a new Workflow"""
    workflow, (port,) = await create_workflow(context, num_port=1)
    step, new_workflow, new_step = await duplicate_and_test(
        workflow,
        ValueFromTransformer,
        {
            "name": utils.random_name() + "-value-from-transformer",
            "processor": CWLTokenProcessor(
                name=port.name,
                workflow=cast(CWLWorkflow, workflow),
            ),
            "port_name": port.name,
            "full_js": True,
            "value_from": f"$(inputs.{port.name} + 1)",
        },
        context,
        test_are_eq=False,
    )
    set_attributes_to_none(step, set_id=True, set_wf=True)
    set_attributes_to_none(new_step, set_id=True, set_wf=True)
    assert (
        step.processor.workflow.persistent_id
        != new_step.processor.workflow.persistent_id
    )
    set_attributes_to_none(step.processor, set_wf=True)
    set_attributes_to_none(new_step.processor, set_wf=True)
    assert are_equals(step, new_step)


@pytest.mark.asyncio
async def test_cwl_conditional_step(context: StreamFlowContext):
    """Test saving CWLConditionalStep on database and re-load it in a new Workflow"""
    workflow, _ = await create_workflow(context, num_port=0)
    await duplicate_and_test(
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
async def test_cwl_empty_scatter_conditional_step(context: StreamFlowContext):
    """Test saving CWLEmptyScatterConditionalStep on database and re-load it in a new Workflow"""
    workflow, _ = await create_workflow(context, num_port=0)
    await duplicate_and_test(
        workflow,
        CWLEmptyScatterConditionalStep,
        {
            "name": utils.random_name() + "-empty-scatter-condition",
            "scatter_method": "dotproduct",
        },
        context,
    )


@pytest.mark.asyncio
async def test_cwl_input_injector_step(context: StreamFlowContext):
    """Test saving CWLInputInjectorStep on database and re-load it in a new Workflow"""
    workflow, (port,) = await create_workflow(context, num_port=1)
    await duplicate_and_test(
        workflow,
        CWLInputInjectorStep,
        {
            "name": utils.random_name() + "-injector",
            "job_port": port,
        },
        context,
    )


@pytest.mark.asyncio
async def test_cwl_loop_conditional_step(context: StreamFlowContext):
    """Test saving CWLLoopConditionalStep on database and re-load it in a new Workflow"""
    workflow, _ = await create_workflow(context, num_port=0)
    await duplicate_and_test(
        workflow,
        CWLLoopConditionalStep,
        {
            "name": utils.random_name() + "-when",
            "expression": f"$(inputs.{utils.random_name()}.length == 1)",
            "full_js": True,
        },
        context,
    )


@pytest.mark.asyncio
async def test_cwl_token_transformer(context: StreamFlowContext):
    """Test saving CWLTokenTransformer on database and re-load it in a new Workflow"""
    workflow, _ = await create_workflow(context, num_port=0)
    step_name = utils.random_name()
    step, new_workflow, new_step = await duplicate_and_test(
        workflow,
        CWLTokenTransformer,
        {
            "name": step_name + "-transformer",
            "port_name": "test",
            "processor": CWLTokenProcessor(
                name=step_name,
                workflow=cast(CWLWorkflow, workflow),
            ),
        },
        context,
        test_are_eq=False,
    )
    set_attributes_to_none(step, set_id=True, set_wf=True)
    set_attributes_to_none(new_step, set_id=True, set_wf=True)
    assert (
        step.processor.workflow.persistent_id
        != new_step.processor.workflow.persistent_id
    )
    set_attributes_to_none(step.processor, set_id=True, set_wf=True)
    set_attributes_to_none(new_step.processor, set_id=True, set_wf=True)
    assert are_equals(step, new_step)


@pytest.mark.asyncio
async def test_cwl_transfer_step(context: StreamFlowContext):
    """Test saving CWLTransferStep on database and re-load it in a new Workflow"""
    workflow, (port,) = await create_workflow(context, num_port=1)
    await duplicate_and_test(
        workflow=workflow,
        step_cls=CWLTransferStep,
        kwargs_step={
            "name": posixpath.join(utils.random_name(), "__transfer__", "test"),
            "job_port": port,
            "writable": True,
        },
        context=context,
    )
