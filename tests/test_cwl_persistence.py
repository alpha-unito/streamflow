import posixpath
from rdflib import Graph

import pytest

from tests.test_persistence import save_load_and_test

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Workflow

from streamflow.workflow.step import CombinatorStep

# abstract classes the extend the Step class: ConditionalStep, InputInjectorStep, LoopOutputStep, TransferStep, Transformer
from streamflow.cwl.utils import LoadListing, SecondaryFile
from streamflow.cwl.step import (
    CWLTransferStep,  # TransferStep
    CWLConditionalStep,  # ConditionalStep
    CWLInputInjectorStep,  # InputInjectorStep
    CWLLoopOutputAllStep,  # LoopOutputStep
    CWLLoopOutputLastStep,  # LoopOutputStep
)
from streamflow.cwl.combinator import ListMergeCombinator  # CombinatorStep
from streamflow.cwl.processor import (
    CWLTokenProcessor,
    CWLMapTokenProcessor,
    CWLObjectTokenProcessor,
    CWLUnionTokenProcessor,
)
from streamflow.cwl.transformer import (
    DefaultTransformer,
    DefaultRetagTransformer,
    CWLTokenTransformer,
    ValueFromTransformer,
    LoopValueFromTransformer,
    AllNonNullTransformer,
    FirstNonNullTransformer,
    ForwardTransformer,
    ListToElementTransformer,
    OnlyNonNullTransformer,
)


def create_cwl_token_processor(name, workflow):
    return CWLTokenProcessor(
        name=name,
        workflow=workflow,
        token_type="enum",
        enum_symbols=["path1", "path2"],
        expression_lib=["expr_lib1", "expr_lib2"],
        secondary_files=[SecondaryFile("file1", True)],
        format_graph=Graph(),
        load_listing=LoadListing.no_listing,
    )


@pytest.mark.asyncio
async def test_list_merge_combinator(context: StreamFlowContext):
    """Test saving and loading CombinatorStep with ListMergeCombinator from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    name = utils.random_name()
    step = workflow.create_step(
        cls=CombinatorStep,
        name=name + "-combinator",
        combinator=ListMergeCombinator(
            name=utils.random_name(),
            workflow=workflow,
            input_names=[port.name],
            output_name=name,
            flatten=False,
        ),
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_default_transformer(context: StreamFlowContext):
    """Test saving and loading DefaultTransformer from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    name = utils.random_name()
    transformer = workflow.create_step(
        cls=DefaultTransformer, name=name + "-transformer", default_port=port
    )
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_default_retag_transformer(context: StreamFlowContext):
    """Test saving and loading DefaultRetagTransformer from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    name = utils.random_name()
    transformer = workflow.create_step(
        cls=DefaultRetagTransformer, name=name + "-transformer", default_port=port
    )
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_cwl_token_transformer(context: StreamFlowContext):
    """Test saving and loading CWLTokenTransformer with CWLTokenProcessor from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None
    name = utils.random_name()
    transformer = workflow.create_step(
        cls=CWLTokenTransformer,
        name=name + "-transformer",
        port_name=port.name,
        processor=create_cwl_token_processor(port.name, workflow),
    )
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_value_from_transformer(context: StreamFlowContext):
    """Test saving and loading ValueFromTransformer with CWLTokenProcessor from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None
    name = utils.random_name()
    transformer = workflow.create_step(
        cls=ValueFromTransformer,
        name=name + "-value-from-transformer",
        processor=create_cwl_token_processor(port.name, workflow),
        port_name=port.name,
        expression_lib=True,
        full_js=False,
        value_from="$(1 + 1)",
    )
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_loop_value_from_transformer(context: StreamFlowContext):
    """Test saving and loading LoopValueFromTransformer with CWLTokenProcessor from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None
    name = utils.random_name()
    transformer = workflow.create_step(
        cls=LoopValueFromTransformer,
        name=name + "-loop-value-from-transformer",
        processor=create_cwl_token_processor(port.name, workflow),
        port_name=port.name,
        expression_lib=True,
        full_js=False,
        value_from="$(1 + 1 == 0)",
    )
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_cwl_map_token_transformer(context: StreamFlowContext):
    """Test saving and loading CWLTokenTransformer with CWLMapTokenProcessor from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None
    name = utils.random_name()
    transformer = workflow.create_step(
        cls=CWLTokenTransformer,
        name=name + "-transformer",
        port_name=port.name,
        processor=CWLMapTokenProcessor(
            name=port.name,
            workflow=workflow,
            processor=create_cwl_token_processor(port.name, workflow),
        ),
    )
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_cwl_object_token_transformer(context: StreamFlowContext):
    """Test saving and loading CWLTokenTransformer with CWLObjectTokenProcessor from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None
    name = utils.random_name()
    transformer = workflow.create_step(
        cls=CWLTokenTransformer,
        name=name + "-transformer",
        port_name=port.name,
        processor=CWLObjectTokenProcessor(
            name=port.name,
            workflow=workflow,
            processors={
                utils.random_name(): create_cwl_token_processor(port.name, workflow)
            },
        ),
    )
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_cwl_union_token_transformer(context: StreamFlowContext):
    """Test saving and loading CWLTokenTransformer with CWLUnionTokenProcessor from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None
    name = utils.random_name()
    transformer = workflow.create_step(
        cls=CWLTokenTransformer,
        name=name + "-transformer",
        port_name=port.name,
        processor=CWLUnionTokenProcessor(
            name=port.name,
            workflow=workflow,
            processors=[create_cwl_token_processor(port.name, workflow)],
        ),
    )
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_all_non_null_transformer(context: StreamFlowContext):
    """Test saving and loading AllNonNullTransformer from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    in_port = workflow.create_port()
    out_port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    name = utils.random_name()
    transformer = workflow.create_step(
        cls=AllNonNullTransformer, name=name + "-transformer"
    )
    transformer.add_input_port(name, in_port)
    transformer.add_output_port(name, out_port)
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_first_non_null_transformer(context: StreamFlowContext):
    """Test saving and loading FirstNonNullTransformer from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    in_port = workflow.create_port()
    out_port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    name = utils.random_name()
    transformer = workflow.create_step(
        cls=FirstNonNullTransformer, name=name + "-transformer"
    )
    transformer.add_input_port(name, in_port)
    transformer.add_output_port(name, out_port)
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_forward_transformer(context: StreamFlowContext):
    """Test saving and loading ForwardTransformer from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    in_port = workflow.create_port()
    out_port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    name = utils.random_name()
    transformer = workflow.create_step(
        cls=ForwardTransformer, name=name + "-transformer"
    )
    transformer.add_input_port(name, in_port)
    transformer.add_output_port(name, out_port)
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_list_to_element_transformer(context: StreamFlowContext):
    """Test saving and loading ListToElementTransformer from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    in_port = workflow.create_port()
    out_port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    name = utils.random_name()
    transformer = workflow.create_step(
        cls=ListToElementTransformer, name=name + "-transformer"
    )
    transformer.add_input_port(name, in_port)
    transformer.add_output_port(name, out_port)
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_only_non_null_transformer(context: StreamFlowContext):
    """Test saving and loading OnlyNonNullTransformer from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    in_port = workflow.create_port()
    out_port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    name = utils.random_name()
    transformer = workflow.create_step(
        cls=OnlyNonNullTransformer, name=name + "-transformer"
    )
    transformer.add_input_port(name, in_port)
    transformer.add_output_port(name, out_port)
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_cwl_conditional_step(context: StreamFlowContext):
    """Test saving and loading CWLConditionalStep from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    step = workflow.create_step(
        cls=CWLConditionalStep,
        name=utils.random_name() + "-when",
        expression="inputs.name.length == 10",
        expression_lib=[],  # MutableSequence[Any]
        full_js=True,
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_transfer_step(context: StreamFlowContext):
    """Test saving and loading CWLTransferStep from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    step = workflow.create_step(
        cls=CWLTransferStep,
        name=posixpath.join(utils.random_name(), "__transfer__", port.name),
        job_port=port,
    )

    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_input_injector_step(context: StreamFlowContext):
    """Test saving and loading CWLInputInjectorStep from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    step = workflow.create_step(
        cls=CWLInputInjectorStep,
        name=posixpath.join(utils.random_name(), "-injector"),
        job_port=port,
    )

    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_loop_output_all_step(context: StreamFlowContext):
    """Test saving and loading CWLLoopOutputAllStep from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    step = workflow.create_step(
        cls=CWLLoopOutputAllStep,
        name=posixpath.join(utils.random_name(), "-loop-output"),
    )

    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_loop_output_last_step(context: StreamFlowContext):
    """Test saving and loading CWLLoopOutputLastStep from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    assert workflow.persistent_id is None
    await workflow.save(context)
    assert workflow.persistent_id is not None

    step = workflow.create_step(
        cls=CWLLoopOutputLastStep,
        name=posixpath.join(utils.random_name(), "-last"),
    )

    await save_load_and_test(step, context)
