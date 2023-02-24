import posixpath
from rdflib import Graph
from ruamel.yaml.scalarstring import LiteralScalarString, DoubleQuotedScalarString

import pytest

from tests.conftest import save_load_and_test

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Workflow

from streamflow.workflow.step import CombinatorStep, ExecuteStep

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
    CWLFileToken,
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
from streamflow.cwl.command import (
    CWLCommand,
    CWLCommandToken,
    CWLObjectCommandToken,
    CWLExpressionCommand,
    CWLStepCommand,
    CWLUnionCommandToken,
    CWLMapCommandToken,
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


def create_cwl_command(step, command_tokens):
    return CWLCommand(
        step=step,
        absolute_initial_workdir_allowed=False,
        base_command=["command", "tool"],
        command_tokens=command_tokens,
        expression_lib=["Requirement"],
        failure_codes=[0, 0],
        full_js=False,
        initial_work_dir="/home",
        inplace_update=False,
        is_shell_command=False,
        success_codes=[1],
        step_stderr=None,
        step_stdin=None,
        step_stdout=None,
        time_limit=1000,
    )


def create_cwl_command_token(cls=CWLCommandToken, value=None):
    return cls(
        is_shell_command=False,
        item_separator="&",
        name="test",
        position=2,
        prefix="--test",
        separate=True,
        shell_quote=True,
        token_type="string",
        value=value,
    )


@pytest.mark.asyncio
async def test_cwl_file_token(context: StreamFlowContext):
    """Test saving and loading CWLFileToken from database"""
    token = CWLFileToken(
        value={
            "location": "file:///home/ubuntu/output.txt",
            "basename": "output.txt",
            "class": "File",
            "checksum": "sha1$aaaaaaaaaaaaaaaaaaaaaaaa",
            "size": 100,
            "path": "/home/ubuntu/output.txt",
        }
    )
    await save_load_and_test(token, context)


@pytest.mark.asyncio
async def test_cwl_command(context: StreamFlowContext):
    """Test saving and loading ExecuteStep with CWLCommand from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    await workflow.save(context)
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=port
    )
    step.command = create_cwl_command(step, [])
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_expression_command(context: StreamFlowContext):
    """Test saving and loading ExecuteStep with CWLExpressionCommand from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    await workflow.save(context)
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=port
    )
    step.command = CWLExpressionCommand(
        step=step,
        expression="some js expression",
        absolute_initial_workdir_allowed=False,
        expression_lib=["a", "b"],
        full_js=False,
        initial_work_dir="/tmp/workdir",
        inplace_update=False,
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_step_command(context: StreamFlowContext):
    """Test saving and loading ExecuteStep with CWLStepCommand from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    await workflow.save(context)
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=port
    )
    step.command = CWLStepCommand(
        step=step,
        absolute_initial_workdir_allowed=False,
        expression_lib=["a", "b"],
        full_js=False,
        initial_work_dir="/tmp/workdir",
        inplace_update=False,
        time_limit=10000,
    )
    step.command.input_expressions["test"] = "test"
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_command_token(context: StreamFlowContext):
    """Test saving and loading CWLCommannd with CWLCommandTokens from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    await workflow.save(context)
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=port
    )

    step.command = create_cwl_command(
        step,
        [
            create_cwl_command_token(value=DoubleQuotedScalarString("60")),
            create_cwl_command_token(
                value=LiteralScalarString("${ return 10 + 20 - (5 * 4) }")
            ),
        ],
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_command_token_nested(context: StreamFlowContext):
    """Test saving and loading CWLCommannd with nested CWLCommandTokens from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    await workflow.save(context)
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=port
    )

    step.command = create_cwl_command(
        step,
        [
            create_cwl_command_token(value=create_cwl_command_token(value=1123)),
            create_cwl_command_token(value=create_cwl_command_token(value="hello")),
        ],
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_object_command_token(context: StreamFlowContext):
    """Test saving and loading CWLCommannd with CWLObjectCommandTokens from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    await workflow.save(context)
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=port
    )
    step.command = create_cwl_command(
        step,
        [
            create_cwl_command_token(
                cls=CWLObjectCommandToken,
                value={
                    "a": create_cwl_command_token(value=10),
                    "b": create_cwl_command_token(value=234),
                },
            )
        ],
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_object_command_token_nested(context: StreamFlowContext):
    """Test saving and loading CWLCommandToken with nested CWLObjectCommandToken from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    await workflow.save(context)
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=port
    )

    command_tokens = [
        create_cwl_command_token(
            cls=CWLObjectCommandToken,
            # in CWL this structure is named record
            # zero:
            #   type: File
            #   params: null
            value={
                "zero": create_cwl_command_token(
                    cls=CWLObjectCommandToken,
                    value={
                        "type": create_cwl_command_token(value="File"),
                        "params": create_cwl_command_token(value=None),
                    },
                )
            },
        ),
        create_cwl_command_token(
            cls=CWLObjectCommandToken,
            value={
                "zero": create_cwl_command_token(
                    cls=CWLObjectCommandToken,
                    value={
                        "one": create_cwl_command_token(value="89"),
                        "two": create_cwl_command_token(value=29),
                        "three": create_cwl_command_token(value=None),
                    },
                )
            },
        ),
        create_cwl_command_token(value=11),
    ]
    step.command = create_cwl_command(step, command_tokens)
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_union_command_token(context: StreamFlowContext):
    """Test saving and loading CWLCommannd with CWLUnionCommandTokens from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    await workflow.save(context)
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=port
    )
    step.command = create_cwl_command(
        step,
        [
            create_cwl_command_token(
                cls=CWLUnionCommandToken,
                value=[
                    create_cwl_command_token(value="qwerty"),
                    create_cwl_command_token(value=987),
                    create_cwl_command_token(value="qaz"),
                ],
            )
        ],
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_union_command_token_nested(context: StreamFlowContext):
    """Test saving and loading CWLCommannd with nested CWLUnionCommandTokens from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    await workflow.save(context)
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=port
    )
    step.command = create_cwl_command(
        step,
        [
            create_cwl_command_token(
                cls=CWLUnionCommandToken,
                value=[
                    create_cwl_command_token(
                        cls=CWLUnionCommandToken,
                        value=[
                            create_cwl_command_token(value="aaa"),
                            create_cwl_command_token(value="bbb"),
                        ],
                    ),
                    create_cwl_command_token(
                        cls=CWLUnionCommandToken,
                        value=[
                            create_cwl_command_token(value="ccc"),
                            create_cwl_command_token(value="ddd"),
                        ],
                    ),
                ],
            )
        ],
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_map_command_token(context: StreamFlowContext):
    """Test saving and loading CWLCommannd with CWLMapCommandTokens from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    await workflow.save(context)
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=port
    )
    step.command = create_cwl_command(
        step,
        [
            create_cwl_command_token(
                cls=CWLMapCommandToken, value=create_cwl_command_token(value="z")
            ),
            create_cwl_command_token(
                cls=CWLMapCommandToken, value=create_cwl_command_token(value="xy")
            ),
        ],
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_list_merge_combinator(context: StreamFlowContext):
    """Test saving and loading CombinatorStep with ListMergeCombinator from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    port = workflow.create_port()
    await workflow.save(context)

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
    await workflow.save(context)

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
    await workflow.save(context)

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
    await workflow.save(context)

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
    await workflow.save(context)

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
    await workflow.save(context)

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
    await workflow.save(context)

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
    await workflow.save(context)

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
    await workflow.save(context)

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
    await workflow.save(context)

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
    await workflow.save(context)

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
    await workflow.save(context)

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
    await workflow.save(context)

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
    await workflow.save(context)

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
    await workflow.save(context)

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
    await workflow.save(context)

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
    await workflow.save(context)

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
    await workflow.save(context)

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
    await workflow.save(context)

    step = workflow.create_step(
        cls=CWLLoopOutputLastStep,
        name=posixpath.join(utils.random_name(), "-last"),
    )
    await save_load_and_test(step, context)
