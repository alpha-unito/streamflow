from __future__ import annotations

import posixpath
from typing import Any, MutableSequence

import pytest
from rdflib import Graph
from ruamel.yaml.scalarstring import DoubleQuotedScalarString, LiteralScalarString

from streamflow.core import utils
from streamflow.core.command import CommandTokenProcessor, UnionCommandTokenProcessor
from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Step, Workflow
from streamflow.cwl.combinator import ListMergeCombinator
from streamflow.cwl.command import (
    CWLCommand,
    CWLCommandTokenProcessor,
    CWLExpressionCommand,
    CWLMapCommandTokenProcessor,
    CWLObjectCommandTokenProcessor,
)
from streamflow.cwl.processor import (
    CWLFileToken,
    CWLMapTokenProcessor,
    CWLObjectTokenProcessor,
    CWLTokenProcessor,
    CWLUnionTokenProcessor,
)
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
from streamflow.cwl.utils import LoadListing, SecondaryFile
from streamflow.cwl.workflow import CWLWorkflow
from streamflow.workflow.port import JobPort
from streamflow.workflow.step import CombinatorStep, ExecuteStep
from tests.conftest import save_load_and_test
from tests.utils.workflow import CWL_VERSION


def _create_cwl_command(
    step: Step, processors: MutableSequence[CommandTokenProcessor]
) -> CWLCommand:
    return CWLCommand(
        step=step,
        absolute_initial_workdir_allowed=False,
        base_command=["command", "tool"],
        processors=processors,
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


def _create_cwl_command_token_processor(
    processor: CommandTokenProcessor | None = None, expression: Any | None = None
):
    return CWLCommandTokenProcessor(
        is_shell_command=False,
        item_separator="&",
        name="test",
        position=2,
        prefix="--test",
        separate=True,
        shell_quote=True,
        token_type="string",
        processor=processor,
        expression=expression,
    )


def _create_cwl_token_processor(name: str, workflow: Workflow) -> CWLTokenProcessor:
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
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
    )
    port = workflow.create_port()
    await workflow.save(context)
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=port
    )
    step.command = _create_cwl_command(step, [])
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_expression_command(context: StreamFlowContext):
    """Test saving and loading ExecuteStep with CWLExpressionCommand from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
    )
    port = workflow.create_port()
    await workflow.save(context)
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=port
    )
    step.command = CWLExpressionCommand(
        step=step,
        expression="$(some.js.expression)",
        absolute_initial_workdir_allowed=False,
        expression_lib=["a", "b"],
        full_js=False,
        initial_work_dir="/tmp/workdir",
        inplace_update=False,
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_command_token_processor(context: StreamFlowContext):
    """Test saving and loading CWLCommand with CWLCommandTokens from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
    )
    port = workflow.create_port()
    await workflow.save(context)
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=port
    )

    step.command = _create_cwl_command(
        step,
        [
            _create_cwl_command_token_processor(
                expression=DoubleQuotedScalarString("60")
            ),
            _create_cwl_command_token_processor(
                expression=LiteralScalarString("${ return 10 + 20 - (5 * 4) }")
            ),
        ],
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_command_token_processors_nested(context: StreamFlowContext):
    """Test saving and loading CWLCommand with nested CWLCommandTokenProcessors from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
    )
    port = workflow.create_port()
    await workflow.save(context)
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=port
    )

    step.command = _create_cwl_command(
        step,
        [
            _create_cwl_command_token_processor(
                processor=_create_cwl_command_token_processor(expression=1123)
            ),
            _create_cwl_command_token_processor(
                processor=_create_cwl_command_token_processor(expression="hello")
            ),
        ],
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_object_command_token_processor(context: StreamFlowContext):
    """Test saving and loading CWLCommand with CWLObjectCommandProcessors from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
    )
    port = workflow.create_port()
    await workflow.save(context)
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=port
    )
    step.command = _create_cwl_command(
        step,
        [
            CWLObjectCommandTokenProcessor(
                name="test",
                processors={
                    "a": _create_cwl_command_token_processor(expression=10),
                    "b": _create_cwl_command_token_processor(expression=234),
                },
            )
        ],
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_object_command_token_processors_nested(context: StreamFlowContext):
    """Test saving and loading CWLCommandToken with nested CWLObjectCommandTokenProcessors from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
    )
    port = workflow.create_port()
    await workflow.save(context)
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=port
    )

    processors = [
        CWLObjectCommandTokenProcessor(
            name="test",
            processors={
                "zero": CWLObjectCommandTokenProcessor(
                    name="test",
                    processors={
                        "type": _create_cwl_command_token_processor(expression="File"),
                        "params": _create_cwl_command_token_processor(expression=None),
                    },
                )
            },
        ),
        CWLObjectCommandTokenProcessor(
            name="test",
            processors={
                "zero": CWLObjectCommandTokenProcessor(
                    name="test",
                    processors={
                        "one": _create_cwl_command_token_processor(expression="89"),
                        "two": _create_cwl_command_token_processor(expression=29),
                        "three": _create_cwl_command_token_processor(expression=None),
                    },
                )
            },
        ),
        _create_cwl_command_token_processor(expression=11),
    ]
    step.command = _create_cwl_command(step, processors)
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_union_command_token_processor(context: StreamFlowContext):
    """Test saving and loading CWLCommand with CWLUnionCommandTokenProcessor from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
    )
    port = workflow.create_port()
    await workflow.save(context)
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=port
    )
    step.command = _create_cwl_command(
        step,
        [
            UnionCommandTokenProcessor(
                name="test",
                processors=[
                    _create_cwl_command_token_processor(expression="qwerty"),
                    _create_cwl_command_token_processor(expression=987),
                    _create_cwl_command_token_processor(expression="qaz"),
                ],
            )
        ],
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_union_command_token_processor_nested(context: StreamFlowContext):
    """Test saving and loading CWLCommand with nested CWLUnionCommandProcessors from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
    )
    port = workflow.create_port()
    await workflow.save(context)
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=port
    )
    step.command = _create_cwl_command(
        step,
        [
            UnionCommandTokenProcessor(
                name="test",
                processors=[
                    UnionCommandTokenProcessor(
                        name="test",
                        processors=[
                            _create_cwl_command_token_processor(expression="aaa"),
                            _create_cwl_command_token_processor(expression="bbb"),
                        ],
                    ),
                    UnionCommandTokenProcessor(
                        name="test",
                        processors=[
                            _create_cwl_command_token_processor(expression="ccc"),
                            _create_cwl_command_token_processor(expression="ddd"),
                        ],
                    ),
                ],
            )
        ],
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_map_command_token_processor(context: StreamFlowContext):
    """Test saving and loading CWLCommand with CWLMapCommandTokenProcessor from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
    )
    port = workflow.create_port()
    await workflow.save(context)
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=port
    )
    step.command = _create_cwl_command(
        step,
        [
            CWLMapCommandTokenProcessor(
                name="test",
                processor=_create_cwl_command_token_processor(expression="z"),
            ),
            CWLMapCommandTokenProcessor(
                name="test",
                processor=_create_cwl_command_token_processor(expression="xy"),
            ),
        ],
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_list_merge_combinator(context: StreamFlowContext):
    """Test saving and loading CombinatorStep with ListMergeCombinator from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
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
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
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
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
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
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
    )
    port = workflow.create_port()
    await workflow.save(context)

    name = utils.random_name()
    transformer = workflow.create_step(
        cls=CWLTokenTransformer,
        name=name + "-transformer",
        port_name=port.name,
        processor=_create_cwl_token_processor(port.name, workflow),
    )
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_value_from_transformer(context: StreamFlowContext):
    """Test saving and loading ValueFromTransformer with CWLTokenProcessor from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
    )
    port = workflow.create_port()
    job_port = workflow.create_port(JobPort)
    await workflow.save(context)

    name = utils.random_name()
    transformer = workflow.create_step(
        cls=ValueFromTransformer,
        name=name + "-value-from-transformer",
        processor=_create_cwl_token_processor(port.name, workflow),
        port_name=port.name,
        expression_lib=True,
        full_js=False,
        value_from="$(1 + 1)",
        job_port=job_port,
    )
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_loop_value_from_transformer(context: StreamFlowContext):
    """Test saving and loading LoopValueFromTransformer with CWLTokenProcessor from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
    )
    port = workflow.create_port()
    job_port = workflow.create_port(JobPort)
    await workflow.save(context)

    name = utils.random_name()
    transformer = workflow.create_step(
        cls=LoopValueFromTransformer,
        name=name + "-loop-value-from-transformer",
        processor=_create_cwl_token_processor(port.name, workflow),
        port_name=port.name,
        expression_lib=True,
        full_js=False,
        value_from="$(1 + 1 == 0)",
        job_port=job_port,
    )
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_cwl_map_token_transformer(context: StreamFlowContext):
    """Test saving and loading CWLTokenTransformer with CWLMapTokenProcessor from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
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
            processor=_create_cwl_token_processor(port.name, workflow),
        ),
    )
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_cwl_object_token_transformer(context: StreamFlowContext):
    """Test saving and loading CWLTokenTransformer with CWLObjectTokenProcessor from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
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
                utils.random_name(): _create_cwl_token_processor(port.name, workflow)
            },
        ),
    )
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_cwl_union_token_transformer(context: StreamFlowContext):
    """Test saving and loading CWLTokenTransformer with CWLUnionTokenProcessor from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
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
            processors=[_create_cwl_token_processor(port.name, workflow)],
        ),
    )
    await save_load_and_test(transformer, context)


@pytest.mark.asyncio
async def test_all_non_null_transformer(context: StreamFlowContext):
    """Test saving and loading AllNonNullTransformer from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
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
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
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
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
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
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
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
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
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
async def test_cwl_empty_scatter_conditional_step(context: StreamFlowContext):
    """Test saving and loading CWLEmptyScatterConditionalStep from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
    )
    in_port = workflow.create_port()
    out_port = workflow.create_port()
    await workflow.save(context)

    name = utils.random_name()
    empty_scatter_conditional_step = workflow.create_step(
        cls=CWLEmptyScatterConditionalStep,
        name=name + "-empty-scatter-condition",
        scatter_method="dotproduct",
    )
    empty_scatter_conditional_step.add_input_port(name, in_port)
    empty_scatter_conditional_step.add_output_port(name, out_port)
    await save_load_and_test(empty_scatter_conditional_step, context)


@pytest.mark.asyncio
async def test_cwl_conditional_step(context: StreamFlowContext):
    """Test saving and loading CWLConditionalStep from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
    )
    skip_port = workflow.create_port()
    await workflow.save(context)

    step = workflow.create_step(
        cls=CWLConditionalStep,
        name=utils.random_name() + "-when",
        expression="$(inputs.name.length == 10)",
        expression_lib=[],
        full_js=True,
    )
    step.add_skip_port("test", skip_port)
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_loop_conditional_step(context: StreamFlowContext):
    """Test saving and loading CWLLoopConditionalStep from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
    )
    skip_port = workflow.create_port()
    await workflow.save(context)

    step = workflow.create_step(
        cls=CWLLoopConditionalStep,
        name=utils.random_name() + "-when",
        expression="$(inputs.name.length == 10)",
        expression_lib=[],
        full_js=True,
    )
    step.add_skip_port("test", skip_port)
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_transfer_step(context: StreamFlowContext):
    """Test saving and loading CWLTransferStep from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
    )
    port = workflow.create_port()
    await workflow.save(context)

    step = workflow.create_step(
        cls=CWLTransferStep,
        name=posixpath.join(utils.random_name(), "__transfer__", port.name),
        job_port=port,
        writable=True,
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_input_injector_step(context: StreamFlowContext):
    """Test saving and loading CWLInputInjectorStep from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
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
@pytest.mark.parametrize("step_cls", [CWLLoopOutputAllStep, CWLLoopOutputLastStep])
async def test_cwl_loop_output(context: StreamFlowContext, step_cls: type[Step]):
    """Test saving and loading CWLLoopOutput from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
    )
    await workflow.save(context)

    step = workflow.create_step(
        cls=step_cls,
        name=posixpath.join(utils.random_name(), "-loop-output"),
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwlworkflow(context: StreamFlowContext):
    """Test saving and loading CWLWorkflow from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
    )
    await save_load_and_test(workflow, context)
