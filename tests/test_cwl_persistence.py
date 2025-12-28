from __future__ import annotations

import posixpath
from collections.abc import MutableSequence
from typing import Any, cast

import pytest
from rdflib import Graph, Literal, URIRef
from ruamel.yaml.scalarstring import DoubleQuotedScalarString, LiteralScalarString

from streamflow.core import utils
from streamflow.core.config import BindingConfig
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import FilterConfig, LocalTarget
from streamflow.core.processor import (
    CommandOutputProcessor,
    MapTokenProcessor,
    NullTokenProcessor,
    ObjectTokenProcessor,
    PopCommandOutputProcessor,
    UnionCommandOutputProcessor,
    UnionTokenProcessor,
)
from streamflow.core.workflow import CommandTokenProcessor, Step
from streamflow.cwl.combinator import ListMergeCombinator
from streamflow.cwl.command import (
    CWLCommand,
    CWLCommandTokenProcessor,
    CWLExpressionCommand,
    CWLMapCommandTokenProcessor,
    CWLObjectCommandTokenProcessor,
)
from streamflow.cwl.hardware import CWLHardwareRequirement
from streamflow.cwl.processor import (
    CWLCommandOutputProcessor,
    CWLExpressionToolOutputProcessor,
    CWLFileToken,
    CWLObjectCommandOutputProcessor,
    CWLTokenProcessor,
)
from streamflow.cwl.step import (
    CWLConditionalStep,
    CWLEmptyScatterConditionalStep,
    CWLExecuteStep,
    CWLInputInjectorStep,
    CWLLoopConditionalStep,
    CWLLoopOutputAllStep,
    CWLLoopOutputLastStep,
    CWLScheduleStep,
    CWLTransferStep,
)
from streamflow.cwl.transformer import (
    AllNonNullTransformer,
    CloneTransformer,
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
from streamflow.workflow.command import UnionCommandTokenProcessor
from streamflow.workflow.port import ConnectorPort, JobPort
from streamflow.workflow.step import CombinatorStep, ExecuteStep
from tests.conftest import save_load_and_test
from tests.utils.utils import get_full_instantiation
from tests.utils.workflow import CWL_VERSION, create_workflow


def _create_cwl_command(
    step: Step, processors: MutableSequence[CommandTokenProcessor] | None = None
) -> CWLCommand:
    return get_full_instantiation(
        cls_=CWLCommand,
        step=step,
        absolute_initial_workdir_allowed=True,
        processors=processors or [],
        base_command=["command", "tool"],
        expression_lib=["Requirement"],
        environment={"ARCH": "$(inputs.arch)", "PYTHONPATH": "$(inputs.pythonpath)"},
        failure_codes=[0, 0],
        full_js=True,
        initial_work_dir="/home",
        inplace_update=True,
        is_shell_command=True,
        success_codes=[1],
        step_stderr="stderr",
        step_stdin="stdin",
        step_stdout="stdout",
        time_limit=1000,
    )


def _create_cwl_command_output_processor(
    name: str, workflow: CWLWorkflow
) -> CommandOutputProcessor:
    return get_full_instantiation(
        cls_=CWLCommandOutputProcessor,
        name=name,
        workflow=workflow,
        target=get_full_instantiation(cls_=LocalTarget, workdir="/home"),
        token_type=["string"],
        enum_symbols=["test"],
        expression_lib=["mylib"],
        file_format="file",
        full_js=True,
        glob="*.png",
        load_contents=True,
        load_listing=LoadListing.shallow_listing,
        optional=True,
        output_eval="$(do something)",
        secondary_files=[
            get_full_instantiation(
                cls_=SecondaryFile, pattern=".bai", required="1 == 1"
            )
        ],
        single=True,
        streamable=True,
    )


def _create_cwl_command_token_processor(
    expression: Any | None = None,
) -> CWLCommandTokenProcessor:
    return get_full_instantiation(
        cls_=CWLCommandTokenProcessor,
        name="test",
        expression=expression,
        processor=CWLCommandTokenProcessor(
            "test1", "test2"
        ),  # no full instantiation required
        token_type="string",
        is_shell_command=True,
        item_separator="&",
        position=2,
        prefix="--test",
        separate=False,
        shell_quote=False,
    )


def _create_cwl_token_processor(name: str, workflow: CWLWorkflow) -> CWLTokenProcessor:
    return get_full_instantiation(
        cls_=CWLTokenProcessor,
        name=name,
        workflow=workflow,
        token_type="enum",
        enum_symbols=["path1", "path2"],
        expression_lib=["expr_lib1", "expr_lib2"],
        file_format="directory",
        full_js=True,
        load_contents=True,
        load_listing=LoadListing.no_listing,
        only_propagate_secondary_files=False,
        secondary_files=[
            get_full_instantiation(cls_=SecondaryFile, pattern="file1", required=True)
        ],
        streamable=True,
    )


@pytest.mark.asyncio
async def test_clone_transformer(context: StreamFlowContext):
    """Test saving and loading CloneTransformer from database"""
    workflow, (in_port, replica_port, out_port) = await create_workflow(
        context=context, num_port=3
    )
    name = utils.random_name()
    step = get_full_instantiation(
        cls_=CloneTransformer,
        name=name + "-transformer",
        replicas_port=replica_port,
        workflow=workflow,
    )
    workflow.steps[step.name] = step
    step.add_input_port(name, in_port)
    step.add_output_port(name, out_port)
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_file_token(context: StreamFlowContext):
    """Test saving and loading CWLFileToken from database"""
    token = get_full_instantiation(
        cls_=CWLFileToken,
        value={
            "basename": "version.py",
            "checksum": "sha1$a4a8b0c0b19d3187a1ab8c9346fc105978115781",
            "class": "File",
            "dirname": "/home/ubuntu/streamflow/streamflow",
            "location": "file:///home/ubuntu/streamflow/streamflow/version.py",
            "nameext": ".py",
            "nameroot": "version",
            "path": "/home/ubuntu/streamflow/streamflow/version.py",
            "size": 24,
        },
        tag="0.0",
        recoverable=True,
    )
    await save_load_and_test(token, context)


@pytest.mark.asyncio
@pytest.mark.parametrize("processor_t", ["none", "primitive", "map", "object", "union"])
async def test_cwl_command(context: StreamFlowContext, processor_t: str):
    """Test saving and loading ExecuteStep with CWLCommand and CWLCommandTokenProcessor classes from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
    )
    job_port = workflow.create_port(JobPort)
    await workflow.save(context)
    match processor_t:
        case "none":
            processors = None
        case "primitive":
            processors = [
                _create_cwl_command_token_processor(
                    expression=DoubleQuotedScalarString("60")
                ),
                _create_cwl_command_token_processor(
                    expression=LiteralScalarString("${ return 10 + 20 - (5 * 4) }")
                ),
            ]
        case "map":
            processors = [
                get_full_instantiation(
                    cls_=CWLMapCommandTokenProcessor,
                    name="test1",
                    processor=_create_cwl_command_token_processor(expression="z"),
                ),
                get_full_instantiation(
                    cls_=CWLMapCommandTokenProcessor,
                    name="test2",
                    processor=_create_cwl_command_token_processor(expression="xy"),
                ),
            ]
        case "object":
            processors = [
                get_full_instantiation(
                    cls_=CWLObjectCommandTokenProcessor,
                    name="test",
                    processors={
                        "a": _create_cwl_command_token_processor(expression=10),
                        "b": _create_cwl_command_token_processor(expression=234),
                    },
                )
            ]
        case "union":
            processors = [
                get_full_instantiation(
                    cls_=UnionCommandTokenProcessor,
                    name="test",
                    processors=[
                        _create_cwl_command_token_processor(expression="qwerty"),
                        _create_cwl_command_token_processor(expression=987),
                        _create_cwl_command_token_processor(expression="qaz"),
                    ],
                )
            ]
        case _:
            raise Exception(f"Unknown processor type: {processor_t}")
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=job_port
    )
    step.command = _create_cwl_command(step=step, processors=processors)
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_expression_command(context: StreamFlowContext):
    """Test saving and loading ExecuteStep with CWLExpressionCommand from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
    )
    job_port = workflow.create_port(JobPort)
    await workflow.save(context)
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=job_port
    )
    step.command = get_full_instantiation(
        cls_=CWLExpressionCommand,
        step=step,
        expression="$(some.js.expression)",
        absolute_initial_workdir_allowed=True,
        expression_lib=["a", "b"],
        full_js=True,
        initial_work_dir=["/tmp/workdir"],
        inplace_update=True,
        time_limit="$(1+1)",
    )
    await save_load_and_test(step, context)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "output_type",
    ["no_output", "default", "expression", "primitive", "object", "union"],
)
async def test_cwl_execute_step(context: StreamFlowContext, output_type: str):
    """Test saving and loading CWLExecuteStep from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
    )
    job_port = workflow.create_port(JobPort)
    if output_type != "no_output":
        port = workflow.create_port()
    await workflow.save(context)

    step = get_full_instantiation(
        cls_=CWLExecuteStep,
        name=utils.random_name(),
        job_port=job_port,
        recoverable="$(inputs.file)",
        full_js=True,
        expression_lib=["a", "b"],
        workflow=workflow,
    )
    workflow.steps[step.name] = step

    if output_type != "no_output":
        match output_type:
            case "default":
                processor = None
            case "expression":
                processor = get_full_instantiation(
                    cls_=CWLExpressionToolOutputProcessor,
                    name=utils.random_name(),
                    workflow=workflow,
                    target=get_full_instantiation(cls_=LocalTarget, workdir="/home"),
                    token_type=["string"],
                    enum_symbols=["test"],
                    file_format="file",
                    optional=True,
                    streamable=True,
                )
            case "primitive":
                processor = _create_cwl_command_output_processor(
                    name=utils.random_name(), workflow=workflow
                )
            case "object":
                processor = get_full_instantiation(
                    cls_=CWLObjectCommandOutputProcessor,
                    name=utils.random_name(),
                    workflow=workflow,
                    processors={
                        "attr_a": _create_cwl_command_output_processor(
                            name=utils.random_name(), workflow=workflow
                        ),
                    },
                    expression_lib=["a", "b"],
                    full_js=True,
                    output_eval="$(1 == 1)",
                    target=LocalTarget("/shared"),
                    single=True,
                )
            case "union":
                inner_p = _create_cwl_command_output_processor(
                    name=utils.random_name(), workflow=workflow
                )
                processor = get_full_instantiation(
                    cls_=UnionCommandOutputProcessor,
                    name=utils.random_name(),
                    workflow=workflow,
                    processors=[
                        get_full_instantiation(
                            cls_=PopCommandOutputProcessor,
                            name=utils.random_name(),
                            workflow=workflow,
                            processor=inner_p,
                            target=LocalTarget("/shared"),
                        ),
                        inner_p,
                    ],
                    target=LocalTarget("/shared"),
                )
            case _:
                raise Exception(f"Unknown output type: {output_type}")
        step.add_output_port("my_output", port, processor)
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_list_merge_combinator(context: StreamFlowContext):
    """Test saving and loading CombinatorStep with ListMergeCombinator from database"""
    workflow, (port,) = await create_workflow(context=context, num_port=1)
    name = utils.random_name()
    combinator = get_full_instantiation(
        cls_=ListMergeCombinator,
        name=utils.random_name(),
        workflow=cast(CWLWorkflow, workflow),
        input_names=[port.name],
        output_name=name,
        flatten=True,
    )
    combinator.add_combinator(
        get_full_instantiation(
            cls_=ListMergeCombinator,
            name=utils.random_name(),
            workflow=cast(CWLWorkflow, workflow),
            input_names=[port.name],
            output_name=name,
            flatten=True,
        ),
        {"item_test"},
    )
    step = get_full_instantiation(
        cls_=CombinatorStep,
        name=name + "-combinator",
        combinator=combinator,
        workflow=workflow,
    )
    workflow.steps[step.name] = step
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_default_transformer(context: StreamFlowContext):
    """Test saving and loading DefaultTransformer from database"""
    workflow, (port,) = await create_workflow(context=context, num_port=1)
    step = get_full_instantiation(
        cls_=DefaultTransformer,
        name=f"{utils.random_name()}-transformer",
        default_port=port,
        workflow=workflow,
    )
    workflow.steps[step.name] = step
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_default_retag_transformer(context: StreamFlowContext):
    """Test saving and loading DefaultRetagTransformer from database"""
    workflow, (port,) = await create_workflow(context=context, num_port=1)
    step = get_full_instantiation(
        cls_=DefaultRetagTransformer,
        name=f"{utils.random_name()}-transformer",
        default_port=port,
        primary_port="prime",
        workflow=workflow,
    )
    workflow.steps[step.name] = step
    await save_load_and_test(step, context)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "processor_t", ["primitive", "map", "object", "union", "optional"]
)
async def test_cwl_token_transformer(context: StreamFlowContext, processor_t: str):
    """Test saving and loading CWLTokenTransformer with different TokenProcessor classes from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
    )
    port = workflow.create_port()
    if workflow.format_graph is None:
        workflow.format_graph = Graph()
    await workflow.save(context)
    processor = None
    match processor_t:
        case "primitive":
            processor = _create_cwl_token_processor(port.name, workflow)
        case "map":
            processor = get_full_instantiation(
                cls_=MapTokenProcessor,
                name=port.name,
                workflow=workflow,
                processor=_create_cwl_token_processor(port.name, workflow),
            )
        case "object":
            processor = get_full_instantiation(
                cls_=ObjectTokenProcessor,
                name=port.name,
                workflow=workflow,
                processors={
                    f"p_{i}": _create_cwl_token_processor(port.name, workflow)
                    for i in range(2)
                },
            )
        case "union" | "optional":
            processor = get_full_instantiation(
                cls_=UnionTokenProcessor,
                name=port.name,
                workflow=workflow,
                processors=[
                    (
                        get_full_instantiation(
                            cls_=NullTokenProcessor, name=port.name, workflow=workflow
                        )
                        if i == 0 and processor_t == "optional"
                        else _create_cwl_token_processor(port.name, workflow)
                    )
                    for i in range(2)
                ],
            )
        case _:
            raise Exception(f"Unknown processor type: {processor_t}")
    step = get_full_instantiation(
        cls_=CWLTokenTransformer,
        name=f"{utils.random_name()}-transformer",
        port_name=port.name,
        processor=processor,
        workflow=workflow,
    )
    workflow.steps[step.name] = step
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_value_from_transformer(context: StreamFlowContext):
    """Test saving and loading ValueFromTransformer with CWLTokenProcessor from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
    )
    port = workflow.create_port()
    if workflow.format_graph is None:
        workflow.format_graph = Graph()
    await workflow.save(context)

    step = get_full_instantiation(
        cls_=ValueFromTransformer,
        name=f"{utils.random_name()}-value-from-transformer",
        processor=_create_cwl_token_processor(port.name, workflow),
        port_name=port.name,
        expression_lib=True,
        full_js=True,
        value_from="$(1 + 1)",
        workflow=workflow,
    )
    workflow.steps[step.name] = step
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_loop_value_from_transformer(context: StreamFlowContext):
    """Test saving and loading LoopValueFromTransformer with CWLTokenProcessor from database"""
    workflow = CWLWorkflow(
        context=context, name=utils.random_name(), config={}, cwl_version=CWL_VERSION
    )
    ports = [workflow.create_port() for _ in range(2)]
    port_name = utils.random_name()
    if workflow.format_graph is None:
        workflow.format_graph = Graph()
    await workflow.save(context)

    step = cast(
        LoopValueFromTransformer,
        get_full_instantiation(
            cls_=LoopValueFromTransformer,
            name=f"{utils.random_name()}-loop-value-from-transformer",
            processor=_create_cwl_token_processor(port_name, workflow),
            port_name=port_name,
            expression_lib=True,
            full_js=True,
            value_from="$(1 + 1 == 0)",
            workflow=workflow,
        ),
    )
    step.add_loop_input_port(port_name, ports[0])
    step.add_loop_source_port(port_name, ports[1])
    workflow.steps[step.name] = step
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_all_non_null_transformer(context: StreamFlowContext):
    """Test saving and loading AllNonNullTransformer from database"""
    workflow, (in_port, out_port) = await create_workflow(context=context)
    name = utils.random_name()
    step = get_full_instantiation(
        cls_=AllNonNullTransformer,
        name=name + "-transformer",
        workflow=workflow,
    )
    workflow.steps[step.name] = step
    step.add_input_port(name, in_port)
    step.add_output_port(name, out_port)
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_first_non_null_transformer(context: StreamFlowContext):
    """Test saving and loading FirstNonNullTransformer from database"""
    workflow, (in_port, out_port) = await create_workflow(context=context)
    name = utils.random_name()
    step = get_full_instantiation(
        cls_=FirstNonNullTransformer,
        name=name + "-transformer",
        workflow=workflow,
    )
    workflow.steps[step.name] = step
    step.add_input_port(name, in_port)
    step.add_output_port(name, out_port)
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_forward_transformer(context: StreamFlowContext):
    """Test saving and loading ForwardTransformer from database"""
    workflow, (in_port, out_port) = await create_workflow(context=context)
    name = utils.random_name()
    step = get_full_instantiation(
        cls_=ForwardTransformer,
        name=name + "-transformer",
        workflow=workflow,
    )
    workflow.steps[step.name] = step
    step.add_input_port(name, in_port)
    step.add_output_port(name, out_port)
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_list_to_element_transformer(context: StreamFlowContext):
    """Test saving and loading ListToElementTransformer from database"""
    workflow, (in_port, out_port) = await create_workflow(context=context)
    name = utils.random_name()
    step = get_full_instantiation(
        cls_=ListToElementTransformer,
        name=name + "-transformer",
        workflow=workflow,
    )
    workflow.steps[step.name] = step
    step.add_input_port(name, in_port)
    step.add_output_port(name, out_port)
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_only_non_null_transformer(context: StreamFlowContext):
    """Test saving and loading OnlyNonNullTransformer from database"""
    workflow, (in_port, out_port) = await create_workflow(context=context)
    name = utils.random_name()
    step = get_full_instantiation(
        cls_=OnlyNonNullTransformer,
        name=name + "-transformer",
        workflow=workflow,
    )
    workflow.steps[step.name] = step
    step.add_input_port(name, in_port)
    step.add_output_port(name, out_port)
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_empty_scatter_conditional_step(context: StreamFlowContext):
    """Test saving and loading CWLEmptyScatterConditionalStep from database"""
    workflow, (in_port, out_port) = await create_workflow(context=context)
    name = utils.random_name()
    step = get_full_instantiation(
        cls_=CWLEmptyScatterConditionalStep,
        name=name + "-empty-scatter-condition",
        scatter_method="dotproduct",
        workflow=workflow,
    )
    workflow.steps[step.name] = step
    step.add_input_port(name, in_port)
    step.add_output_port(name, out_port)
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_conditional_step(context: StreamFlowContext):
    """Test saving and loading CWLConditionalStep from database"""
    workflow, (skip_port,) = await create_workflow(context=context, num_port=1)
    step = get_full_instantiation(
        cls_=CWLConditionalStep,
        name=f"{utils.random_name()}-when",
        expression="$(inputs.name.length == 10)",
        expression_lib=[],
        full_js=True,
        workflow=workflow,
    )
    workflow.steps[step.name] = step
    step.add_skip_port("test", skip_port)
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_loop_conditional_step(context: StreamFlowContext):
    """Test saving and loading CWLLoopConditionalStep from database"""
    workflow, (skip_port,) = await create_workflow(context=context, num_port=1)
    step = get_full_instantiation(
        cls_=CWLLoopConditionalStep,
        name=f"{utils.random_name()}-when",
        expression="$(inputs.name.length == 10)",
        expression_lib=[],
        full_js=True,
        workflow=workflow,
    )
    workflow.steps[step.name] = step
    step.add_skip_port("test", skip_port)
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_transfer_step(context: StreamFlowContext):
    """Test saving and loading CWLTransferStep from database"""
    workflow, (job_port,) = await create_workflow(context=context, num_port=1)
    await workflow.save(context)
    step = get_full_instantiation(
        cls_=CWLTransferStep,
        name=posixpath.join(utils.random_name(), "__transfer__", "test"),
        job_port=job_port,
        writable=True,
        prefix_path=False,
        workflow=workflow,
    )
    workflow.steps[step.name] = step
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_input_injector_step(context: StreamFlowContext):
    """Test saving and loading CWLInputInjectorStep from database"""
    workflow, (job_port,) = await create_workflow(context=context, num_port=1)
    await workflow.save(context)
    step = get_full_instantiation(
        cls_=CWLInputInjectorStep,
        name=posixpath.join(utils.random_name(), "-injector"),
        job_port=job_port,
        workflow=workflow,
    )
    workflow.steps[step.name] = step
    await save_load_and_test(step, context)


@pytest.mark.asyncio
@pytest.mark.parametrize("step_cls", [CWLLoopOutputAllStep, CWLLoopOutputLastStep])
async def test_cwl_loop_output(context: StreamFlowContext, step_cls: type[Step]):
    """Test saving and loading CWLLoopOutput from database"""
    workflow, _ = await create_workflow(context=context, num_port=0)
    step = get_full_instantiation(
        cls_=step_cls,
        name=posixpath.join(utils.random_name(), "-loop-output"),
        workflow=workflow,
    )
    workflow.steps[step.name] = step
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_schedule_step(context: StreamFlowContext):
    """Test saving and loading CWLScheduleStep with a CWLHardwareRequirement from database"""
    workflow, (job_port,) = await create_workflow(context, 1)
    binding_config = get_full_instantiation(
        cls_=BindingConfig,
        targets=[
            get_full_instantiation(cls_=LocalTarget, workdir=utils.random_name()),
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
        cls_=CWLScheduleStep,
        name=posixpath.join(utils.random_name(), "__schedule__"),
        binding_config=binding_config,
        connector_ports=connector_ports,
        job_port=job_port,
        job_prefix="something",
        hardware_requirement=get_full_instantiation(
            cls_=CWLHardwareRequirement,
            cwl_version=CWL_VERSION,
            cores=1.5,
            memory="$(inputs.mem * 2)",
            tmpdir=1024,
            outdir=4096,
            full_js=True,
            expression_lib=["function function nope(message) {return message}"],
        ),
        input_directory="/inputs",
        output_directory="/outputs",
        tmp_directory="/tmp",
        workflow=workflow,
    )
    workflow.steps[step.name] = step
    await save_load_and_test(step, context)


@pytest.mark.asyncio
async def test_cwl_workflow(context: StreamFlowContext):
    """Test saving and loading CWLWorkflow from database"""
    g = Graph()
    g.add(
        (
            URIRef("http://example.org/book1"),
            URIRef("http://example.org/title"),
            Literal("The Great Gatsby"),
        )
    )
    workflow = get_full_instantiation(
        cls_=CWLWorkflow,
        context=context,
        name=utils.random_name(),
        config={"key": "value"},
        cwl_version=CWL_VERSION,
        format_graph=g,
    )
    await save_load_and_test(workflow, context)
