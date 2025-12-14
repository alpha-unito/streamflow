from __future__ import annotations

import posixpath
from collections.abc import MutableSequence
from typing import cast

import pytest

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Port, Status, Step, Token, Workflow
from streamflow.cwl.combinator import ListMergeCombinator
from streamflow.cwl.command import CWLCommand, CWLCommandTokenProcessor
from streamflow.cwl.hardware import CWLHardwareRequirement
from streamflow.cwl.processor import CWLTokenProcessor
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
    CartesianProductSizeTransformer,
    CloneTransformer,
    CWLTokenTransformer,
    DefaultRetagTransformer,
    DefaultTransformer,
    DotProductSizeTransformer,
    FirstNonNullTransformer,
    ForwardTransformer,
    ListToElementTransformer,
    LoopValueFromTransformer,
    OnlyNonNullTransformer,
    ValueFromTransformer,
)
from streamflow.cwl.translator import create_command_output_processor_base
from streamflow.cwl.workflow import CWLWorkflow
from streamflow.workflow.combinator import (
    CartesianProductCombinator,
    DotProductCombinator,
)
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.step import CombinatorStep
from streamflow.workflow.token import (
    IterationTerminationToken,
    ListToken,
    TerminationToken,
)
from streamflow.workflow.transformer import ManyToOneTransformer
from tests.test_provenance import (
    create_and_run_step,
    inject_tokens,
    verify_dependency_tokens,
)
from tests.utils.cwl import get_cwl_parser
from tests.utils.workflow import (
    CWL_VERSION,
    create_deploy_step,
    create_schedule_step,
    create_workflow,
)


async def _run_many_to_one_transformer(
    many_to_one_cls: type[ManyToOneTransformer],
    token_lists: MutableSequence[MutableSequence[Token]],
    input_ports: MutableSequence[Port],
    out_port: Port,
    workflow: Workflow,
    context: StreamFlowContext,
):
    step = workflow.create_step(
        cls=many_to_one_cls,
        name=f"{utils.random_name()}-scatter-size-transformer",
    )
    step.add_output_port("test", out_port)
    for i, (token_list, input_port) in enumerate(
        zip(token_lists, input_ports, strict=True)
    ):
        await inject_tokens(token_list, input_port, context)
        step.add_input_port(f"param{i}", input_port)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    assert len(out_port.token_list) == len(token_lists)
    assert isinstance(out_port.token_list[-1], TerminationToken)


@pytest.mark.asyncio
async def test_cartesian_product_size_transformer(context: StreamFlowContext):
    """Test token provenance for CartesianProductSizeTransformer"""
    workflow, (in_port_1, in_port_2, out_port) = await create_workflow(
        context, num_port=3
    )
    token_1, token_2 = Token(5), Token(4)
    await _run_many_to_one_transformer(
        many_to_one_cls=CartesianProductSizeTransformer,
        token_lists=[[token_1], [token_2]],
        input_ports=[in_port_1, in_port_2],
        out_port=out_port,
        workflow=workflow,
        context=context,
    )

    assert out_port.token_list[0].value == (token_1.value * token_2.value)
    await verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=[token_1, token_2],
    )


@pytest.mark.asyncio
async def test_clone_transformer(context: StreamFlowContext):
    """Test token provenance for CloneTransformer"""
    workflow, (in_port, replicas_port, out_port) = await create_workflow(
        context, num_port=3
    )
    token_list = [Token("a")]
    size_token = Token(3)
    await size_token.save(context)
    replicas_port.put(size_token)
    replicas_port.put(TerminationToken())
    await create_and_run_step(
        context=context,
        workflow=workflow,
        in_port=in_port,
        out_port=out_port,
        step_cls=CloneTransformer,
        kwargs_step={
            "name": f"{utils.random_name()}-scatter-size-transformer",
            "replicas_port": replicas_port,
        },
        token_list=token_list,
    )

    # len(token_list) = N output tokens + 1 termination token
    assert len(out_port.token_list) == 4
    await verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=[token_list[0], size_token],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("step_cls", [DefaultTransformer, DefaultRetagTransformer])
async def test_default_transformer(context: StreamFlowContext, step_cls: type[Step]):
    """Test token provenance for DefaultTransformer and DefaultRetagTransformer"""
    workflow, (in_port, out_port) = await create_workflow(context)
    token_list = [Token("a")]
    await create_and_run_step(
        context=context,
        workflow=workflow,
        in_port=in_port,
        out_port=out_port,
        step_cls=step_cls,
        kwargs_step={
            "name": utils.random_name() + "-transformer",
            "default_port": in_port,
        }
        | (
            {"primary_port": "pci"}
            if issubclass(step_cls, DefaultRetagTransformer)
            else {}
        ),
        token_list=token_list,
    )

    # len(token_list) = N output tokens + 1 termination token
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=token_list,
    )


@pytest.mark.asyncio
async def test_dot_product_size_transformer(context: StreamFlowContext):
    """Test token provenance for DotProductSizeTransformer"""
    workflow, (in_port_1, in_port_2, out_port) = await create_workflow(
        context, num_port=3
    )
    token_1, token_2 = (Token(5) for _ in range(2))
    await _run_many_to_one_transformer(
        many_to_one_cls=DotProductSizeTransformer,
        token_lists=[[token_1], [token_2]],
        input_ports=[in_port_1, in_port_2],
        out_port=out_port,
        workflow=workflow,
        context=context,
    )
    assert out_port.token_list[0].value == token_1.value == token_2.value
    await verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=[token_1, token_2],
    )


@pytest.mark.asyncio
async def test_cwl_execute_step(context: StreamFlowContext):
    """Test token provenance for CWLExecuteStep"""
    workflow, (in_port_schedule, in_port, out_port) = await create_workflow(
        context, num_port=3
    )
    deploy_step = create_deploy_step(workflow)
    schedule_step = create_schedule_step(
        workflow,
        cls=CWLScheduleStep,
        deploy_steps=[deploy_step],
        hardware_requirement=CWLHardwareRequirement(cwl_version=CWL_VERSION),
    )
    in_port_name = "in-1"
    out_port_name = "out-1"
    token_value = "Hello"
    execute_step = workflow.create_step(
        cls=CWLExecuteStep,
        name=utils.random_name(),
        job_port=schedule_step.get_output_port(),
        recoverable=True,
    )
    execute_step.command = CWLCommand(
        step=execute_step,
        base_command=["echo"],
        processors=[CWLCommandTokenProcessor(name=in_port_name, expression=None)],
    )
    execute_step.add_output_port(
        name=out_port_name,
        port=out_port,
        output_processor=create_command_output_processor_base(
            port_name=out_port.name,
            workflow=cast(CWLWorkflow, workflow),
            port_target=None,
            port_type="string",
            cwl_element=get_cwl_parser(CWL_VERSION).CommandOutputParameter(
                type_="string"
            ),
            context={"hints": {}, "requirements": {}, "version": CWL_VERSION},
        ),
    )
    token_list = [Token(token_value)]

    execute_step.add_input_port(in_port_name, in_port)
    await inject_tokens(token_list, in_port, context)

    schedule_step.add_input_port(in_port_name, in_port_schedule)
    await inject_tokens(token_list, in_port_schedule, context)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    job_token = execute_step.get_input_port("__job__").token_list[0]
    await verify_dependency_tokens(
        token=job_token,
        port=execute_step.get_input_port("__job__"),
        context=context,
        expected_depender=[execute_step.get_output_port(out_port_name).token_list[0]],
        expected_dependee=[deploy_step.get_output_port().token_list[0], token_list[0]],
    )
    await verify_dependency_tokens(
        token=execute_step.get_output_port(out_port_name).token_list[0],
        port=execute_step.get_output_port(out_port_name),
        context=context,
        expected_dependee=list(job_token.value.inputs.values()) + [job_token],
    )


@pytest.mark.asyncio
async def test_cwl_token_transformer(context: StreamFlowContext):
    """Test token provenance for CWLTokenTransformer"""
    workflow, (in_port, out_port) = await create_workflow(context)
    port_name = "test"
    step_name = utils.random_name()
    token_list = [Token("a")]
    await create_and_run_step(
        context=context,
        workflow=workflow,
        in_port=in_port,
        out_port=out_port,
        step_cls=CWLTokenTransformer,
        kwargs_step={
            "name": step_name + "-transformer",
            "port_name": port_name,
            "processor": CWLTokenProcessor(
                name=step_name,
                workflow=cast(CWLWorkflow, workflow),
                token_type="string",
            ),
        },
        token_list=token_list,
        port_name=port_name,
    )
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=token_list,
    )


@pytest.mark.asyncio
async def test_value_from_transformer(context: StreamFlowContext):
    """Test token provenance for ValueFromTransformer"""
    workflow, (in_port, out_port) = await create_workflow(context)
    port_name = "test"
    token_list = [Token(10)]
    await create_and_run_step(
        context=context,
        workflow=workflow,
        in_port=in_port,
        out_port=out_port,
        step_cls=ValueFromTransformer,
        kwargs_step={
            "name": utils.random_name() + "-value-from-transformer",
            "processor": CWLTokenProcessor(
                name=in_port.name,
                workflow=cast(CWLWorkflow, workflow),
                token_type="long",
            ),
            "port_name": in_port.name,
            "full_js": True,
            "value_from": f"$(inputs.{port_name} + 1)",
        },
        token_list=token_list,
        port_name=port_name,
    )
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=token_list,
    )


@pytest.mark.asyncio
async def test_all_non_null_transformer(context: StreamFlowContext):
    """Test token provenance for AllNonNullTransformer"""
    workflow, (in_port, out_port) = await create_workflow(context)
    token_list = [ListToken([Token("a"), Token(None), Token("b")])]
    await create_and_run_step(
        context=context,
        workflow=workflow,
        in_port=in_port,
        out_port=out_port,
        step_cls=AllNonNullTransformer,
        kwargs_step={
            "name": utils.random_name() + "-transformer",
        },
        token_list=token_list,
    )
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=token_list,
    )


@pytest.mark.asyncio
async def test_first_non_null_transformer(context: StreamFlowContext):
    """Test token provenance for FirstNonNullTransformer"""
    workflow, (in_port, out_port) = await create_workflow(context)
    token_list = [ListToken([Token(None), Token("a")])]
    await create_and_run_step(
        context=context,
        workflow=workflow,
        in_port=in_port,
        out_port=out_port,
        step_cls=FirstNonNullTransformer,
        kwargs_step={
            "name": utils.random_name() + "-transformer",
        },
        token_list=token_list,
    )
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=token_list,
    )


@pytest.mark.asyncio
async def test_forward_transformer(context: StreamFlowContext):
    """Test token provenance for ForwardTransformer"""
    workflow, (in_port, out_port) = await create_workflow(context)
    token_list = [ListToken([Token("a")])]
    await create_and_run_step(
        context=context,
        workflow=workflow,
        in_port=in_port,
        out_port=out_port,
        step_cls=ForwardTransformer,
        kwargs_step={
            "name": utils.random_name() + "-transformer",
        },
        token_list=token_list,
    )
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=token_list,
    )


@pytest.mark.asyncio
async def test_list_to_element_transformer(context: StreamFlowContext):
    """Test token provenance for ListToElementTransformer"""
    workflow, (in_port, out_port) = await create_workflow(context)
    token_list = [ListToken([Token("a")])]
    await create_and_run_step(
        context=context,
        workflow=workflow,
        in_port=in_port,
        out_port=out_port,
        step_cls=ListToElementTransformer,
        kwargs_step={
            "name": utils.random_name() + "-transformer",
        },
        token_list=token_list,
    )
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=token_list,
    )


@pytest.mark.asyncio
async def test_only_non_null_transformer(context: StreamFlowContext):
    """Test token provenance for OnlyNonNullTransformer"""
    workflow, (in_port, out_port) = await create_workflow(context)
    token_list = [ListToken([Token(None), Token("a")])]
    await create_and_run_step(
        context=context,
        workflow=workflow,
        in_port=in_port,
        out_port=out_port,
        step_cls=OnlyNonNullTransformer,
        kwargs_step={
            "name": utils.random_name() + "-transformer",
        },
        token_list=token_list,
    )
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=token_list,
    )


@pytest.mark.asyncio
async def test_cwl_conditional_step(context: StreamFlowContext):
    """Test token provenance for CWLConditionalStep"""
    workflow, (in_port, out_port) = await create_workflow(context)
    port_name = "test"
    token_list = [ListToken([Token("a")])]
    await create_and_run_step(
        context=context,
        workflow=workflow,
        in_port=in_port,
        out_port=out_port,
        step_cls=CWLConditionalStep,
        kwargs_step={
            "name": utils.random_name() + "-when",
            "expression": f"$(inputs.{port_name}.length == 1)",
            "full_js": True,
        },
        token_list=token_list,
        port_name=port_name,
    )
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=token_list,
    )


@pytest.mark.asyncio
async def test_cwl_empty_scatter_conditional_step(context: StreamFlowContext):
    """Test token provenance for CWLEmptyScatterConditionalStep"""
    workflow, (in_port, out_port) = await create_workflow(context)
    port_name = "test"
    token_list = [ListToken([Token("a")])]
    await create_and_run_step(
        context=context,
        workflow=workflow,
        in_port=in_port,
        out_port=out_port,
        step_cls=CWLEmptyScatterConditionalStep,
        kwargs_step={
            "name": utils.random_name() + "-empty-scatter-condition",
            "scatter_method": "dotproduct",
        },
        token_list=token_list,
        port_name=port_name,
    )
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=token_list,
    )


@pytest.mark.asyncio
async def test_cwl_loop_conditional_step(context: StreamFlowContext):
    """Test token provenance for CWLLoopConditionalStep"""
    workflow, (in_port, out_port) = await create_workflow(context)
    port_name = "test"
    token_list = [ListToken([Token("a")])]
    await create_and_run_step(
        context=context,
        workflow=workflow,
        in_port=in_port,
        out_port=out_port,
        step_cls=CWLLoopConditionalStep,
        kwargs_step={
            "name": utils.random_name() + "-when",
            "expression": f"$(inputs.{port_name}.length == 1)",
            "full_js": True,
        },
        token_list=token_list,
        port_name=port_name,
    )
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=token_list,
    )


@pytest.mark.asyncio
async def test_cwl_transfer_step(context: StreamFlowContext):
    """Test token provenance for CWLTransferStep"""
    workflow, (in_port, out_port) = await create_workflow(context)
    deploy_step = create_deploy_step(workflow)
    schedule_step = create_schedule_step(workflow, deploy_steps=[deploy_step])
    port_name = "test"
    token_list = [Token("a")]
    transfer_step = await create_and_run_step(
        context=context,
        workflow=workflow,
        in_port=in_port,
        out_port=out_port,
        step_cls=CWLTransferStep,
        kwargs_step={
            "name": posixpath.join(utils.random_name(), "__transfer__", port_name),
            "job_port": schedule_step.get_output_port(),
            "writable": True,
        },
        token_list=token_list,
        port_name=port_name,
    )
    job_token = transfer_step.get_input_port("__job__").token_list[0]
    await context.scheduler.notify_status(job_token.value.name, Status.COMPLETED)
    token_list.append(job_token)
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=token_list,
    )


@pytest.mark.asyncio
async def test_cwl_input_injector_step(context: StreamFlowContext):
    """Test token provenance for CWLInputInjectorStep"""
    workflow, (in_port, out_port) = await create_workflow(context)
    deploy_step = create_deploy_step(workflow)
    schedule_step = create_schedule_step(workflow, deploy_steps=[deploy_step])
    token_list = [Token("a")]
    injector = await create_and_run_step(
        context=context,
        workflow=workflow,
        in_port=in_port,
        out_port=out_port,
        step_cls=CWLInputInjectorStep,
        kwargs_step={
            "name": posixpath.join(utils.random_name(), "-injector"),
            "job_port": schedule_step.get_output_port(),
        },
        token_list=token_list,
    )
    job_token = injector.get_input_port("__job__").token_list[0]
    await context.scheduler.notify_status(job_token.value.name, Status.COMPLETED)
    token_list.append(job_token)
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=token_list,
    )


@pytest.mark.asyncio
async def test_empty_scatter_conditional_step(context: StreamFlowContext):
    """Test token provenance for CWLEmptyScatterConditionalStep"""
    workflow, (in_port, out_port) = await create_workflow(context)
    token_list = [ListToken([Token(i), Token(i * 100)]) for i in range(1, 5)]
    await create_and_run_step(
        context=context,
        workflow=workflow,
        in_port=in_port,
        out_port=out_port,
        step_cls=CWLEmptyScatterConditionalStep,
        kwargs_step={
            "name": utils.random_name() + "-empty-scatter-condition",
            "scatter_method": "dotproduct",
        },
        token_list=token_list,
    )

    assert len(out_port.token_list) == 5
    for in_token, out_token in zip(
        in_port.token_list[:-1], out_port.token_list[:-1], strict=True
    ):
        await verify_dependency_tokens(
            token=out_token,
            port=out_port,
            context=context,
            expected_dependee=[in_token],
        )


@pytest.mark.asyncio
async def test_list_merge_combinator(context: StreamFlowContext):
    """Test token provenance for ListMergeCombinator"""
    workflow, (in_port, out_port) = await create_workflow(context)
    port_name = "test"
    step_name = utils.random_name()
    step = workflow.create_step(
        cls=CombinatorStep,
        name=step_name + "-combinator",
        combinator=ListMergeCombinator(
            name=utils.random_name(),
            workflow=cast(CWLWorkflow, workflow),
            input_names=[port_name],
            output_name=port_name,
            flatten=False,
        ),
    )
    port_name = "test"
    step.add_input_port(port_name, in_port)
    step.add_output_port(port_name, out_port)

    list_token = [ListToken([Token("a"), Token("b")])]
    await inject_tokens(list_token, in_port, context)

    step.combinator.add_item(port_name)
    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=list_token,
    )


@pytest.mark.asyncio
async def test_loop_value_from_transformer(context: StreamFlowContext):
    """Test token provenance for LoopValueFromTransformer"""
    workflow, (in_port, out_port, loop_port) = await create_workflow(
        context, num_port=3
    )
    port_name = "test"
    transformer = workflow.create_step(
        cls=LoopValueFromTransformer,
        name=f"{utils.random_name()}-loop-value-from-transformer",
        processor=CWLTokenProcessor(
            name=in_port.name,
            workflow=cast(CWLWorkflow, workflow),
            token_type="long",
        ),
        port_name=port_name,
        full_js=True,
        value_from=f"$(inputs.{port_name} + 1)",
    )
    transformer.add_loop_input_port(port_name, in_port)
    transformer.add_loop_source_port(port_name, loop_port)
    transformer.add_output_port(port_name, out_port)

    token_list = [Token(10)]
    await inject_tokens(token_list, in_port, context)
    await inject_tokens(token_list, loop_port, context)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    assert len(transformer.get_output_port(port_name).token_list) == 2
    await verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=token_list,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("step_cls", [CWLLoopOutputAllStep, CWLLoopOutputLastStep])
async def test_cwl_loop_output(context: StreamFlowContext, step_cls: type[Step]):
    """Test token provenance for CWLLoopOutput"""
    workflow, (in_port, out_port) = await create_workflow(context)

    step = workflow.create_step(
        cls=step_cls,
        name=posixpath.join(utils.random_name(), "-loop-output"),
    )
    port_name = "test"
    step.add_input_port(port_name, in_port)
    step.add_output_port(port_name, out_port)
    token_list = [
        Token("b", tag="0.1"),
        IterationTerminationToken("0.1"),
    ]
    await inject_tokens(token_list, in_port, context)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=[token_list[0]],
    )


@pytest.mark.asyncio
async def test_nested_crossproduct_combinator(context: StreamFlowContext):
    """Test token provenance for CWL nested_crossproduct feature"""
    workflow, (in_port_1, in_port_2, out_port_1, out_port_2) = await create_workflow(
        context, num_port=4
    )
    port_name_1 = "echo_in1"
    port_name_2 = "echo_in2"
    prefix_name = "/step1-scatter"
    step_name = prefix_name + "-combinator"
    combinator = DotProductCombinator(
        workflow=workflow, name=prefix_name + "-dot-product-combinator"
    )
    c1 = CartesianProductCombinator(name=step_name, workflow=workflow)
    c1.add_item(port_name_1)
    c1.add_item(port_name_2)
    items = c1.get_items(False)
    combinator.add_combinator(
        c1,
        items,
    )

    step = workflow.create_step(
        cls=CombinatorStep,
        name=step_name,
        combinator=combinator,
    )

    step.add_input_port(port_name_1, in_port_1)
    step.add_input_port(port_name_2, in_port_2)
    step.add_output_port(port_name_1, out_port_1)
    step.add_output_port(port_name_2, out_port_2)

    list_token_1 = [
        ListToken([Token("a"), Token("b")], tag="0.0"),
        ListToken([Token("c"), Token("d")], tag="0.1"),
    ]
    await inject_tokens(list_token_1, in_port_1, context)

    list_token_2 = [
        ListToken([Token("1"), Token("2")], tag="0.0"),
        ListToken([Token("3"), Token("4")], tag="0.1"),
    ]
    await inject_tokens(list_token_2, in_port_2, context)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    nested_crossproduct_1 = [[t1, t2] for t2 in list_token_2 for t1 in list_token_1]
    nested_crossproduct_2 = [[t1, t2] for t1 in list_token_1 for t2 in list_token_2]

    # The tokens id produced by combinators depend on the order of input tokens.
    # The use of the alternative_expected_dependee parameter is necessary
    # For example:
    # input port_1 token: (id, tag, value)
    #   (  3, 0.0, ['a', 'b'] )
    #   (  6, 0.1, ['c', 'd'] )
    # input port_2 token: (id, tag, value)
    #   (  9, 0.0, ['1', '2'] )
    #   ( 12, 0.1, ['3', '4'] )

    # case #1: port_1 input tokens arrive first
    # - output port_1 token: (id, tag, value)
    #   ( 13, 0.0.0, ['a', 'b'] )
    #   ( 15, 0.0.1, ['a', 'b'] )
    #   ( 17, 0.1.0, ['c', 'd'] )
    #   ( 19, 0.1.1, ['c', 'd'] )
    # - output port_2 token: (id, tag, value)
    #   ( 14, 0.0.0, ['1', '2'] )
    #   ( 16, 0.0.1, ['3', '4'] )
    #   ( 18, 0.1.0, ['1', '2'] )
    #   ( 20, 0.1.1, ['3', '4'] )
    # - provenance token in port_1: { output token id : input token id list }
    #   { 13 : [3, 9], 15 : [3, 12], 17 : [6, 9], 19 : [6, 12] }
    # - provenance token in port_2: { output token id : input token id list }
    #   { 14 : [3, 9], 16 : [3, 12], 18 : [6, 9], 20 : [6, 12] }

    # case #2: port_2 input tokens arrive first
    # - output port_1 token: (id, tag, value)
    #   ( 13, 0.0.0, ['a', 'b'] )
    #   ( 15, 0.1.0, ['c', 'd'] )
    #   ( 17, 0.0.1, ['a', 'b'] )
    #   ( 19, 0.1.1, ['c', 'd'] )
    # - output port_2 token: (id, tag, value)
    #   ( 14, 0.0.0, ['1', '2'] )
    #   ( 16, 0.1.0, ['1', '2'] )
    #   ( 18, 0.0.1, ['3', '4'] )
    #   ( 20, 0.1.1, ['3', '4'] )
    # - provenance token in port_1: { output token id : input token id list }
    #   { 13 : [3, 9], 15 : [6, 9], 17 : [3, 12], 19 : [6, 12] }
    # - provenance token in port_2: { output token id : input token id list }
    #   { 14 : [3, 9], 16 : [6, 9], 18 : [3, 12], 20 : [6, 12] }

    # check port_1 outputs
    assert len(out_port_1.token_list) == 5
    for i, out_token in enumerate(out_port_1.token_list[:-1]):
        await verify_dependency_tokens(
            token=out_token,
            port=out_port_1,
            context=context,
            expected_dependee=nested_crossproduct_1[i],
            alternative_expected_dependee=nested_crossproduct_2[i],
        )

    # check port_2 outputs
    assert len(out_port_2.token_list) == 5
    for i, out_token in enumerate(out_port_2.token_list[:-1]):
        await verify_dependency_tokens(
            token=out_token,
            port=out_port_2,
            context=context,
            expected_dependee=nested_crossproduct_1[i],
            alternative_expected_dependee=nested_crossproduct_2[i],
        )
