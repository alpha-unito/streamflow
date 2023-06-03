import posixpath

import pytest

from streamflow.cwl.combinator import ListMergeCombinator
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.step import CombinatorStep
from streamflow.workflow.token import (
    ListToken,
    IterationTerminationToken,
)

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Token, Status

from streamflow.cwl.step import (
    CWLTransferStep,
    CWLConditionalStep,
    CWLInputInjectorStep,
    CWLEmptyScatterConditionalStep,
    CWLLoopOutputAllStep,  # CWLLoopOutputLastStep,  # LoopOutputStep
)
from streamflow.cwl.processor import CWLTokenProcessor

from streamflow.cwl.transformer import (
    DefaultTransformer,
    DefaultRetagTransformer,
    CWLTokenTransformer,
    ValueFromTransformer,
    AllNonNullTransformer,
    FirstNonNullTransformer,
    ForwardTransformer,
    ListToElementTransformer,
    OnlyNonNullTransformer,
    LoopValueFromTransformer,
)

from tests.test_provenance import (
    verify_dependency_tokens,
    _create_deploy_step,
    _create_schedule_step,
    _general_test,
    _create_workflow,
    _put_tokens,
)


@pytest.mark.asyncio
async def test_default_transformer(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    token_list = [Token("a")]
    await _general_test(
        context,
        workflow,
        in_port,
        out_port,
        DefaultTransformer,
        {"name": utils.random_name() + "-transformer", "default_port": in_port},
        token_list,
    )

    # len(token_list) = N output tokens + 1 termination token
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        out_port.token_list[0], out_port, (), token_list, context
    )


@pytest.mark.asyncio
async def test_default_retag_transformer(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    token_list = [Token("a")]
    await _general_test(
        context,
        workflow,
        in_port,
        out_port,
        DefaultRetagTransformer,
        {"name": utils.random_name() + "-transformer", "default_port": in_port},
        token_list,
    )
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        out_port.token_list[0], out_port, (), token_list, context
    )


@pytest.mark.asyncio
async def test_cwl_token_transformer(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    port_name = "test"
    step_name = utils.random_name()
    token_list = [Token("a")]
    await _general_test(
        context,
        workflow,
        in_port,
        out_port,
        CWLTokenTransformer,
        {
            "name": step_name + "-transformer",
            "port_name": port_name,
            "processor": CWLTokenProcessor(
                name=step_name,
                workflow=workflow,
            ),
        },
        token_list,
        port_name,
    )
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        out_port.token_list[0], out_port, (), token_list, context
    )


@pytest.mark.asyncio
async def test_value_from_transformer(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    port_name = "test"
    token_list = [Token(10)]
    await _general_test(
        context,
        workflow,
        in_port,
        out_port,
        ValueFromTransformer,
        {
            "name": utils.random_name() + "-value-from-transformer",
            "processor": CWLTokenProcessor(
                name=in_port.name,
                workflow=workflow,
            ),
            "port_name": in_port.name,
            "full_js": True,
            "value_from": f"$(inputs.{port_name} + 1)",
        },
        token_list,
        port_name,
    )
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        out_port.token_list[0], out_port, (), token_list, context
    )


@pytest.mark.asyncio
async def test_all_non_null_transformer(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    token_list = [ListToken([Token("a"), Token(None), Token("b")])]
    await _general_test(
        context,
        workflow,
        in_port,
        out_port,
        AllNonNullTransformer,
        {
            "name": utils.random_name() + "-transformer",
        },
        token_list,
    )
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        out_port.token_list[0], out_port, (), token_list, context
    )


@pytest.mark.asyncio
async def test_first_non_null_transformer(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    token_list = [ListToken([Token(None), Token("a")])]
    await _general_test(
        context,
        workflow,
        in_port,
        out_port,
        FirstNonNullTransformer,
        {
            "name": utils.random_name() + "-transformer",
        },
        token_list,
    )
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        out_port.token_list[0], out_port, (), token_list, context
    )


@pytest.mark.asyncio
async def test_forward_transformer(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    token_list = [ListToken([Token("a")])]
    await _general_test(
        context,
        workflow,
        in_port,
        out_port,
        ForwardTransformer,
        {
            "name": utils.random_name() + "-transformer",
        },
        token_list,
    )
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        out_port.token_list[0], out_port, (), token_list, context
    )


@pytest.mark.asyncio
async def test_list_to_element_transformer(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    token_list = [ListToken([Token("a")])]
    await _general_test(
        context,
        workflow,
        in_port,
        out_port,
        ListToElementTransformer,
        {
            "name": utils.random_name() + "-transformer",
        },
        token_list,
    )
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        out_port.token_list[0], out_port, (), token_list, context
    )


@pytest.mark.asyncio
async def test_only_non_null_transformer(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    token_list = [ListToken([Token(None), Token("a")])]
    await _general_test(
        context,
        workflow,
        in_port,
        out_port,
        OnlyNonNullTransformer,
        {
            "name": utils.random_name() + "-transformer",
        },
        token_list,
    )
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        out_port.token_list[0], out_port, (), token_list, context
    )


@pytest.mark.asyncio
async def test_cwl_conditional_step(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    port_name = "test"
    token_list = [ListToken([Token("a")])]
    await _general_test(
        context,
        workflow,
        in_port,
        out_port,
        CWLConditionalStep,
        {
            "name": utils.random_name() + "-when",
            "expression": f"$(inputs.{port_name}.length == 1)",
            "full_js": True,
        },
        token_list,
        port_name=port_name,
    )
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        out_port.token_list[0], out_port, (), token_list, context
    )


@pytest.mark.asyncio
async def test_transfer_step(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    deploy_step = _create_deploy_step(workflow)
    schedule_step = _create_schedule_step(workflow, deploy_step)
    port_name = "test"
    token_list = [Token("a")]
    transfer_step = await _general_test(
        context,
        workflow,
        in_port,
        out_port,
        CWLTransferStep,
        {
            "name": posixpath.join(utils.random_name(), "__transfer__", port_name),
            "job_port": schedule_step.get_output_port(),
        },
        token_list,
        port_name=port_name,
    )
    job_token = transfer_step.get_input_port("__job__").token_list[0]
    await context.scheduler.notify_status(job_token.value.name, Status.COMPLETED)
    token_list.append(job_token)
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        out_port.token_list[0], out_port, (), token_list, context
    )


@pytest.mark.asyncio
async def test_cwl_input_injector_step(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    deploy_step = _create_deploy_step(workflow)
    schedule_step = _create_schedule_step(workflow, deploy_step)
    token_list = [Token("a")]
    injector = await _general_test(
        context,
        workflow,
        in_port,
        out_port,
        CWLInputInjectorStep,
        {
            "name": posixpath.join(utils.random_name(), "-injector"),
            "job_port": schedule_step.get_output_port(),
        },
        token_list,
    )
    job_token = injector.get_input_port("__job__").token_list[0]
    await context.scheduler.notify_status(job_token.value.name, Status.COMPLETED)
    token_list.append(job_token)
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        out_port.token_list[0], out_port, (), token_list, context
    )


@pytest.mark.asyncio
async def test_empty_scatter_conditional_step(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    token_list = [ListToken([Token(i), Token(i * 100)]) for i in range(1, 5)]
    await _general_test(
        context,
        workflow,
        in_port,
        out_port,
        CWLEmptyScatterConditionalStep,
        {
            "name": utils.random_name() + "-empty-scatter-condition",
            "scatter_method": "dotproduct",
        },
        token_list,
    )

    assert len(out_port.token_list) == 5
    for in_token, out_token in zip(in_port.token_list[:-1], out_port.token_list[:-1]):
        await verify_dependency_tokens(out_token, out_port, (), [in_token], context)


@pytest.mark.asyncio
async def test_list_merge_combinator(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    port_name = "test"
    step_name = utils.random_name()
    step = workflow.create_step(
        cls=CombinatorStep,
        name=step_name + "-combinator",
        combinator=ListMergeCombinator(
            name=utils.random_name(),
            workflow=workflow,
            input_names=[port_name],
            output_name=port_name,
            flatten=False,
        ),
    )
    port_name = "test"
    step.add_input_port(port_name, in_port)
    step.add_output_port(port_name, out_port)

    list_token = [ListToken([Token("a"), Token("b")])]
    await _put_tokens(list_token, in_port, context)

    step.combinator.add_item(port_name)
    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        out_port.token_list[0], out_port, [], list_token, context
    )


@pytest.mark.asyncio
async def test_loop_value_from_transformer(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)

    name = utils.random_name()
    port_name = "test"
    transformer = workflow.create_step(
        cls=LoopValueFromTransformer,
        name=name + "-loop-value-from-transformer",
        processor=CWLTokenProcessor(
            name=in_port.name,
            workflow=workflow,
        ),
        port_name=port_name,
        full_js=True,
        value_from=f"$(inputs.{port_name} + 1)",
    )
    loop_port = workflow.create_port()
    transformer.add_loop_input_port(port_name, in_port)
    transformer.add_loop_source_port(port_name, loop_port)
    transformer.add_output_port(port_name, out_port)

    token_list = [Token(10)]
    await _put_tokens(token_list, in_port, context)
    await _put_tokens(token_list, loop_port, context)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    assert len(transformer.get_output_port(port_name).token_list) == 2
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        token_list,
        context,
    )


@pytest.mark.asyncio
async def test_cwl_loop_output_all_step(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)

    step = workflow.create_step(
        cls=CWLLoopOutputAllStep,
        name=posixpath.join(utils.random_name(), "-loop-output"),
    )
    port_name = "test"
    step.add_input_port(port_name, in_port)
    step.add_output_port(port_name, out_port)
    tag = "0.1"
    list_token = [
        ListToken([Token("a"), Token("b")], tag=tag),
        IterationTerminationToken(tag),
    ]

    await _put_tokens(list_token, in_port, context)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        out_port.token_list[0], out_port, [], [list_token[0]], context
    )
