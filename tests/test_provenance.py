from __future__ import annotations

from typing import cast

import pytest

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Status, Token
from streamflow.workflow.combinator import (
    CartesianProductCombinator,
    DotProductCombinator,
    LoopCombinator,
    LoopTerminationCombinator,
)
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.step import (
    CombinatorStep,
    GatherStep,
    LoopCombinatorStep,
    ScatterStep,
    ScheduleStep,
)
from streamflow.workflow.token import (
    IterationTerminationToken,
    ListToken,
    TerminationToken,
)
from tests.utils.utils import (
    create_and_run_step,
    inject_tokens,
    verify_dependency_tokens,
)
from tests.utils.workflow import (
    create_deploy_step,
    create_schedule_step,
    create_workflow,
)


@pytest.mark.asyncio
async def test_scatter_step(context: StreamFlowContext):
    """Test token provenance for ScatterStep"""
    workflow, (in_port, out_port) = await create_workflow(context)
    token_list = [ListToken([Token("a"), Token("b"), Token("c")])]
    step = cast(
        ScatterStep,
        await create_and_run_step(
            context=context,
            workflow=workflow,
            in_port=in_port,
            out_port=out_port,
            step_cls=ScatterStep,
            kwargs_step={"name": utils.random_name() + "-scatter"},
            token_list=token_list,
        ),
    )
    assert len(out_port.token_list) == 4

    size_port = step.get_size_port()
    assert len(size_port.token_list) == 2
    assert isinstance(out_port.token_list[-1], TerminationToken)
    assert isinstance(size_port.token_list[-1], TerminationToken)

    for curr_token in out_port.token_list[:-1]:
        await verify_dependency_tokens(
            token=curr_token,
            port=out_port,
            context=context,
            expected_dependee=[in_port.token_list[0]],
        )
    await verify_dependency_tokens(
        token=size_port.token_list[0],
        port=size_port,
        context=context,
        expected_dependee=[in_port.token_list[0]],
    )


@pytest.mark.asyncio
async def test_deploy_step(context: StreamFlowContext):
    """Test token provenance for DeployStep"""
    workflow, _ = await create_workflow(context, num_port=0)
    step = create_deploy_step(workflow)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    assert len(step.get_output_port().token_list) == 2
    await verify_dependency_tokens(
        token=step.get_output_port().token_list[0],
        port=step.get_output_port(),
        context=context,
    )


@pytest.mark.asyncio
async def test_schedule_step(context: StreamFlowContext):
    """Test token provenance for ScheduleStep"""
    workflow, _ = await create_workflow(context, num_port=0)
    deploy_step = create_deploy_step(workflow)
    schedule_step = create_schedule_step(
        workflow, cls=ScheduleStep, deploy_steps=[deploy_step]
    )

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    job_token = schedule_step.get_output_port("__job__").token_list[0]
    await context.scheduler.notify_status(job_token.value.name, Status.COMPLETED)

    await verify_dependency_tokens(
        token=deploy_step.get_output_port().token_list[0],
        port=deploy_step.get_output_port(),
        context=context,
        expected_depender=[job_token],
    )
    await verify_dependency_tokens(
        token=job_token,
        port=schedule_step.get_output_port("__job__"),
        context=context,
        expected_dependee=[deploy_step.get_output_port().token_list[0]],
    )


@pytest.mark.asyncio
async def test_gather_step(context: StreamFlowContext):
    """Test token provenance for GatherStep"""
    workflow, (in_port, out_port, size_port) = await create_workflow(
        context, num_port=3
    )
    base_tag = "0"
    token_list = [Token(i, tag=f"{base_tag}.{i}") for i in range(5)]
    size_token = Token(len(token_list), tag=base_tag)
    await size_token.save(context)
    size_port.put(size_token)
    size_port.put(TerminationToken())
    await create_and_run_step(
        context=context,
        workflow=workflow,
        in_port=in_port,
        out_port=out_port,
        step_cls=GatherStep,
        kwargs_step={"name": utils.random_name() + "-gather", "size_port": size_port},
        token_list=token_list,
    )
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=[*token_list, size_token],
    )


@pytest.mark.asyncio
async def test_gather_step_no_size(context: StreamFlowContext):
    """Test token provenance for GatherStep without size token"""
    workflow, (in_port, out_port, size_port) = await create_workflow(
        context, num_port=3
    )
    base_tag = "0"
    token_list = [Token(i, tag=f"{base_tag}.{i}") for i in range(5)]

    # TerminationToken is necessary
    size_port.put(TerminationToken())
    gather_step = await create_and_run_step(
        context=context,
        workflow=workflow,
        in_port=in_port,
        out_port=out_port,
        step_cls=GatherStep,
        kwargs_step={"name": utils.random_name() + "-gather", "size_port": size_port},
        token_list=token_list,
    )
    assert len(out_port.token_list) == 2
    size_token = cast(GatherStep, gather_step).size_map[base_tag]
    await verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=[*token_list, size_token],
    )


@pytest.mark.asyncio
async def test_combinator_step_dot_product(context: StreamFlowContext):
    """Test token provenance for DotProductCombinator"""
    workflow, (in_port, out_port, in_port_2, out_port_2) = await create_workflow(
        context, num_port=4
    )
    step = workflow.create_step(
        cls=CombinatorStep,
        name=f"{utils.random_name()}-combinator",
        combinator=DotProductCombinator(name=utils.random_name(), workflow=workflow),
    )
    port_name = "test"
    step.add_input_port(port_name, in_port)
    step.add_output_port(port_name, out_port)

    port_name_2 = f"{port_name}_2"
    step.add_input_port(port_name_2, in_port_2)
    step.add_output_port(port_name_2, out_port_2)

    await workflow.save(context)
    list_token = ListToken([Token("fst"), Token("snd")])
    await inject_tokens([list_token], in_port, context)
    step.combinator.add_item(port_name)

    tt = Token("4")
    await inject_tokens([tt], in_port_2, context)
    step.combinator.add_item(port_name_2)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=[list_token, tt],
    )
    assert len(out_port_2.token_list) == 2
    await verify_dependency_tokens(
        token=out_port_2.token_list[0],
        port=out_port_2,
        context=context,
        expected_dependee=[list_token, tt],
    )


@pytest.mark.asyncio
async def test_combinator_step_cartesian_product(context: StreamFlowContext):
    """Test token provenance for CartesianProductCombinator"""
    workflow, (in_port, out_port, in_port_2, out_port_2) = await create_workflow(
        context, num_port=4
    )
    step = workflow.create_step(
        cls=CombinatorStep,
        name=f"{utils.random_name()}-combinator",
        combinator=CartesianProductCombinator(
            name=utils.random_name(), workflow=workflow
        ),
    )
    port_name = "test"
    step.add_input_port(port_name, in_port)
    step.add_output_port(port_name, out_port)

    port_name_2 = f"{port_name}_2"
    step.add_input_port(port_name_2, in_port_2)
    step.add_output_port(port_name_2, out_port_2)

    await workflow.save(context)
    token_list = [ListToken([Token("a"), Token("b")])]
    await inject_tokens(token_list, in_port, context)
    step.combinator.add_item(port_name)

    token_list_2 = [Token("4")]
    await inject_tokens(token_list_2, in_port_2, context)
    step.combinator.add_item(port_name_2)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=[token_list[0], token_list_2[0]],
    )

    assert len(out_port_2.token_list) == 2
    await verify_dependency_tokens(
        token=out_port_2.token_list[0],
        port=out_port_2,
        context=context,
        expected_dependee=[token_list[0], token_list_2[0]],
    )


@pytest.mark.asyncio
async def test_loop_combinator_step(context: StreamFlowContext):
    """Test token provenance for LoopCombinator"""
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
    tag = "0.0"
    token_list = [
        ListToken([Token("a"), Token("b")], tag=tag),
        IterationTerminationToken(tag),
    ]
    await inject_tokens(token_list, in_port, context)
    step.combinator.add_item(port_name)

    token_list_2 = [Token("4", tag=tag), IterationTerminationToken(tag=tag)]
    await inject_tokens(token_list_2, in_port_2, context)
    step.combinator.add_item(port_name_2)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=[token_list[0], token_list_2[0]],
    )


@pytest.mark.asyncio
async def test_loop_termination_combinator(context: StreamFlowContext):
    """Test token provenance for LoopTerminationCombinator"""
    workflow, (in_port, out_port) = await create_workflow(context, num_port=2)
    step_name = utils.random_name()
    loop_terminator_combinator = LoopTerminationCombinator(
        workflow=workflow, name=step_name + "-loop-termination-combinator"
    )
    loop_terminator_step = workflow.create_step(
        cls=CombinatorStep,
        name=step_name + "-loop-terminator",
        combinator=loop_terminator_combinator,
    )
    port_name = "test"
    loop_terminator_step.add_input_port(port_name, in_port)
    loop_terminator_step.add_output_port(port_name, out_port)
    loop_terminator_combinator.add_output_item(port_name)
    loop_terminator_combinator.add_item(port_name)

    list_token = [
        ListToken([Token("a"), Token("b")], tag="0.0"),
        ListToken([Token("c"), Token("d")], tag="0.1"),
    ]
    await inject_tokens(list_token, in_port, context)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    assert len(out_port.token_list) == 3
    for out_token, in_token in zip(out_port.token_list[:-1], list_token, strict=True):
        await verify_dependency_tokens(
            token=out_token,
            port=out_port,
            context=context,
            expected_dependee=[in_token],
        )
