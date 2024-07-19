from __future__ import annotations

from typing import Any, MutableMapping, MutableSequence, cast

import pytest

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Port, Status, Step, Token, Workflow
from streamflow.cwl.command import CWLCommand, CWLCommandTokenProcessor
from streamflow.cwl.translator import _create_command_output_processor_base
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.persistence.utils import load_depender_tokens, load_dependee_tokens
from streamflow.workflow.combinator import (
    CartesianProductCombinator,
    DotProductCombinator,
    LoopCombinator,
    LoopTerminationCombinator,
)
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.step import (
    CombinatorStep,
    ExecuteStep,
    GatherStep,
    LoopCombinatorStep,
    ScatterStep,
)
from streamflow.workflow.token import (
    IterationTerminationToken,
    ListToken,
    TerminationToken,
)
from tests.utils.workflow import (
    create_workflow,
    create_deploy_step,
    create_schedule_step,
)


def _contains_id(id_: int, token_list: MutableSequence[Token]) -> bool:
    for t in token_list:
        if id_ == t.persistent_id:
            return True
    return False


async def _general_test(
    context: StreamFlowContext,
    workflow: Workflow,
    in_port: Port,
    out_port: Port,
    step_cls: type[Step],
    kwargs_step: MutableMapping[str, Any],
    token_list: MutableSequence[Token],
    port_name: str = "test",
    save_input_token: bool = True,
) -> Step:
    step = workflow.create_step(cls=step_cls, **kwargs_step)
    step.add_input_port(port_name, in_port)
    step.add_output_port(port_name, out_port)

    await _put_tokens(token_list, in_port, context, save_input_token)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    return step


async def _put_tokens(
    token_list: MutableSequence[Token],
    in_port: Port,
    context: StreamFlowContext,
    save_input_token: bool = True,
) -> None:
    for t in token_list:
        if save_input_token and not isinstance(t, TerminationToken):
            await t.save(context, in_port.persistent_id)
        in_port.put(t)
    in_port.put(TerminationToken())


async def _verify_dependency_tokens(
    token: Token,
    port: Port,
    context: StreamFlowContext,
    expected_depender: MutableSequence[Token] | None = None,
    expected_dependee: MutableSequence[Token] | None = None,
    alternative_expected_dependee: MutableSequence[Token] | None = None,
):
    loading_context = DefaultDatabaseLoadingContext()
    expected_depender = expected_depender or []
    expected_dependee = expected_dependee or []

    token_reloaded = await context.database.get_token(token_id=token.persistent_id)
    assert token_reloaded["port"] == port.persistent_id

    depender_list = await load_depender_tokens(
        token.persistent_id, context, loading_context
    )
    print(
        "depender:",
        {token.persistent_id: [t.persistent_id for t in depender_list]},
    )
    print(
        "expected_depender",
        [t.persistent_id for t in expected_depender],
    )
    assert len(depender_list) == len(expected_depender)
    for t1 in depender_list:
        assert _contains_id(t1.persistent_id, expected_depender)

    dependee_list = await load_dependee_tokens(
        token.persistent_id, context, loading_context
    )
    print(
        "dependee:",
        {token.persistent_id: [t.persistent_id for t in dependee_list]},
    )
    print(
        "expected_dependee",
        [t.persistent_id for t in expected_dependee],
    )
    try:
        assert len(dependee_list) == len(expected_dependee)
        for t1 in dependee_list:
            assert _contains_id(t1.persistent_id, expected_dependee)
    except AssertionError as err:
        if alternative_expected_dependee is None:
            raise err
        else:
            assert len(dependee_list) == len(alternative_expected_dependee)
            for t1 in dependee_list:
                assert _contains_id(t1.persistent_id, alternative_expected_dependee)


@pytest.mark.asyncio
async def test_scatter_step(context: StreamFlowContext):
    """Test token provenance for ScatterStep"""
    workflow, (in_port, out_port) = await create_workflow(context)
    token_list = [ListToken([Token("a"), Token("b"), Token("c")])]
    step = cast(
        ScatterStep,
        await _general_test(
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
        await _verify_dependency_tokens(
            token=curr_token,
            port=out_port,
            context=context,
            expected_dependee=[in_port.token_list[0]],
        )
    await _verify_dependency_tokens(
        token=size_port.token_list[0],
        port=size_port,
        context=context,
        expected_dependee=[in_port.token_list[0]],
    )


@pytest.mark.asyncio
async def test_deploy_step(context: StreamFlowContext):
    """Test token provenance for DeployStep"""
    workflow = (await create_workflow(context, num_port=0))[0]
    step = create_deploy_step(workflow)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    assert len(step.get_output_port().token_list) == 2
    await _verify_dependency_tokens(
        token=step.get_output_port().token_list[0],
        port=step.get_output_port(),
        context=context,
    )


@pytest.mark.asyncio
async def test_schedule_step(context: StreamFlowContext):
    """Test token provenance for ScheduleStep"""
    workflow = (await create_workflow(context, num_port=0))[0]
    deploy_step = create_deploy_step(workflow)
    schedule_step = create_schedule_step(workflow, [deploy_step])

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    job_token = schedule_step.get_output_port("__job__").token_list[0]
    await context.scheduler.notify_status(job_token.value.name, Status.COMPLETED)

    await _verify_dependency_tokens(
        token=deploy_step.get_output_port().token_list[0],
        port=deploy_step.get_output_port(),
        context=context,
        expected_depender=[job_token],
    )
    await _verify_dependency_tokens(
        token=job_token,
        port=schedule_step.get_output_port("__job__"),
        context=context,
        expected_dependee=[deploy_step.get_output_port().token_list[0]],
    )


@pytest.mark.asyncio
async def test_execute_step(context: StreamFlowContext):
    """Test token provenance for ExecuteStep"""
    workflow, (in_port_schedule, in_port, out_port) = await create_workflow(
        context, num_port=3
    )
    deploy_step = create_deploy_step(workflow)
    schedule_step = create_schedule_step(workflow, [deploy_step])

    in_port_name = "in-1"
    out_port_name = "out-1"
    token_value = "Hello"

    execute_step = workflow.create_step(
        cls=ExecuteStep,
        name=utils.random_name(),
        job_port=schedule_step.get_output_port(),
    )
    execute_step.command = CWLCommand(
        step=execute_step,
        base_command=["echo"],
        processors=[CWLCommandTokenProcessor(name=in_port_name, expression=None)],
    )
    execute_step.add_output_port(
        name=out_port_name,
        port=out_port,
        output_processor=_create_command_output_processor_base(
            port_name=out_port.name,
            workflow=workflow,
            port_target=None,
            port_type="string",
            cwl_element={},
            context={"hints": {}, "requirements": {}},
        ),
    )
    token_list = [Token(token_value)]

    execute_step.add_input_port(in_port_name, in_port)
    await _put_tokens(token_list, in_port, context)

    schedule_step.add_input_port(in_port_name, in_port_schedule)
    await _put_tokens(token_list, in_port_schedule, context)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    job_token = execute_step.get_input_port("__job__").token_list[0]
    await _verify_dependency_tokens(
        token=job_token,
        port=execute_step.get_input_port("__job__"),
        context=context,
        expected_depender=[execute_step.get_output_port(out_port_name).token_list[0]],
        expected_dependee=[deploy_step.get_output_port().token_list[0], token_list[0]],
    )
    await _verify_dependency_tokens(
        token=execute_step.get_output_port(out_port_name).token_list[0],
        port=execute_step.get_output_port(out_port_name),
        context=context,
        expected_dependee=list(job_token.value.inputs.values()) + [job_token],
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
    await _general_test(
        context=context,
        workflow=workflow,
        in_port=in_port,
        out_port=out_port,
        step_cls=GatherStep,
        kwargs_step={"name": utils.random_name() + "-gather", "size_port": size_port},
        token_list=token_list,
    )
    assert len(out_port.token_list) == 2
    await _verify_dependency_tokens(
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
    gather_step = await _general_test(
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
    await _verify_dependency_tokens(
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
        name=utils.random_name() + "-combinator",
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
    await _put_tokens([list_token], in_port, context)
    step.combinator.add_item(port_name)

    tt = Token("4")
    await _put_tokens([tt], in_port_2, context)
    step.combinator.add_item(port_name_2)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    assert len(out_port.token_list) == 2
    await _verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=[list_token, tt],
    )
    assert len(out_port_2.token_list) == 2
    await _verify_dependency_tokens(
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
        name=utils.random_name() + "-combinator",
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
    await _put_tokens(token_list, in_port, context)
    step.combinator.add_item(port_name)

    token_list_2 = [Token("4")]
    await _put_tokens(token_list_2, in_port_2, context)
    step.combinator.add_item(port_name_2)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    assert len(out_port.token_list) == 2
    await _verify_dependency_tokens(
        token=out_port.token_list[0],
        port=out_port,
        context=context,
        expected_dependee=[token_list[0], token_list_2[0]],
    )

    assert len(out_port_2.token_list) == 2
    await _verify_dependency_tokens(
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
    await _put_tokens(token_list, in_port, context)
    step.combinator.add_item(port_name)

    token_list_2 = [Token("4", tag=tag), IterationTerminationToken(tag=tag)]
    await _put_tokens(token_list_2, in_port_2, context)
    step.combinator.add_item(port_name_2)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    assert len(out_port.token_list) == 2
    await _verify_dependency_tokens(
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
    await _put_tokens(list_token, in_port, context)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    assert len(out_port.token_list) == 3
    for out_token, in_token in zip(out_port.token_list[:-1], list_token):
        await _verify_dependency_tokens(
            token=out_token,
            port=out_port,
            context=context,
            expected_dependee=[in_token],
        )
