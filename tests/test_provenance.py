import posixpath
from typing import MutableMapping, Any, MutableSequence, Type

import pytest

from streamflow.core.utils import contains_id
from streamflow.cwl.command import CWLCommand, CWLCommandToken
from streamflow.cwl.translator import _create_command_output_processor_base
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.workflow.combinator import (
    DotProductCombinator,
    CartesianProductCombinator,
    LoopCombinator,
    LoopTerminationCombinator,
)
from streamflow.workflow.executor import StreamFlowExecutor
from tests.conftest import get_docker_deployment_config, random_dir_path

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.config import BindingConfig
from streamflow.core.deployment import Target
from streamflow.core.workflow import Token, Workflow, Status, Port, Step

from streamflow.workflow.port import ConnectorPort
from streamflow.workflow.step import (
    DeployStep,
    ExecuteStep,
    ScheduleStep,
    ScatterStep,
    GatherStep,
    CombinatorStep,
    LoopCombinatorStep,
)

from streamflow.workflow.token import (
    ListToken,
    TerminationToken,
    IterationTerminationToken,
)


async def _put_tokens(token_list, in_port, context):
    for t in token_list:
        if not isinstance(t, TerminationToken):
            await t.save(context, in_port.persistent_id)
        in_port.put(t)
    in_port.put(TerminationToken())


async def _create_workflow(context: StreamFlowContext, num_port=2):
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    ports = []
    for _ in range(num_port):
        ports.append(workflow.create_port())
    await workflow.save(context)
    return workflow, *ports


async def _general_test(
    context: StreamFlowContext,
    workflow: Workflow,
    in_port: Port,
    out_port: Port,
    step_cls: Type[Step],
    kargs_step: MutableMapping[str, Any],
    token_list: MutableSequence[Token],
    port_name: str = "test",
):
    step = workflow.create_step(cls=step_cls, **kargs_step)
    step.add_input_port(port_name, in_port)
    step.add_output_port(port_name, out_port)

    await _put_tokens(token_list, in_port, context)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    return step


async def verify_dependency_tokens(
    token,
    port,
    expected_depender,
    expected_dependee,
    context: StreamFlowContext,
    alternative_expected_dependee=None,
):
    loading_context = DefaultDatabaseLoadingContext()

    token_reloaded = await context.database.get_token(token_id=token.persistent_id)
    assert token_reloaded["port"] == port.persistent_id

    depender_list = await loading_context.load_next_tokens(context, token.persistent_id)
    print(
        "depender:",
        {token.persistent_id: [t.persistent_id for t in depender_list]},
    )
    print("expected_depender", [t.persistent_id for t in expected_depender])
    assert len(depender_list) == len(expected_depender)
    for t1 in depender_list:
        assert contains_id(t1.persistent_id, expected_depender)

    dependee_list = await loading_context.load_prev_tokens(context, token.persistent_id)
    print(
        "dependee:",
        {token.persistent_id: [t.persistent_id for t in dependee_list]},
    )
    print("expected_dependee", [t.persistent_id for t in expected_dependee])
    try:
        assert len(dependee_list) == len(expected_dependee)
        for t1 in dependee_list:
            assert contains_id(t1.persistent_id, expected_dependee)
    except AssertionError as err:
        if alternative_expected_dependee is None:
            raise err
        else:
            assert len(dependee_list) == len(alternative_expected_dependee)
            for t1 in dependee_list:
                assert contains_id(t1.persistent_id, alternative_expected_dependee)


def _create_deploy_step(workflow, deployment_config=None):
    connector_port = workflow.create_port(cls=ConnectorPort)
    if not deployment_config:
        deployment_config = get_docker_deployment_config()
    return workflow.create_step(
        cls=DeployStep,
        name=posixpath.join("__deploy__", deployment_config.name),
        deployment_config=deployment_config,
        connector_port=connector_port,
    )


def _create_schedule_step(workflow, deploy_step, binding_config=None):
    if not binding_config:
        binding_config = BindingConfig(
            targets=[
                Target(
                    deployment=deploy_step.deployment_config,
                    workdir=utils.random_name(),
                )
            ]
        )
    return workflow.create_step(
        cls=ScheduleStep,
        name=posixpath.join(utils.random_name(), "__schedule__"),
        job_prefix=random_dir_path(1),
        connector_ports={
            binding_config.targets[0].deployment.name: deploy_step.get_output_port()
        },
        binding_config=binding_config,
    )


@pytest.mark.asyncio
async def test_scatter_step(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    token_list = [ListToken([Token("a"), Token("b"), Token("c")])]
    await _general_test(
        context,
        workflow,
        in_port,
        out_port,
        ScatterStep,
        {"name": utils.random_name() + "-scatter"},
        token_list,
    )
    assert len(out_port.token_list) == 4
    for curr_token in out_port.token_list[:-1]:
        await verify_dependency_tokens(
            curr_token, out_port, (), [in_port.token_list[0]], context
        )


@pytest.mark.asyncio
async def test_deploy_step(context: StreamFlowContext):
    """ """
    workflow = (await _create_workflow(context, num_port=0))[0]
    step = _create_deploy_step(workflow)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    # len(token_list) = N output tokens + 1 termination token
    assert len(step.get_output_port().token_list) == 2
    await verify_dependency_tokens(
        step.get_output_port().token_list[0], step.get_output_port(), (), (), context
    )


@pytest.mark.asyncio
async def test_schedule_step(context: StreamFlowContext):
    """ """
    workflow = (await _create_workflow(context, num_port=0))[0]
    deploy_step = _create_deploy_step(workflow)
    schedule_step = _create_schedule_step(workflow, deploy_step)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    job_token = schedule_step.get_output_port("__job__").token_list[0]
    await context.scheduler.notify_status(job_token.value.name, Status.COMPLETED)

    await verify_dependency_tokens(
        deploy_step.get_output_port().token_list[0],
        deploy_step.get_output_port(),
        [job_token],
        [],
        context,
    )
    await verify_dependency_tokens(
        job_token,
        schedule_step.get_output_port("__job__"),
        [],
        [deploy_step.get_output_port().token_list[0]],
        context,
    )


@pytest.mark.asyncio
async def test_execute_step(context: StreamFlowContext):
    """ """
    workflow, in_port_schedule, in_port, out_port = await _create_workflow(
        context, num_port=3
    )
    deploy_step = _create_deploy_step(workflow)
    schedule_step = _create_schedule_step(workflow, deploy_step)

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
        command_tokens=[CWLCommandToken(name=in_port_name, value=None)],
    )
    execute_step.add_output_port(
        out_port_name,
        out_port,
        _create_command_output_processor_base(
            out_port.name,
            workflow,
            None,
            "string",
            {},
            {"hints": {}, "requirements": {}},
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
    await verify_dependency_tokens(
        job_token,
        execute_step.get_input_port("__job__"),
        [execute_step.get_output_port(out_port_name).token_list[0]],
        [deploy_step.get_output_port().token_list[0], token_list[0]],
        context,
    )
    await verify_dependency_tokens(
        execute_step.get_output_port(out_port_name).token_list[0],
        execute_step.get_output_port(out_port_name),
        [],
        list(job_token.value.inputs.values()) + [job_token],
        context,
    )


@pytest.mark.asyncio
async def test_gather_step(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    token_list = [Token(i) for i in range(3)]
    await _general_test(
        context,
        workflow,
        in_port,
        out_port,
        GatherStep,
        {"name": utils.random_name() + "-gather"},
        token_list,
    )
    assert len(out_port.token_list) == 2
    await verify_dependency_tokens(
        out_port.token_list[0], out_port, (), token_list, context
    )


@pytest.mark.asyncio
async def test_combinator_step_dot_product(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port, in_port_2, out_port_2 = await _create_workflow(
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
    await verify_dependency_tokens(
        out_port.token_list[0], out_port, [], [list_token, tt], context
    )
    assert len(out_port_2.token_list) == 2
    await verify_dependency_tokens(
        out_port_2.token_list[0], out_port_2, [], [list_token, tt], context
    )


@pytest.mark.asyncio
async def test_combinator_step_cartesian_product(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port, in_port_2, out_port_2 = await _create_workflow(
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
    await verify_dependency_tokens(
        out_port.token_list[0], out_port, [], [token_list[0], token_list_2[0]], context
    )

    assert len(out_port_2.token_list) == 2
    await verify_dependency_tokens(
        out_port_2.token_list[0],
        out_port_2,
        [],
        [token_list[0], token_list_2[0]],
        context,
    )


@pytest.mark.asyncio
async def test_loop_combinator_step(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port, in_port_2, out_port_2 = await _create_workflow(
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
    await verify_dependency_tokens(
        out_port.token_list[0], out_port, [], [token_list[0], token_list_2[0]], context
    )


@pytest.mark.asyncio
async def test_loop_termination_combinator(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context, num_port=2)
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
        await verify_dependency_tokens(out_token, out_port, [], [in_token], context)


@pytest.mark.asyncio
async def test_nested_crossproduct_combinator(context: StreamFlowContext):
    """ """
    workflow, in_port_1, in_port_2, out_port_1, out_port_2 = await _create_workflow(
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
    await _put_tokens(list_token_1, in_port_1, context)

    list_token_2 = [
        ListToken([Token("1"), Token("2")], tag="0.0"),
        ListToken([Token("3"), Token("4")], tag="0.1"),
    ]
    await _put_tokens(list_token_2, in_port_2, context)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    nested_crossproduct_1 = [(t1, t2) for t2 in list_token_2 for t1 in list_token_1]
    nested_crossproduct_2 = [(t1, t2) for t1 in list_token_1 for t2 in list_token_2]

    for t in list_token_1:
        print(f"{t.persistent_id}, {t.tag}, {[tt.value for tt in t.value]}")

    for t in list_token_2:
        print(f"{t.persistent_id}, {t.tag}, {[tt.value for tt in t.value]}")
    print()

    for out_token in out_port_1.token_list[:-1]:
        print(
            f"{out_token.persistent_id}, {out_token.tag}, {[tt.value for tt in out_token.value]}"
        )
    for out_token in out_port_2.token_list[:-1]:
        print(
            f"{out_token.persistent_id}, {out_token.tag}, {[tt.value for tt in out_token.value]}"
        )
    print()
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
            out_token,
            out_port_1,
            [],
            nested_crossproduct_1[i],
            context,
            alternative_expected_dependee=nested_crossproduct_2[i],
        )

    # check port_2 outputs
    assert len(out_port_2.token_list) == 5
    for i, out_token in enumerate(out_port_2.token_list[:-1]):
        await verify_dependency_tokens(
            out_token,
            out_port_2,
            [],
            nested_crossproduct_1[i],
            context,
            alternative_expected_dependee=nested_crossproduct_2[i],
        )
