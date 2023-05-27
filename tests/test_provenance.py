import asyncio
import itertools
import posixpath
from typing import MutableMapping, Any, MutableSequence

import pytest

from streamflow.cwl.command import CWLCommand
from streamflow.cwl.translator import _create_command_output_processor_base
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.workflow.combinator import (
    DotProductCombinator,
    CartesianProductCombinator,
)
from streamflow.workflow.executor import StreamFlowExecutor
from tests.conftest import get_docker_deployment_config

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
        if not isinstance(t, TerminationToken) and not isinstance(
            t, IterationTerminationToken
        ):
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
    step_cls: Step,
    kargs_step: MutableMapping[str, Any],
    token_list: MutableSequence[Token],
    port_name: str = "test",
):
    """ """
    step = workflow.create_step(cls=step_cls, **kargs_step)
    step.add_input_port(port_name, in_port)
    step.add_output_port(port_name, out_port)

    await _put_tokens(token_list, in_port, context)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    return step


async def _load_dependee(token_id, loading_context, context):
    rows = await context.database.get_dependee(token_id)
    return await asyncio.gather(
        *(
            asyncio.create_task(loading_context.load_token(context, row["dependee"]))
            for row in rows
        )
    )


async def _load_depender(token_id, loading_context, context):
    rows = await context.database.get_depender(token_id)
    return await asyncio.gather(
        *(
            asyncio.create_task(loading_context.load_token(context, row["depender"]))
            for row in rows
        )
    )


def contains_id(id, token_list):
    for t in token_list:
        if id == t.persistent_id:
            return True
    return False


# todo: remove msg param ... used only for debugging
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

    depender_list = await _load_depender(token.persistent_id, loading_context, context)
    print(
        "depender:",
        {token.persistent_id: [t.persistent_id for t in depender_list]},
    )
    assert len(depender_list) == len(expected_depender)
    for t1 in depender_list:
        assert contains_id(t1.persistent_id, expected_depender)

    print("search dependee of ", token.persistent_id)
    dependee_list = await _load_dependee(token.persistent_id, loading_context, context)
    print(
        "dependee:",
        {token.persistent_id: [t.persistent_id for t in dependee_list]},
    )
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
        job_prefix="something",
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
    for i in range(len(out_port.token_list) - 1):
        await verify_dependency_tokens(
            out_port.token_list[i], out_port, (), [in_port.token_list[0]], context
        )


@pytest.mark.asyncio
async def test_deploy_step(context: StreamFlowContext):
    """ """
    workflow = (await _create_workflow(context, num_port=0))[0]
    step = _create_deploy_step(workflow)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
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
    workflow, out_port = await _create_workflow(context, num_port=1)
    deploy_step = _create_deploy_step(workflow)
    schedule_step = _create_schedule_step(workflow, deploy_step)
    # todo: add inputs in job
    step = workflow.create_step(
        cls=ExecuteStep,
        name=utils.random_name(),
        job_port=schedule_step.get_output_port(),
    )
    step.command = CWLCommand(
        step=step,
        base_command=["ls"],
    )
    step.add_output_port(
        "out-1",
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
    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    await verify_dependency_tokens(
        step.get_input_port("__job__").token_list[0],
        step.get_input_port("__job__"),
        [step.get_output_port("out-1").token_list[0]],
        [schedule_step.get_input_port().token_list[0]],
        context,
    )
    job_token = step.get_input_port("__job__").token_list[0]
    await verify_dependency_tokens(
        step.get_output_port("out-1").token_list[0],
        step.get_output_port("out-1"),
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

    print("out token dot_prod", out_port.token_list[0])
    await verify_dependency_tokens(
        out_port.token_list[0], out_port, [], [list_token, tt], context
    )
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

    await verify_dependency_tokens(
        out_port.token_list[0], out_port, [], [token_list[0], token_list_2[0]], context
    )
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
    await workflow.save(context)
    name = utils.random_name()
    step = workflow.create_step(
        cls=LoopCombinatorStep,
        name=name + "-combinator",
        combinator=CartesianProductCombinator(name=name, workflow=workflow, depth=1),
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

    await verify_dependency_tokens(
        out_port.token_list[0], out_port, [], [token_list[0], token_list_2[0]], context
    )


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

    # TMP
    _ = list(itertools.permutations(nested_crossproduct_1))

    # Combinator are not deterministic. Possible cases:
    # {output_token_id: [input_token_id_list]}
    # case #1: { 77: [67, 73], 79: [70, 73], 81: [67, 76], 83: [70, 76] }
    # case #2: { 77: [67, 73], 79: [67, 76], 81: [70, 73], 83: [70, 76] }
    loading_context = DefaultDatabaseLoadingContext()
    for i, out_token in enumerate(out_port_1.token_list[:-1]):
        print(
            "out_token",
            out_token.persistent_id,
            "tag",
            out_token.tag,
            "value: ",
            [t.value for t in out_token.value],
        )
        print(
            "cerca input",
            [t.persistent_id for t in nested_crossproduct_1[i]],
            "or",
            [t.persistent_id for t in nested_crossproduct_2[i]],
        )
        dependee_list = await _load_dependee(
            out_token.persistent_id, loading_context, context
        )
        print("trovati", [t.persistent_id for t in dependee_list])
        await verify_dependency_tokens(
            out_token,
            out_port_1,
            [],
            nested_crossproduct_1[i],
            context,
            alternative_expected_dependee=nested_crossproduct_2[i],
        )
        print()

    # case #1: { 78: [67, 73], 80: [70, 73], 82: [67, 76], 84: [70, 76] }
    # case #2: { 78: [67, 73], 80: [67, 76], 82: [70, 73], 84: [70, 76] }
    # for i, out_token in enumerate(out_port_2.token_list[:-1]):
    #     # print("out_token", out_token.persistent_id, "tag", out_token.tag, "value: ", [t.value for t in out_token.value])
    #     # print("cerca input", [t.persistent_id for t in nested_crossproduct_1[i]], "or",
    #     #       [t.persistent_id for t in nested_crossproduct_2[i]])
    #     await verify_dependency_tokens(
    #         out_token,
    #         out_port_2,
    #         [],
    #         nested_crossproduct_2[i],
    #         context,
    #         "test_nested_crossproduct_combinator-2."
    #     )
