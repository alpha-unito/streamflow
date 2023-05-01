import asyncio
import posixpath
from typing import MutableMapping, Any, MutableSequence

import pytest

from streamflow.cwl.command import CWLCommand
from streamflow.cwl.translator import _create_command_output_processor_base
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
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
)

from streamflow.workflow.token import ListToken, TerminationToken


# TODO:
#    LoopCombinatorStep,
#    LoopCombinator,
#    LoopTerminationCombinator,
#     CombinatorStep,
#     CartesianProductCombinator,
#     DotProductCombinator,


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
    set_job_completed: bool = False,
):
    """ """
    step = workflow.create_step(cls=step_cls, **kargs_step)
    step.add_input_port(port_name, in_port)
    step.add_output_port(port_name, out_port)

    for token in token_list:
        await token.save(context, in_port.persistent_id)
        in_port.put(token)
    in_port.put(TerminationToken())

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


# todo: remove msg param ... used only for debugging
async def verify_dependency_tokens(
    token, port, expected_depender, expected_dependee, context: StreamFlowContext, msg
):
    loading_context = DefaultDatabaseLoadingContext()

    token_reloaded = await context.database.get_token(token_id=token.persistent_id)
    assert token_reloaded["port"] == port.persistent_id

    depender_list = await _load_depender(token.persistent_id, loading_context, context)
    print()
    print(
        msg,
        "depender:",
        {token.persistent_id: [t.persistent_id for t in depender_list]},
    )
    assert len(depender_list) == len(expected_depender)
    for t1, t2 in zip(depender_list, expected_depender):
        assert t1.persistent_id == t2.persistent_id

    dependee_list = await _load_dependee(token.persistent_id, loading_context, context)
    print(
        msg,
        "dependee:",
        {token.persistent_id: [t.persistent_id for t in dependee_list]},
    )
    assert len(dependee_list) == len(expected_dependee)
    for t1, t2 in zip(dependee_list, expected_dependee):
        assert t1.persistent_id == t2.persistent_id


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
    scatter_step = await _general_test(
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
            out_port.token_list[i],
            out_port,
            (),
            [in_port.token_list[0]],
            context,
            scatter_step.__class__.__name__,
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
        step.get_output_port().token_list[0],
        step.get_output_port(),
        (),
        (),
        context,
        step.__class__.__name__,
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
        "schedule-1.",
    )
    await verify_dependency_tokens(
        job_token,
        schedule_step.get_output_port("__job__"),
        [],
        [deploy_step.get_output_port().token_list[0]],
        context,
        schedule_step.__class__.__name__,
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
        "execute-1.",
    )
    job_token = step.get_input_port("__job__").token_list[0]
    await verify_dependency_tokens(
        step.get_output_port("out-1").token_list[0],
        step.get_output_port("out-1"),
        [],
        list(job_token.value.inputs.values()) + [job_token],
        context,
        step.__class__.__name__,
    )


@pytest.mark.asyncio
async def test_gather_step(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    token_list = [Token(i) for i in range(3)]
    step = await _general_test(
        context,
        workflow,
        in_port,
        out_port,
        GatherStep,
        {"name": utils.random_name() + "-gather"},
        token_list,
    )
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        token_list,
        context,
        step.__class__.__name__,
    )


# async def _test_on_combinator_step(workflow, step, context, port_name="test"):
#     in_port = workflow.create_port()
#     step.add_input_port(port_name, in_port)
#     step.add_output_port(port_name, workflow.create_port())
#     await in_port.save(context)
#     for fst, snd in (("a", "b"), ("c", "d")):
#         list_token = ListToken([Token(fst), Token(snd)])
#         await list_token.save(context, in_port.persistent_id)
#         in_port.put(list_token)
#     in_port.put(TerminationToken())
#
#     step.combinator.add_item(port_name)
#     await workflow.save(context)
#     executor = StreamFlowExecutor(workflow)
#     await executor.run()
#
#     await verify_dependency_tokens(
#         step.get_input_port(port_name).token_list[0],
#         step.get_input_port(port_name),
#         [step.get_output_port(port_name).token_list[0]],
#         [],
#         context,
#         "combinator.",
#     )


# @pytest.mark.asyncio
# async def test_combinator_step_dot_product(context: StreamFlowContext):
#     """ """
#     workflow = Workflow(
#         context=context, type="cwl", name=utils.random_name(), config={}
#     )
#     step = workflow.create_step(
#         cls=CombinatorStep,
#         name=utils.random_name() + "-combinator",
#         combinator=DotProductCombinator(name=utils.random_name(), workflow=workflow),
#     )
#     await _test_on_combinator_step(workflow, step, context)


# @pytest.mark.asyncio
# async def test_combinator_step_cartesian_product(context: StreamFlowContext):
#     """ """
#     workflow = Workflow(
#         context=context, type="cwl", name=utils.random_name(), config={}
#     )
#     step = workflow.create_step(
#         cls=CombinatorStep,
#         name=utils.random_name() + "-combinator",
#         combinator=CartesianProductCombinator(
#             name=utils.random_name(), workflow=workflow
#         ),
#     )
#     await _test_on_combinator_step(workflow, step, context)


#
#
# @pytest.mark.asyncio
# async def test_loop_combinator_step(context: StreamFlowContext):
#     """ """
#
#     workflow = Workflow(
#         context=context, type="cwl", name=utils.random_name(), config={}
#     )
#     step = workflow.create_step(
#         cls=LoopCombinatorStep,
#         name=utils.random_name() + "-combinator",
#         combinator=CartesianProductCombinator(
#             name=utils.random_name(), workflow=workflow, depth=1
#         ),
#     )
#     await workflow.save(context)
#
#
