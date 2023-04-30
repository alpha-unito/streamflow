import asyncio
import posixpath
from typing import MutableSequence

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
from streamflow.core.workflow import Token, Workflow, Status

from streamflow.workflow.port import ConnectorPort
from streamflow.workflow.step import (
    DeployStep,
    ExecuteStep,
    ScheduleStep,
    ScatterStep,
    CombinatorStep,
    GatherStep,
)

#    LoopCombinatorStep,
#    GatherStep,
#     LoopCombinator,
#     LoopTerminationCombinator,
from streamflow.workflow.combinator import (
    CartesianProductCombinator,
    DotProductCombinator,
)
from streamflow.workflow.token import ListToken, TerminationToken


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
    in_token, expected_depender, expected_dependee, context, msg
):
    loading_context = DefaultDatabaseLoadingContext()
    depender_list = await _load_depender(
        in_token.persistent_id, loading_context, context
    )
    print(
        msg,
        "depender:",
        {in_token.persistent_id: [t.persistent_id for t in depender_list]},
    )
    assert len(depender_list) == len(expected_depender)
    for t1, t2 in zip(depender_list, expected_depender):
        assert t1.persistent_id == t2.persistent_id

    dependee_list = await _load_dependee(
        in_token.persistent_id, loading_context, context
    )
    print(
        msg,
        "dependee:",
        {in_token.persistent_id: [t.persistent_id for t in dependee_list]},
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
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    step = workflow.create_step(cls=ScatterStep, name=utils.random_name() + "-scatter")
    step.add_input_port("in-1", workflow.create_port())
    step.add_output_port("out-1", workflow.create_port())
    in_port = step.get_input_port("in-1")
    await in_port.save(context)
    list_token = ListToken([Token("a"), Token("b")])
    await list_token.save(context, in_port.persistent_id)
    in_port.put(list_token)
    in_port.put(TerminationToken())

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    output_token_list = [
        t
        for t in step.get_output_port("out-1").token_list
        if not isinstance(t, TerminationToken)
    ]
    await verify_dependency_tokens(
        list_token,
        output_token_list,
        (),
        context,
        "scatter-in.",
    )
    for t in output_token_list:
        await verify_dependency_tokens(
            t,
            (),
            (list_token,),
            context,
            f"scatter-out-{t.persistent_id}.",
        )


@pytest.mark.asyncio
async def test_deploy_step(context: StreamFlowContext):
    """ """
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    step = _create_deploy_step(workflow)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    await verify_dependency_tokens(
        step.get_output_port().token_list[0],
        (),
        (),
        context,
        "deploy.",
    )


async def _combinator_step(workflow, step, context):
    in_port = workflow.create_port()
    step.add_input_port("fst", in_port)
    step.add_output_port("fst", workflow.create_port())
    list_token = ListToken([Token("a"), Token("b"), Token("c")])
    await list_token.save(context, in_port.persistent_id)
    in_port.put(list_token)
    in_port.put(TerminationToken())

    step.combinator.add_item("fst")
    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    await verify_dependency_tokens(
        step.get_input_port("fst").token_list[0],
        [step.get_output_port("fst").token_list[0]],
        [],
        context,
        "combinator.",
    )


@pytest.mark.asyncio
async def test_combinator_step_dot_product(context: StreamFlowContext):
    """ """
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    step = workflow.create_step(
        cls=CombinatorStep,
        name=utils.random_name() + "-combinator",
        combinator=DotProductCombinator(name=utils.random_name(), workflow=workflow),
    )
    await _combinator_step(workflow, step, context)


@pytest.mark.asyncio
async def test_combinator_step_cartesian_product(context: StreamFlowContext):
    """ """
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    step = workflow.create_step(
        cls=CombinatorStep,
        name=utils.random_name() + "-combinator",
        combinator=CartesianProductCombinator(
            name=utils.random_name(), workflow=workflow
        ),
    )
    await _combinator_step(workflow, step, context)


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


@pytest.mark.asyncio
async def test_schedule_step(context: StreamFlowContext):
    """Test saving and loading ScheduleStep from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )

    deploy_step = _create_deploy_step(workflow)
    schedule_step = _create_schedule_step(workflow, deploy_step)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    job_token = schedule_step.get_output_port("__job__").token_list[0]
    await context.scheduler.notify_status(job_token.value.name, Status.COMPLETED)

    await verify_dependency_tokens(
        deploy_step.get_output_port().token_list[0],
        [job_token],
        [],
        context,
        "schedule-1.",
    )
    await verify_dependency_tokens(
        job_token,
        [],
        [deploy_step.get_output_port().token_list[0]],
        context,
        "schedule-2.",
    )


@pytest.mark.asyncio
async def test_execute_step(context: StreamFlowContext):
    """Test saving and loading ExecuteStep from database"""
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
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
    out_port = workflow.create_port()
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
        [step.get_output_port("out-1").token_list[0]],
        [schedule_step.get_input_port().token_list[0]],
        context,
        "execute-1.",
    )
    await verify_dependency_tokens(
        step.get_output_port("out-1").token_list[0],
        [],
        [
            t
            for v in step.get_input_ports().values()
            for t in v.token_list
            if not isinstance(t, TerminationToken)
        ],
        context,
        "execute-2.",
    )


#
#
# @pytest.mark.asyncio
# async def test_gather_step(context: StreamFlowContext):
#     """Test saving and loading GatherStep from database"""
#     workflow = Workflow(
#         context=context, type="cwl", name=utils.random_name(), config={}
#     )
#     step = workflow.create_step(
#         cls=GatherStep, name=utils.random_name() + "-gather", depth=1
#     )
#     await workflow.save(context)
#
#
# @pytest.mark.asyncio
# async def test_dot_product_combinator(context: StreamFlowContext):
#     """ """
#     workflow = Workflow(
#         context=context, type="cwl", name=utils.random_name(), config={}
#     )
#     step = workflow.create_step(
#         cls=CombinatorStep,
#         name=utils.random_name() + "-combinator",
#         combinator=DotProductCombinator(name=utils.random_name(), workflow=workflow),
#     )
#     await workflow.save(context)
#
#
# @pytest.mark.asyncio
# async def test_loop_combinator(context: StreamFlowContext):
#     """Test saving and loading CombinatorStep with LoopCombinator from database"""
#     workflow = Workflow(
#         context=context, type="cwl", name=utils.random_name(), config={}
#     )
#     step = workflow.create_step(
#         cls=CombinatorStep,
#         name=utils.random_name() + "-combinator",
#         combinator=LoopCombinator(name=utils.random_name(), workflow=workflow),
#     )
#     await workflow.save(context)
#
#
# @pytest.mark.asyncio
# async def test_loop_termination_combinator(context: StreamFlowContext):
#     """ """
#     workflow = Workflow(
#         context=context, type="cwl", name=utils.random_name(), config={}
#     )
#     step = workflow.create_step(
#         cls=CombinatorStep,
#         name=utils.random_name() + "-combinator",
#         combinator=LoopTerminationCombinator(
#             name=utils.random_name(), workflow=workflow
#         ),
#     )
#     await workflow.save(context)
