import posixpath

import pytest

from streamflow.workflow.token import ListToken

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Token, Status

from streamflow.cwl.step import (
    CWLTransferStep,  # TransferStep
    CWLConditionalStep,  # ConditionalStep
    CWLInputInjectorStep,  # InputInjectorStep
    CWLEmptyScatterConditionalStep,  # LoopOutputStep
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
)

from tests.test_provenance import (
    verify_dependency_tokens,
    _create_deploy_step,
    _create_schedule_step,
    _general_test,
    _create_workflow,
)

# TODO
#  ListMergeCombinator
#  LoopValueFromTransformer,
#  CWLLoopOutputAllStep,
#  CWLLoopOutputLastStep,


@pytest.mark.asyncio
async def test_default_transformer(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    token_list = [Token("a")]
    transformer = await _general_test(
        context,
        workflow,
        in_port,
        out_port,
        DefaultTransformer,
        {"name": utils.random_name() + "-transformer", "default_port": in_port},
        token_list,
    )
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        token_list,
        context,
        transformer.__class__.__name__,
    )


@pytest.mark.asyncio
async def test_default_retag_transformer(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    token_list = [Token("a")]
    transformer = await _general_test(
        context,
        workflow,
        in_port,
        out_port,
        DefaultRetagTransformer,
        {"name": utils.random_name() + "-transformer", "default_port": in_port},
        token_list,
    )
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        token_list,
        context,
        transformer.__class__.__name__,
    )


@pytest.mark.asyncio
async def test_cwl_token_transformer(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    port_name = "test"
    step_name = utils.random_name()
    token_list = [Token("a")]
    transformer = await _general_test(
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
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        token_list,
        context,
        transformer.__class__.__name__,
    )


@pytest.mark.asyncio
async def test_value_from_transformer(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    port_name = "test"
    token_list = [Token(10)]
    transformer = await _general_test(
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
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        token_list,
        context,
        transformer.__class__.__name__,
    )


@pytest.mark.asyncio
async def test_all_non_null_transformer(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    token_list = [ListToken([Token("a"), Token(None), Token("b")])]
    transformer = await _general_test(
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
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        token_list,
        context,
        transformer.__class__.__name__,
    )


@pytest.mark.asyncio
async def test_first_non_null_transformer(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    token_list = [ListToken([Token(None), Token("a")])]
    transformer = await _general_test(
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
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        token_list,
        context,
        transformer.__class__.__name__,
    )


@pytest.mark.asyncio
async def test_forward_transformer(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    token_list = [ListToken([Token("a")])]
    transformer = await _general_test(
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
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        token_list,
        context,
        transformer.__class__.__name__,
    )


@pytest.mark.asyncio
async def test_list_to_element_transformer(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    token_list = [ListToken([Token("a")])]
    transformer = await _general_test(
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
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        token_list,
        context,
        transformer.__class__.__name__,
    )


@pytest.mark.asyncio
async def test_only_non_null_transformer(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    token_list = [ListToken([Token(None), Token("a")])]
    transformer = await _general_test(
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
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        token_list,
        context,
        transformer.__class__.__name__,
    )


@pytest.mark.asyncio
async def test_cwl_conditional_step(context: StreamFlowContext):
    """ """
    workflow, in_port, out_port = await _create_workflow(context)
    port_name = "test"
    token_list = [ListToken([Token("a")])]
    conditional_step = await _general_test(
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
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        token_list,
        context,
        conditional_step.__class__.__name__,
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
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        token_list,
        context,
        transfer_step.__class__.__name__,
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
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        token_list,
        context,
        injector.__class__.__name__,
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
    for i in range(len(in_port.token_list) - 1):
        curr_token = out_port.token_list[i]
        await verify_dependency_tokens(
            curr_token,
            out_port,
            (),
            [in_port.token_list[i]],
            context,
            f"CWLEmptyScatterConditionalStep-out-{curr_token.persistent_id}",
        )


# @pytest.mark.asyncio
# async def test_loop_value_from_transformer(context: StreamFlowContext):
#     """ """
#     workflow = Workflow(
#         context=context, type="cwl", name=utils.random_name(), config={}
#     )
#     in_port = workflow.create_port()
#     out_port = workflow.create_port()
#     await workflow.save(context)
#     name = utils.random_name()
#     port_name = "test"
#     transformer = workflow.create_step(
#         cls=LoopValueFromTransformer,
#         name=name + "-loop-value-from-transformer",
#         processor=CWLTokenProcessor(
#             name=in_port.name,
#             workflow=workflow,
#         ),
#         port_name=port_name,
#         full_js=True,
#         value_from=f"$(inputs.{port_name} + 1)",
#     )
#     # transformer.add_loop_input_port(port_name, in_port)
#     # transformer.add_output_port(port_name, out_port)
#
#     token = Token(10)
#     await token.save(context, in_port.persistent_id)
#     in_port.put(token)
#     in_port.put(TerminationToken())
#
#     await workflow.save(context)
#     executor = StreamFlowExecutor(workflow)
#     await executor.run()
#     await verify_dependency_tokens(
#         transformer.get_output_port(port_name).token_list[0],
#         out_port,
#         (),
#         [token],
#         context,
#         "value_from_transformer.",
#     )


# @pytest.mark.asyncio
# async def test_list_merge_combinator(context: StreamFlowContext):
#     """ """
#     workflow = Workflow(
#         context=context, type="cwl", name=utils.random_name(), config={}
#     )
#
#     port_name = "test"
#     step_name = utils.random_name()
#     step = workflow.create_step(
#         cls=CombinatorStep,
#         name=step_name + "-combinator",
#         combinator=ListMergeCombinator(
#             name=utils.random_name(),
#             workflow=workflow,
#             input_names=[port_name],
#             output_name=port_name,
#             flatten=False,
#         ),
#     )
#     await _test_on_combinator_step(workflow, step, context, port_name=port_name)


# @pytest.mark.asyncio
# async def test_cwl_loop_output_all_step(context: StreamFlowContext):
#     """ """
#     workflow = Workflow(
#         context=context, type="cwl", name=utils.random_name(), config={}
#     )
#     await workflow.save(context)
#
#     step = workflow.create_step(
#         cls=CWLLoopOutputAllStep,
#         name=posixpath.join(utils.random_name(), "-loop-output"),
#     )
#     await save_load_and_test(step, context)

#
# @pytest.mark.asyncio
# async def test_cwl_loop_output_last_step(context: StreamFlowContext):
#     """ """
#     workflow = Workflow(
#         context=context, type="cwl", name=utils.random_name(), config={}
#     )
#     await workflow.save(context)
#
#     step = workflow.create_step(
#         cls=CWLLoopOutputLastStep,
#         name=posixpath.join(utils.random_name(), "-last"),
#     )
#     await save_load_and_test(step, context)
