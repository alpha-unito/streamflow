import posixpath

import pytest

from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.token import TerminationToken, ListToken

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Workflow, Token, Status

from streamflow.workflow.step import CombinatorStep, ExecuteStep

# abstract classes the extend the Step class: ConditionalStep, InputInjectorStep, LoopOutputStep, TransferStep, Transformer
from streamflow.cwl.utils import LoadListing, SecondaryFile
from streamflow.cwl.step import (
    CWLTransferStep,  # TransferStep
    CWLConditionalStep,  # ConditionalStep
    CWLInputInjectorStep,  # InputInjectorStep
    CWLLoopOutputAllStep,  # LoopOutputStep
    CWLLoopOutputLastStep,
    CWLEmptyScatterConditionalStep,  # LoopOutputStep
)
from streamflow.cwl.combinator import ListMergeCombinator  # CombinatorStep
from streamflow.cwl.processor import (
    CWLTokenProcessor,
    CWLMapTokenProcessor,
    CWLObjectTokenProcessor,
    CWLUnionTokenProcessor,
    CWLFileToken,
)
from streamflow.cwl.transformer import (
    DefaultTransformer,
    DefaultRetagTransformer,
    CWLTokenTransformer,
    ValueFromTransformer,
    LoopValueFromTransformer,
    AllNonNullTransformer,
    FirstNonNullTransformer,
    ForwardTransformer,
    ListToElementTransformer,
    OnlyNonNullTransformer,
)
from streamflow.cwl.command import (
    CWLCommand,
    CWLCommandToken,
    CWLObjectCommandToken,
    CWLExpressionCommand,
    CWLStepCommand,
    CWLUnionCommandToken,
    CWLMapCommandToken,
)
from tests.test_provenance import (
    verify_dependency_tokens,
    _test_on_combinator_step,
    _create_deploy_step,
    _create_schedule_step,
)


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


@pytest.mark.asyncio
async def test_default_transformer(context: StreamFlowContext):
    """ """
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    in_port = workflow.create_port()
    out_port = workflow.create_port()
    await workflow.save(context)

    name = utils.random_name()
    transformer = workflow.create_step(
        cls=DefaultTransformer, name=name + "-transformer", default_port=in_port
    )
    port_name = "test"
    transformer.add_input_port(port_name, in_port)
    transformer.add_output_port(port_name, out_port)

    token = Token("a")
    await token.save(context, in_port.persistent_id)
    in_port.put(token)
    in_port.put(TerminationToken())

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        [token],
        context,
        "default_transformer.",
    )


@pytest.mark.asyncio
async def test_default_retag_transformer(context: StreamFlowContext):
    """ """
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    in_port = workflow.create_port()
    out_port = workflow.create_port()
    await workflow.save(context)

    name = utils.random_name()
    transformer = workflow.create_step(
        cls=DefaultRetagTransformer, name=name + "-transformer", default_port=in_port
    )
    port_name = "test"
    transformer.add_input_port(port_name, in_port)
    transformer.add_output_port(port_name, out_port)

    token = Token("a")
    await token.save(context, in_port.persistent_id)
    in_port.put(token)
    in_port.put(TerminationToken())

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        [token],
        context,
        "default_transformer.",
    )


@pytest.mark.asyncio
async def test_cwl_token_transformer(context: StreamFlowContext):
    """ """
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    in_port = workflow.create_port()
    out_port = workflow.create_port()
    name = utils.random_name()
    port_name = "test"
    transformer = workflow.create_step(
        cls=CWLTokenTransformer,
        name=name + "-transformer",
        port_name=port_name,
        processor=CWLTokenProcessor(
            name=name,
            workflow=workflow,
        ),
    )
    transformer.add_input_port(port_name, in_port)
    transformer.add_output_port(port_name, out_port)

    token = Token("a")
    await token.save(context, in_port.persistent_id)
    in_port.put(token)
    in_port.put(TerminationToken())

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        [token],
        context,
        "cwl_token_transformer.",
    )


@pytest.mark.asyncio
async def test_value_from_transformer(context: StreamFlowContext):
    """ """
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    in_port = workflow.create_port()
    out_port = workflow.create_port()
    await workflow.save(context)
    name = utils.random_name()
    port_name = "test"
    transformer = workflow.create_step(
        cls=ValueFromTransformer,
        name=name + "-value-from-transformer",
        processor=CWLTokenProcessor(
            name=in_port.name,
            workflow=workflow,
        ),
        port_name=in_port.name,
        full_js=True,
        value_from=f"$(inputs.{port_name} + 1)",
    )
    transformer.add_input_port(port_name, in_port)
    transformer.add_output_port(port_name, out_port)

    token = Token(10)
    await token.save(context, in_port.persistent_id)
    in_port.put(token)
    in_port.put(TerminationToken())

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        [token],
        context,
        "value_from_transformer.",
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


@pytest.mark.asyncio
async def test_all_non_null_transformer(context: StreamFlowContext):
    """ """
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    in_port = workflow.create_port()
    out_port = workflow.create_port()
    await workflow.save(context)
    name = utils.random_name()
    port_name = "test"
    transformer = workflow.create_step(
        cls=AllNonNullTransformer, name=name + "-transformer"
    )
    transformer.add_input_port(port_name, in_port)
    transformer.add_output_port(port_name, out_port)
    list_token = ListToken([Token("a"), Token(None), Token("b")])
    await list_token.save(context, in_port.persistent_id)
    in_port.put(list_token)
    in_port.put(TerminationToken())

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        [list_token],
        context,
        "all_non_null_transformer.",
    )


@pytest.mark.asyncio
async def test_first_non_null_transformer(context: StreamFlowContext):
    """ """
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    in_port = workflow.create_port()
    out_port = workflow.create_port()
    await workflow.save(context)

    name = utils.random_name()
    port_name = "test"
    transformer = workflow.create_step(
        cls=FirstNonNullTransformer, name=name + "-transformer"
    )
    transformer.add_input_port(port_name, in_port)
    transformer.add_output_port(port_name, out_port)
    list_token = ListToken([Token(None), Token("a")])
    await list_token.save(context, in_port.persistent_id)
    in_port.put(list_token)
    in_port.put(TerminationToken())

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        [list_token],
        context,
        "first_non_null_transformer.",
    )


@pytest.mark.asyncio
async def test_forward_transformer(context: StreamFlowContext):
    """ """
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    in_port = workflow.create_port()
    out_port = workflow.create_port()
    await workflow.save(context)
    name = utils.random_name()
    port_name = "test"
    transformer = workflow.create_step(
        cls=ForwardTransformer, name=name + "-transformer"
    )
    transformer.add_input_port(port_name, in_port)
    transformer.add_output_port(port_name, out_port)

    list_token = ListToken([Token("a")])
    await list_token.save(context, in_port.persistent_id)
    in_port.put(list_token)
    in_port.put(TerminationToken())

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        [list_token],
        context,
        "forward_transformer.",
    )


@pytest.mark.asyncio
async def test_list_to_element_transformer(context: StreamFlowContext):
    """ """
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    in_port = workflow.create_port()
    out_port = workflow.create_port()
    await workflow.save(context)

    name = utils.random_name()
    port_name = "test"
    transformer = workflow.create_step(
        cls=ListToElementTransformer, name=name + "-transformer"
    )
    transformer.add_input_port(port_name, in_port)
    transformer.add_output_port(port_name, out_port)

    list_token = ListToken([Token("a")])
    await list_token.save(context, in_port.persistent_id)
    in_port.put(list_token)
    in_port.put(TerminationToken())

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        [list_token],
        context,
        "list_to_element_transformer.",
    )


@pytest.mark.asyncio
async def test_only_non_null_transformer(context: StreamFlowContext):
    """ """
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    in_port = workflow.create_port()
    out_port = workflow.create_port()
    await workflow.save(context)
    port_name = "test"
    name = utils.random_name()
    transformer = workflow.create_step(
        cls=OnlyNonNullTransformer, name=name + "-transformer"
    )
    transformer.add_input_port(port_name, in_port)
    transformer.add_output_port(port_name, out_port)

    list_token = ListToken([Token(None), Token("a")])
    await list_token.save(context, in_port.persistent_id)
    in_port.put(list_token)
    in_port.put(TerminationToken())

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        [list_token],
        context,
        "forward_transformer.",
    )


@pytest.mark.asyncio
async def test_cwl_conditional_step(context: StreamFlowContext):
    """ """
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )

    port_name = "test"
    step = workflow.create_step(
        cls=CWLConditionalStep,
        name=utils.random_name() + "-when",
        expression=f"$(inputs.{port_name}.length == 1)",
        full_js=True,
    )
    in_port = workflow.create_port()
    out_port = workflow.create_port()
    step.add_input_port(port_name, in_port)
    step.add_output_port(port_name, out_port)
    list_token = ListToken([Token("a")])
    await workflow.save(context)
    await list_token.save(context, in_port.persistent_id)
    in_port.put(list_token)
    in_port.put(TerminationToken())

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        [list_token],
        context,
        "cwl_conditional_step.",
    )


@pytest.mark.asyncio
async def test_transfer_step(context: StreamFlowContext):
    """ """
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    deploy_step = _create_deploy_step(workflow)
    schedule_step = _create_schedule_step(workflow, deploy_step)
    port_name = "test"
    step = workflow.create_step(
        cls=CWLTransferStep,
        name=posixpath.join(utils.random_name(), "__transfer__", port_name),
        job_port=schedule_step.get_output_port(),
    )
    in_port = workflow.create_port()
    out_port = workflow.create_port()
    step.add_input_port(port_name, in_port)
    step.add_output_port(port_name, out_port)
    await workflow.save(context)
    token = Token("a")
    await token.save(context, in_port.persistent_id)
    in_port.put(token)
    in_port.put(TerminationToken())
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    job_token = schedule_step.get_output_port().token_list[0]
    await verify_dependency_tokens(
        out_port.token_list[0],
        out_port,
        (),
        [token, job_token],
        context,
        "transfer_step.",
    )
    await context.scheduler.notify_status(job_token.value.name, Status.COMPLETED)


@pytest.mark.asyncio
async def test_cwl_input_injector_step(context: StreamFlowContext):
    """ """
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    deploy_step = _create_deploy_step(workflow)
    schedule_step = _create_schedule_step(workflow, deploy_step)
    step = workflow.create_step(
        cls=CWLInputInjectorStep,
        name=posixpath.join(utils.random_name(), "-injector"),
        job_port=schedule_step.get_output_port(),
    )
    port_name = "test"
    in_port = workflow.create_port()
    step.add_input_port(port_name, in_port)
    step.add_output_port(port_name, workflow.create_port())
    await workflow.save(context)
    token = Token("a")
    await token.save(context, in_port.persistent_id)
    in_port.put(token)
    in_port.put(TerminationToken())
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    job_token = schedule_step.get_output_port().token_list[0]
    await verify_dependency_tokens(
        step.get_output_port(port_name).token_list[0],
        step.get_output_port(port_name),
        (),
        [token, job_token],
        context,
        "cwl_input_injector_step.",
    )
    await context.scheduler.notify_status(job_token.value.name, Status.COMPLETED)


@pytest.mark.asyncio
async def test_empty_scatter_conditional_step(context: StreamFlowContext):
    """ """
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    step = workflow.create_step(
        cls=CWLEmptyScatterConditionalStep,
        name=utils.random_name() + "-empty-scatter-condition",
        scatter_method="dotproduct",
    )
    port_name = "test"
    step.add_input_port(port_name, workflow.create_port())
    step.add_output_port(port_name, workflow.create_port())
    in_port = step.get_input_port(port_name)
    await in_port.save(context)
    for i in range(1, 5):
        t = ListToken([Token(i), Token(i * 100)])
        await t.save(context, in_port.persistent_id)
        in_port.put(t)
    in_port.put(TerminationToken())

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    for i in range(len(in_port.token_list) - 1):
        await verify_dependency_tokens(
            step.get_output_port(port_name).token_list[i],
            step.get_output_port(port_name),
            (),
            [step.get_input_port(port_name).token_list[i]],
            context,
            f"empty_scatter_conditional_step-out-{step.get_output_port(port_name).token_list[i].persistent_id}.",
        )


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
