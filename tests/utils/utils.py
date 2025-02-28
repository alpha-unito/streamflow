from __future__ import annotations

import logging
from collections.abc import MutableMapping, MutableSequence
from typing import Any

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Port, Step, Token, Workflow
from streamflow.log_handler import logger
from streamflow.persistence.loading_context import (
    DefaultDatabaseLoadingContext,
    WorkflowBuilder,
)
from streamflow.persistence.utils import load_dependee_tokens, load_depender_tokens
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.step import Combinator
from streamflow.workflow.token import TerminationToken
from tests.conftest import are_equals


def _contains_token_id(id_: int, token_list: MutableSequence[Token]) -> bool:
    return any(id_ == t.persistent_id for t in token_list)


def check_combinators(original_combinator: Combinator, new_combinator: Combinator):
    assert (
        original_combinator.workflow.persistent_id
        != new_combinator.workflow.persistent_id
    )
    for original_inner, new_inner in zip(
        original_combinator.combinators.values(), new_combinator.combinators.values()
    ):
        check_combinators(original_inner, new_inner)


def check_persistent_id(
    original_workflow: Workflow,
    new_workflow: Workflow,
    original_elem: Step | Port,
    new_elem: Step | Port,
):
    """This method asserts that the original entities and the new entities have different persistent ids."""
    assert original_workflow.persistent_id is not None
    assert new_workflow.persistent_id is not None
    assert original_workflow.persistent_id != new_workflow.persistent_id
    if isinstance(original_elem, Step):
        assert new_elem.name in new_workflow.steps.keys()
    elif isinstance(original_elem, Port):
        assert new_elem.name in new_workflow.ports.keys()
    else:
        raise ValueError(
            f"Invalid input element; accepted values are `Step` and `Port`, but got `{type(original_elem)}`."
        )
    assert original_elem.persistent_id is not None
    assert new_elem.persistent_id is not None
    assert original_elem.persistent_id != new_elem.persistent_id
    assert new_elem.workflow.persistent_id == new_workflow.persistent_id


async def create_and_run_step(
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
    # Create step
    step = workflow.create_step(cls=step_cls, **kwargs_step)
    step.add_input_port(port_name, in_port)
    step.add_output_port(port_name, out_port)
    # Inject inputs
    await inject_tokens(token_list, in_port, context, save_input_token)
    # Save the workflow in the database and execute
    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    return step


async def duplicate_and_test(
    workflow: Workflow,
    step_cls: type[Step],
    kwargs_step: MutableMapping[str, Any],
    context: StreamFlowContext,
    test_are_eq: bool = True,
):
    step = workflow.create_step(cls=step_cls, **kwargs_step)
    await workflow.save(context)
    new_workflow, new_step = await duplicate_elements(step, workflow, context)
    check_persistent_id(workflow, new_workflow, step, new_step)
    if test_are_eq:
        for p1, p2 in zip(workflow.ports.values(), new_workflow.ports.values()):
            assert p1.persistent_id != p2.persistent_id
            assert p1.workflow.name != p2.workflow.name
        for p in workflow.ports.values():
            p.persistent_id = None
            p.workflow = None
        for p in new_workflow.ports.values():
            p.persistent_id = None
            p.workflow = None
        set_attributes_to_none(step, set_id=True, set_wf=True)
        set_attributes_to_none(new_step, set_id=True, set_wf=True)
        assert are_equals(step, new_step)
        return None, None, None
    else:
        return step, new_workflow, new_step


async def duplicate_elements(
    step: Step, workflow: Workflow, context: StreamFlowContext
):
    new_workflow = Workflow(context=context, name=utils.random_name(), config={})
    loading_context = WorkflowBuilder(workflow=new_workflow)
    new_step = await loading_context.load_step(context, step.persistent_id)
    new_workflow.steps[new_step.name] = new_step

    # Ports are not loaded in new_workflow. It is necessary to do it manually
    for port in workflow.ports.values():
        new_port = await loading_context.load_port(context, port.persistent_id)
        new_workflow.ports[new_port.name] = new_port
    await new_workflow.save(context)
    return new_workflow, new_step


async def inject_tokens(
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


def inject_workflow_combinator(combinator: Combinator, new_workflow: Workflow | None):
    """Replace in the combinator the value of the `workflow` attribute with `new_workflow`."""
    combinator.workflow = new_workflow
    for c in combinator.combinators.values():
        inject_workflow_combinator(c, new_workflow)


def set_attributes_to_none(
    elem: Step | Port, set_id: bool = False, set_wf: bool = False
):
    if set_id:
        elem.persistent_id = None
    if set_wf:
        elem.workflow = None


async def verify_dependency_tokens(
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
    if logger.isEnabledFor(logging.DEBUG) and {
        t.persistent_id for t in depender_list
    } != {t.persistent_id for t in expected_depender}:
        logger.debug(
            f"Token id {token.persistent_id} has {[t.persistent_id for t in depender_list]} dependers while "
            f" the expected dependers are: {[t.persistent_id for t in expected_depender]}"
        )
    assert len(depender_list) == len(expected_depender)
    for t1 in depender_list:
        assert _contains_token_id(t1.persistent_id, expected_depender)

    dependee_list = await load_dependee_tokens(
        token.persistent_id, context, loading_context
    )
    if logger.isEnabledFor(logging.DEBUG) and {
        t.persistent_id for t in dependee_list
    } != {t.persistent_id for t in expected_dependee}:
        logger.debug(
            f"Token id {token.persistent_id} has {[t.persistent_id for t in depender_list]} dependee while "
            f" the expected dependee are: {[t.persistent_id for t in expected_dependee]}"
        )
    try:
        assert len(dependee_list) == len(expected_dependee)
        for t1 in dependee_list:
            assert _contains_token_id(t1.persistent_id, expected_dependee)
    except AssertionError as err:
        if alternative_expected_dependee is None:
            raise err
        else:
            assert len(dependee_list) == len(alternative_expected_dependee)
            for t1 in dependee_list:
                assert _contains_token_id(
                    t1.persistent_id, alternative_expected_dependee
                )
