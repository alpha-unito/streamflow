from __future__ import annotations

import asyncio
import inspect
import logging
import os
from collections.abc import MutableMapping, MutableSequence
from typing import Any

import pytest

from streamflow.core.context import StreamFlowContext
from streamflow.core.utils import contains_persistent_id
from streamflow.core.workflow import Port, Step, Token, Workflow
from streamflow.data.remotepath import StreamFlowPath
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


def get_full_instantiation(cls_: type[Any], **arguments) -> Any:
    """
    Instantiates a class using the provided arguments, checking whether the resulting
    instance has values that differ from the class's default values.

    All class arguments are mandatory in this instantiation. Moreover, values for parameters that have
    default values must be explicitly provided and must differ from those defaults.

    :param cls_: The class to instantiate.
    :param arguments: A mapping of argument names to values used for instantiation.
    :returns: An instance of the class, created using the provided arguments.
    """
    sig = inspect.signature(cls_.__init__)

    if new_args := {k for k in sig.parameters.keys() if k != "self"} - set(
        arguments.keys()
    ):
        pytest.fail(f"Object instantiation failed due to missing arguments: {new_args}")
    new_args = []
    for name, param in sig.parameters.items():
        if (
            name != "self"
            and param.default != inspect.Parameter.empty
            and arguments[name] == param.default
        ):
            new_args.append(name)
    if new_args:
        pytest.fail(
            f"Cannot create the object because default values were used for the following arguments: {new_args}"
        )
    return cls_(**arguments)


def check_combinators(
    original_combinator: Combinator, new_combinator: Combinator
) -> None:
    assert type(original_combinator) is type(new_combinator)
    assert (
        original_combinator.workflow.persistent_id
        != new_combinator.workflow.persistent_id
    )
    for original_inner, new_inner in zip(
        original_combinator.combinators.values(),
        new_combinator.combinators.values(),
        strict=True,
    ):
        check_combinators(original_inner, new_inner)


def check_persistent_id(
    original_workflow: Workflow,
    new_workflow: Workflow,
    original_elem: Step | Port,
    new_elem: Step | Port,
) -> None:
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


async def compare_remote_dirs(
    context: StreamFlowContext, src_path: StreamFlowPath, dst_path: StreamFlowPath
) -> None:
    assert await dst_path.exists()

    # The two directories must have the same order of elements
    src_path, src_dirs, src_files = await src_path.walk(
        # follow_symlinks=True
    ).__anext__()
    dst_path, dst_dirs, dst_files = await dst_path.walk(
        # follow_symlinks=True
    ).__anext__()
    assert len(src_files) == len(dst_files)
    for src_file, dst_file in zip(sorted(src_files), sorted(dst_files), strict=True):
        logger.info(
            f"dst_path: {dst_path}, dst_file: {dst_file}\n"
            f"Path exists? {dst_path / dst_file}: {await (dst_path / dst_file).exists()}"
        )
        assert await (dst_path / dst_file).exists()
        logger.info(
            f"Comparing {src_path / src_file}:{await (src_path / src_file).checksum()} "
            f"and {dst_path / dst_file}:{await (dst_path / dst_file).checksum()}"
        )
        assert (
            await (src_path / src_file).checksum()
            == await (dst_path / dst_file).checksum()
        )
    assert len(src_dirs) == len(dst_dirs)
    tasks = []
    for src_dir, dst_dir in zip(sorted(src_dirs), sorted(dst_dirs), strict=True):
        logger.info(
            f"dst_path: {dst_path}, dst_dir: {dst_dir}\n"
            f"Path {dst_path / dst_dir} exists? {await (dst_path / dst_dir).exists()}"
        )
        assert await (dst_path / dst_dir).exists()
        assert os.path.basename(src_dir) == os.path.basename(dst_dir)
        tasks.append(
            asyncio.create_task(
                compare_remote_dirs(context, src_path / src_dir, dst_path / dst_dir)
            )
        )
    await asyncio.gather(*tasks)


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
) -> tuple[Step, Workflow, Step] | tuple[None, None, None]:
    step = workflow.create_step(cls=step_cls, **kwargs_step)
    await workflow.save(context)
    new_workflow, new_step = await duplicate_elements(step, workflow, context)
    check_persistent_id(workflow, new_workflow, step, new_step)
    if test_are_eq:
        for p1, p2 in zip(
            workflow.ports.values(), new_workflow.ports.values(), strict=True
        ):
            assert type(p1) is type(p2)
            assert p1.persistent_id != p2.persistent_id
            assert p1.workflow.name == p2.workflow.name
        for p in workflow.ports.values():
            set_attributes_to_none(p, set_id=True, set_wf=True)
        for p in new_workflow.ports.values():
            set_attributes_to_none(p, set_id=True, set_wf=True)
        set_attributes_to_none(step, set_id=True, set_wf=True)
        set_attributes_to_none(new_step, set_id=True, set_wf=True)
        assert are_equals(step, new_step)
        return None, None, None
    else:
        return step, new_workflow, new_step


async def duplicate_elements(
    step: Step, workflow: Workflow, context: StreamFlowContext
) -> tuple[Workflow, Step]:
    loading_context = WorkflowBuilder(deep_copy=False)
    new_workflow = await loading_context.load_workflow(context, workflow.persistent_id)
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


def inject_workflow_combinator(
    combinator: Combinator, new_workflow: Workflow | None
) -> None:
    """Replace in the combinator the value of the `workflow` attribute with `new_workflow`."""
    combinator.workflow = new_workflow
    for c in combinator.combinators.values():
        inject_workflow_combinator(c, new_workflow)


def set_attributes_to_none(
    elem: Any, set_id: bool = False, set_wf: bool = False
) -> None:
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
) -> None:
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
        assert contains_persistent_id(t1.persistent_id, expected_depender)

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
            assert contains_persistent_id(t1.persistent_id, expected_dependee)
    except AssertionError as err:
        if alternative_expected_dependee is None:
            raise err
        else:
            assert len(dependee_list) == len(alternative_expected_dependee)
            for t1 in dependee_list:
                assert contains_persistent_id(
                    t1.persistent_id, alternative_expected_dependee
                )
