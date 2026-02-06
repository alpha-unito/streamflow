from __future__ import annotations

import asyncio
import itertools
import os
import posixpath
import tempfile
import uuid
from collections.abc import AsyncGenerator, MutableMapping, MutableSequence
from typing import Any

import pytest
import pytest_asyncio

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import ExecutionLocation
from streamflow.core.utils import compare_tags, get_job_tag
from streamflow.core.workflow import Job, Status, Token
from streamflow.data.remotepath import StreamFlowPath
from streamflow.main import build_context
from streamflow.workflow.combinator import LoopCombinator
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.step import GatherStep, LoopCombinatorStep, ScatterStep
from streamflow.workflow.token import (
    FileToken,
    IterationTerminationToken,
    JobToken,
    ListToken,
    ObjectToken,
    TerminationToken,
)
from tests.utils.deployment import get_deployment_config, get_location
from tests.utils.utils import duplicate_elements, inject_tokens
from tests.utils.workflow import (
    BaseFileToken,
    InjectorFailureCommand,
    RecoveryTranslator,
    create_workflow,
    random_job_name,
)

FAILURE_STEP = ["execute", "transfer", "schedule"]
NUM_STEPS = {"single_step": 1, "pipeline": 4}
NUM_FAILURES = {"one_failure": 1, "two_failures_in_row": 2}
ERROR_TYPE = [InjectorFailureCommand.SOFT_ERROR, InjectorFailureCommand.FAIL_STOP]
TOKEN_TYPE = ["primitive", "file", "list", "object"]


async def _assert_token_result(
    input_value: Any,
    output_token: Token,
    context: StreamFlowContext,
    location: ExecutionLocation,
) -> None:
    if isinstance(output_token, FileToken):
        path = await StreamFlowPath(
            output_token.value, context=context, location=location
        ).resolve()
        assert path is not None
        assert await path.is_file()
        res = input_value.get("checksum") == f"sha1${await path.checksum()}"
        assert res
        assert input_value.get("size") == await path.size()
    elif isinstance(output_token, ListToken):
        for inner_value, inner_token in zip(
            input_value, output_token.value, strict=True
        ):
            await _assert_token_result(inner_value, inner_token, context, location)
    elif isinstance(output_token, ObjectToken):
        assert set(input_value.keys()) == set(output_token.value.keys())
        for key in input_value.keys():
            await _assert_token_result(
                input_value[key], output_token.value[key], context, location
            )
    else:
        assert input_value == output_token.value


async def _create_file(
    context: StreamFlowContext, location: ExecutionLocation, content: str | None = None
) -> MutableMapping[str, Any]:
    path = StreamFlowPath(
        tempfile.gettempdir() if location.local else "/tmp",
        utils.random_name(),
        context=context,
        location=location,
    )
    await path.write_text(
        content if content is not None else "StreamFlow fault tolerance"
    )
    path = await path.resolve()
    return {
        "basename": os.path.basename(path),
        "checksum": f"sha1${await path.checksum()}",
        "class": "File",
        "path": str(path),
        "size": await path.size(),
    }


async def _get_token_value(
    context: StreamFlowContext,
    location: ExecutionLocation,
    token_type: str,
    **kwargs,
) -> Any:
    if token_type == "primitive":
        return 100
    elif token_type == "file":
        return await _create_file(context, location)
    elif token_type == "list":
        return await asyncio.gather(
            *(
                asyncio.create_task(
                    _create_file(context, location, f"StreamFlow Manager: test {i}")
                )
                for i in range(int(kwargs.get("list_len", 3)))
            )
        )
    elif token_type == "object":
        return dict(
            zip(
                (
                    f"{i}-{utils.random_name()}"
                    for i in range(int(kwargs.get("obj_len", 3)))
                ),
                await asyncio.gather(
                    *(
                        asyncio.create_task(
                            _create_file(
                                context, location, f"StreamFlow Manager: test {i}"
                            )
                        )
                        for i in range(int(kwargs.get("obj_len", 3)))
                    )
                ),
            )
        )
    else:
        raise RuntimeError(f"Unknown token type: {token_type}")


@pytest_asyncio.fixture(scope="module")
async def fault_tolerant_context(
    chosen_deployment_types: MutableSequence[str],
) -> AsyncGenerator[StreamFlowContext, Any]:
    _context = build_context(
        {
            "failureManager": {
                "type": "default",
                "config": {"max_retries": 10, "retry_delay": 0},
            },
            "database": {"type": "default", "config": {"connection": ":memory:"}},
            "path": os.getcwd(),
        },
    )
    for deployment_t in (
        *chosen_deployment_types,
        "local-fs-volatile",
    ):
        config = await get_deployment_config(_context, deployment_t)
        await _context.deployment_manager.deploy(config)
    yield _context
    await _context.deployment_manager.undeploy_all()
    await _context.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "num_of_steps,failure_step,error_type,num_of_failures,token_type",
    itertools.product(
        NUM_STEPS.values(), FAILURE_STEP, ERROR_TYPE, NUM_FAILURES.values(), TOKEN_TYPE
    ),
    ids=[
        f"{n_step}_{step_t}_{error_t}_{n_failure}_{token_t}"
        for n_step, step_t, error_t, n_failure, token_t in itertools.product(
            NUM_STEPS.keys(), FAILURE_STEP, ERROR_TYPE, NUM_FAILURES.keys(), TOKEN_TYPE
        )
    ],
)
async def test_execute(
    fault_tolerant_context: StreamFlowContext,
    failure_step: str,
    error_type: str,
    num_of_failures: int,
    num_of_steps: int,
    token_type: str,
):
    deployment_t = "local-fs-volatile"
    workflow = next(iter(await create_workflow(fault_tolerant_context, num_port=0)))
    translator = RecoveryTranslator(workflow)
    deployment_config = await get_deployment_config(
        fault_tolerant_context, deployment_t
    )
    execution_location = await get_location(fault_tolerant_context, deployment_t)
    translator.deployment_configs = {deployment_config.name: deployment_config}
    input_ports = {}
    token_value = await _get_token_value(
        fault_tolerant_context, execution_location, token_type
    )
    input_name = f"test_in_{str(uuid.uuid1())}"
    output_name = f"test_out_{str(uuid.uuid1())}"
    injector_step = translator.get_base_injector_step(
        [deployment_config.name],
        input_name,
        posixpath.join(posixpath.sep, input_name),
        workflow,
    )
    input_ports[input_name] = injector_step.get_output_port(input_name)
    injector_step.get_input_port(input_name).put(Token(token_value, recoverable=True))
    injector_step.get_input_port(input_name).put(TerminationToken())
    execute_steps = []
    for i in range(num_of_steps):
        input_port = next(iter(input_ports.keys()))
        execute_steps.append(
            translator.get_execute_pipeline(
                command=f"lambda x : ('copy', '{token_type}', x['{input_port}'].value)",
                deployment_names=[deployment_config.name],
                input_ports=input_ports,
                outputs={output_name: token_type},
                step_name=os.path.join(posixpath.sep, str(i), utils.random_name()),
                workflow=workflow,
                failure_tags={"0": num_of_failures},
                failure_step=failure_step,
                failure_type=error_type,
            )
        )
        input_ports = execute_steps[-1].get_output_ports()
    await workflow.save(fault_tolerant_context)
    executor = StreamFlowExecutor(workflow)
    _ = await executor.run()
    # Check workflow output token
    assert all(s.status == Status.COMPLETED for s in workflow.steps.values())
    result_token = execute_steps[-1].get_output_port(output_name).token_list
    assert len(result_token) == 2
    await _assert_token_result(
        input_value=token_value,
        output_token=result_token[0],
        context=fault_tolerant_context,
        location=execution_location,
    )
    assert isinstance(result_token[1], TerminationToken)
    # Check number of retries of the steps
    for step in execute_steps:
        for job_name in map(
            lambda t: t.value.name,
            filter(
                lambda t: isinstance(t, JobToken),
                step.get_input_port("__job__").token_list,
            ),
        ):
            expected_failures = num_of_failures
            if error_type == InjectorFailureCommand.FAIL_STOP:
                if token_type != "primitive":
                    if num_of_steps > 1 and num_of_failures > 1:
                        # Domino effect for each failure
                        expected_failures = (
                            num_of_steps - int(step.name.split(os.sep)[1])
                        ) * 2
                    else:
                        # The failure of a step involves all the previous steps
                        expected_failures *= num_of_steps - int(
                            step.name.split(os.sep)[1]
                        )
                    if (
                        num_of_steps > 1
                        and failure_step == "schedule"
                        and step.name.split(os.sep)[1] != "0"
                    ):
                        # ExecuteStepA -> ScheduleStepB
                        # ExecuteStepA -> TransferStepB
                        # When the ScheduleStepB fails and the data are lost, the recover
                        # will roll back ExecuteStepA and re-execute ScheduleStepB,
                        # so a new token of ExecuteStepA is created. However, TransferStepB
                        # receives the old token. The transfer step fails and retries using
                        # the token generated by rollback of ExecuteStepA
                        expected_failures += 1
            # Version starts from 1
            retry_request = fault_tolerant_context.failure_manager.get_request(job_name)
            assert retry_request.version == expected_failures + 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prefix_tag,resume_from",
    itertools.product(("0", "0.1"), ("begin", "first", "half")),
)
async def test_resume_loop_combinator_step(
    context: StreamFlowContext, prefix_tag: str, resume_from: str
) -> None:
    input_names = ["in_a", "in_b", "in_c"]
    num_iter = 5
    workflow, ports = await create_workflow(
        context, num_port=len(input_names), type_="default"
    )
    input_ports = {name: port for name, port in zip(input_names, ports, strict=True)}
    step_name = posixpath.join(posixpath.sep, utils.random_name())
    loop_combinator = LoopCombinator(
        workflow=workflow, name=step_name + "-loop-combinator"
    )
    for port_name in input_ports:
        loop_combinator.add_item(port_name)
    combinator_step = workflow.create_step(
        cls=LoopCombinatorStep,
        name=step_name + "-loop-combinator",
        combinator=loop_combinator,
    )
    for port_name, port in input_ports.items():
        combinator_step.add_input_port(port_name, port)
        await inject_tokens(
            token_list=[
                Token(
                    value=f"{port_name}_a",
                    tag=prefix_tag,
                ),
                TerminationToken(Status.COMPLETED),
                *(
                    Token(value=f"{port_name}_a", tag=f"{prefix_tag}.{i}")
                    for i in range(num_iter)
                ),
                IterationTerminationToken(tag=prefix_tag),
            ],
            in_port=port,
            context=context,
        )
        combinator_step.add_output_port(port_name, workflow.create_port())
    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    # Duplicate the combinator and resume it
    new_workflow, new_combinator_step = await duplicate_elements(
        combinator_step, workflow, context
    )
    match resume_from:
        case "begin":
            restart_idx = 0
        case "first":
            restart_idx = 1
        case "half":
            restart_idx = num_iter // 2
        case _:
            raise NotImplementedError

    # Inject input tokens, resume and execute the new workflow
    for port_name, port in new_combinator_step.get_input_ports().items():
        tokens = [
            t
            for t in combinator_step.get_input_port(port_name).token_list
            if not isinstance(t, (TerminationToken, IterationTerminationToken))
        ]
        tag = tokens[restart_idx].tag
        await inject_tokens(
            token_list=[
                Token(
                    value=f"{port_name}_a",
                    tag=tag,
                ),
                IterationTerminationToken(tag),
            ],
            in_port=port,
            context=context,
        )

    await new_combinator_step.restore(
        on_tokens={
            name: [
                token
                for token in port.token_list[restart_idx : len(port.token_list)]
                if not isinstance(token, (TerminationToken, IterationTerminationToken))
            ]
            for name, port in combinator_step.get_output_ports().items()
        }
    )
    await new_workflow.save(context)
    executor = StreamFlowExecutor(new_workflow)
    await executor.run()
    # Test input values                            -  New_combinator_step port tags
    # prefix='0'     resume_from='begin' ('0')     -  input='0'     output='0.0'
    # prefix='0'     resume_from='first' ('0.0')   -  input='0.0'   output='0.1'
    # prefix='0'     resume_from='half' ('0.1')    -  input='0.1'   output='0.2'
    # prefix='0.1'   resume_from='begin' ('0.1')   -  input='0.1'   output='0.1.0'
    # prefix='0.1'   resume_from='first' ('0.1.0') -  input='0.1.0' output='0.1.1'
    # prefix='0.1'   resume_from='half' ('0.1.1')  -  input='0.1.1' output='0.1.2'
    for port_name, new_port in new_combinator_step.get_output_ports().items():
        old_port = combinator_step.get_output_port(port_name)
        # Expected: num_iter + IterationTerminationToken + TerminationToken
        assert len(old_port.token_list) == num_iter + 2
        # Expected: Token + TerminationToken
        assert len(new_port.token_list) == 2
        assert isinstance(new_port.token_list[-1], TerminationToken)
        assert old_port.token_list[restart_idx].tag == new_port.token_list[-2].tag


@pytest.mark.asyncio
async def test_resume_scatter_step(context: StreamFlowContext) -> None:
    input_name = "in_a"
    scatter_size = 6
    prefix_tag = "0.0"
    step_name = posixpath.join(posixpath.sep, utils.random_name())
    workflow, _ = await create_workflow(context, num_port=0, type_="default")
    scatter_step = workflow.create_step(
        cls=ScatterStep, name=posixpath.join(step_name, input_name) + "-scatter"
    )
    scatter_step.add_input_port(input_name, workflow.create_port())
    scatter_step.add_output_port(input_name, workflow.create_port())
    # Inject input tokens
    await inject_tokens(
        token_list=[
            ListToken(
                value=[Token(value=f"a_{i}") for i in range(scatter_size)],
                tag=prefix_tag,
            )
        ],
        in_port=scatter_step.get_input_port(input_name),
        context=context,
    )
    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    # Duplicate the step and resume it
    new_workflow, new_scatter_step = await duplicate_elements(
        scatter_step, workflow, context
    )
    output_port = scatter_step.get_output_port(input_name)
    await new_scatter_step.restore(
        on_tokens={
            output_port.name: [
                t
                for i, t in enumerate(output_port.token_list)
                if i % 2 == 0 and not isinstance(t, TerminationToken)
            ]
        }
    )
    await inject_tokens(
        token_list=[
            ListToken(
                value=[Token(value=f"a_{i}") for i in range(scatter_size)],
                tag=prefix_tag,
            )
        ],
        in_port=new_scatter_step.get_input_port(input_name),
        context=context,
    )
    await new_workflow.save(context)
    executor = StreamFlowExecutor(new_workflow)
    await executor.run()
    # Check size_port
    size_port = scatter_step.get_size_port()
    new_size_port = new_scatter_step.get_size_port()
    assert len(size_port.token_list) == len(new_size_port.token_list) == 2
    assert isinstance(size_port.token_list[-1], TerminationToken)
    assert isinstance(new_size_port.token_list[-1], TerminationToken)
    assert (
        size_port.token_list[0].value
        == new_size_port.token_list[0].value
        == scatter_size
    )
    # Check output port
    new_output_port = new_scatter_step.get_output_port(input_name)
    assert isinstance(output_port.token_list[-1], TerminationToken)
    assert isinstance(new_output_port.token_list[-1], TerminationToken)
    assert len(output_port.token_list) == scatter_size + 1
    assert len(new_output_port.token_list) == (scatter_size // 2) + 1
    for i, token in enumerate(output_port.token_list[:-1]):
        assert token.tag == f"{prefix_tag}.{i}"
        # The output port of the resumed step has only tokens with even tags
        if i % 2 == 0:
            assert token.tag == new_output_port.token_list[i // 2].tag


@pytest.mark.asyncio
async def test_scatter(fault_tolerant_context: StreamFlowContext):
    num_of_failures = 0
    deployment_t = "local-fs-volatile"
    workflow = next(iter(await create_workflow(fault_tolerant_context, num_port=0)))
    translator = RecoveryTranslator(workflow)
    deployment_config = await get_deployment_config(
        fault_tolerant_context, deployment_t
    )
    execution_location = await get_location(fault_tolerant_context, deployment_t)
    translator.deployment_configs = {deployment_config.name: deployment_config}
    input_name = f"test_in_{utils.random_name()}"
    output_name = f"test_out_{utils.random_name()}"
    injector_step = translator.get_base_injector_step(
        [deployment_t], input_name, posixpath.join(posixpath.sep, input_name), workflow
    )
    token_value = await _get_token_value(
        fault_tolerant_context, execution_location, "list", list_len=4
    )
    injector_step.get_input_port(input_name).put(Token(token_value, recoverable=True))
    injector_step.get_input_port(input_name).put(TerminationToken())
    # ExecuteStep before the scatter
    step = translator.get_execute_pipeline(
        command=f"lambda x : ('copy', 'list', x['{input_name}'].value)",
        deployment_names=[deployment_t],
        input_ports={input_name: injector_step.get_output_port(input_name)},
        outputs={output_name: "list"},
        step_name=os.path.join(posixpath.sep, "a", utils.random_name()),
        workflow=workflow,
    )
    # ExecuteStep inside the scatter
    scatter_step_name = os.path.join(posixpath.sep, "b", utils.random_name())
    scatter_step = workflow.create_step(
        cls=ScatterStep, name=f"{scatter_step_name}-scatter"
    )
    scatter_step.add_input_port(output_name, step.get_output_port(output_name))
    scatter_step.add_output_port(output_name, workflow.create_port())
    step = translator.get_execute_pipeline(
        command=f"lambda x : ('copy', 'list', x['{output_name}'].value)",
        deployment_names=[deployment_t],
        input_ports={output_name: scatter_step.get_output_port(output_name)},
        outputs={output_name: "file"},
        step_name=scatter_step_name,
        failure_step="execute",
        failure_tags={"0.2": num_of_failures},
        failure_type=InjectorFailureCommand.FAIL_STOP,
        workflow=workflow,
    )
    gather_step = workflow.create_step(
        cls=GatherStep,
        name=f"{scatter_step_name}-gather",
        size_port=scatter_step.get_size_port(),
    )
    gather_step.add_input_port(output_name, step.get_output_port(output_name))
    gather_step.add_output_port(output_name, workflow.create_port())
    # ExecuteStep after the gather
    step = translator.get_execute_pipeline(
        command=f"lambda x : ('copy', 'list', x['{output_name}'].value)",
        deployment_names=[deployment_t],
        input_ports=gather_step.get_output_ports(),
        outputs={output_name: "list"},
        step_name=os.path.join(posixpath.sep, "c", utils.random_name()),
        workflow=workflow,
    )
    # Run
    await workflow.save(fault_tolerant_context)
    executor = StreamFlowExecutor(workflow)
    _ = await executor.run()
    result_token = step.get_output_port(output_name).token_list
    assert len(result_token) == 2
    await _assert_token_result(
        input_value=token_value,
        output_token=result_token[0],
        context=fault_tolerant_context,
        location=await get_location(fault_tolerant_context, deployment_t),
    )
    assert isinstance(result_token[1], TerminationToken)

    for job_name in map(
        lambda t: t.value.name,
        filter(
            lambda t: isinstance(t, JobToken),
            step.get_input_port("__job__").token_list,
        ),
    ):
        retry_request = fault_tolerant_context.failure_manager.get_request(job_name)
        assert retry_request.version == num_of_failures + 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_on_iteration", [0, 1, 3])
async def test_loop(
    fault_tolerant_context: StreamFlowContext, failure_on_iteration: int
):
    num_of_failures = 1
    task = "execute"
    error_t = InjectorFailureCommand.FAIL_STOP
    deployment_t = "local-fs-volatile"
    max_iterations = 4
    workflow = next(iter(await create_workflow(fault_tolerant_context, num_port=0)))
    translator = RecoveryTranslator(workflow)
    translator.deployment_configs = {
        deployment_t: await get_deployment_config(fault_tolerant_context, deployment_t)
    }
    input_ports = {}
    execution_location = await get_location(fault_tolerant_context, deployment_t)
    token_type = "file"
    token_value = await _get_token_value(
        fault_tolerant_context, execution_location, token_type
    )
    for input_name, value in {
        "test": token_value,
        "counter": 0,
        "limit": max_iterations,
    }.items():
        injector_step = translator.get_base_injector_step(
            [deployment_t],
            input_name,
            posixpath.join(posixpath.sep, input_name),
            workflow,
        )
        input_ports[input_name] = injector_step.get_output_port(input_name)
        injector_step.get_input_port(input_name).put(Token(value, recoverable=True))
        injector_step.get_input_port(input_name).put(TerminationToken())
        workflow.input_ports[input_name] = injector_step.get_input_port(input_name)

    step_name = os.path.join(posixpath.sep, "0", utils.random_name())
    loop_input_ports = translator.get_input_loop(
        step_name,
        input_ports,
        'lambda x: x["counter"].value < x["limit"].value',
    )
    counter = translator.get_execute_pipeline(
        command="lambda x : ('inc', 'integer', x['counter'].value)",
        deployment_names=[deployment_t],
        input_ports={"counter": loop_input_ports["counter"]},
        outputs={"counter": "primitive"},
        step_name=os.path.join(posixpath.sep, "increment", utils.random_name()),
        workflow=workflow,
    )
    execute_step = translator.get_execute_pipeline(
        command=f"lambda x : ('copy', '{token_type}', x['test'].value)",
        deployment_names=[deployment_t],
        input_ports=loop_input_ports,
        outputs={"test1": token_type},
        step_name=step_name,
        workflow=workflow,
        failure_type=error_t,
        failure_step=task,
        failure_tags={
            f"0.{failure_on_iteration}": num_of_failures + failure_on_iteration
        },
    )
    output_ports = translator.get_output_loop(
        step_name,
        {
            "test": execute_step.get_output_port("test1"),
            "counter": counter.get_output_port("counter"),
            "limit": loop_input_ports["limit"],
        },
        {"test"},
    )
    await workflow.save(fault_tolerant_context)
    executor = StreamFlowExecutor(workflow)
    _ = await executor.run()
    assert len(output_ports) == 1
    result_token = next(iter(output_ports.values())).token_list
    assert len(result_token) == 2
    await _assert_token_result(
        input_value=token_value,
        output_token=result_token[0],
        context=fault_tolerant_context,
        location=await get_location(fault_tolerant_context, deployment_t),
    )
    assert isinstance(result_token[1], TerminationToken)

    for job_name in map(
        lambda t: t.value.name,
        filter(
            lambda t: isinstance(t, JobToken),
            execute_step.get_input_port("__job__").token_list,
        ),
    ):
        attempts = 1  # every job starts from 1
        if compare_tags(f"0.{failure_on_iteration}", get_job_tag(job_name)) > -1:
            attempts += num_of_failures + failure_on_iteration
        assert (
            fault_tolerant_context.failure_manager.get_request(job_name).version
            == attempts
        )


@pytest.mark.asyncio
async def test_synchro(fault_tolerant_context: StreamFlowContext):
    step_t = "execute"
    num_of_steps = 1
    num_of_failures = 1
    token_t = "file"
    deployment_t = "local-fs-volatile"
    workflow = next(iter(await create_workflow(fault_tolerant_context, num_port=0)))
    translator = RecoveryTranslator(workflow)
    deployment_config = await get_deployment_config(
        fault_tolerant_context, deployment_t
    )
    execution_location = await get_location(fault_tolerant_context, deployment_t)
    translator.deployment_configs = {deployment_config.name: deployment_config}
    input_ports = {}
    if token_t == "default":
        token_value = 100
    elif token_t == "file":
        token_value = await _create_file(fault_tolerant_context, execution_location)
    else:
        raise RuntimeError(f"Unknown token type: {token_t}")
    input_name = f"test_in_{utils.random_name()}"
    output_name = f"test_out_{utils.random_name()}"
    injector_step = translator.get_base_injector_step(
        [deployment_t], input_name, posixpath.join(posixpath.sep, input_name), workflow
    )
    input_ports[input_name] = injector_step.get_output_port(input_name)
    injector_step.get_input_port(input_name).put(Token(token_value, recoverable=True))
    injector_step.get_input_port(input_name).put(TerminationToken())
    execute_steps = []
    for _ in range(num_of_steps):
        execute_steps.append(
            translator.get_execute_pipeline(
                command=f"lambda x : ('copy', 'file', x['{input_name}'].value)",
                deployment_names=[deployment_t],
                input_ports=input_ports,
                outputs={output_name: token_t},
                step_name=os.path.join(posixpath.sep, utils.random_name()),
                workflow=workflow,
                failure_step=step_t,
                failure_tags={"0": num_of_failures},
                failure_type=InjectorFailureCommand.INJECT_TOKEN,
            )
        )
        input_ports = execute_steps[-1].get_output_ports()
    await workflow.save(fault_tolerant_context)
    executor = StreamFlowExecutor(workflow)
    _ = await executor.run()
    for step in execute_steps:
        result_token = step.get_output_port(output_name).token_list
        assert len(result_token) == 2
        await _assert_token_result(
            input_value=token_value,
            output_token=result_token[0],
            context=fault_tolerant_context,
            location=await get_location(fault_tolerant_context, deployment_t),
        )
        assert isinstance(result_token[1], TerminationToken)

        for job_name in map(
            lambda t: t.value.name,
            filter(
                lambda t: isinstance(t, JobToken),
                step.get_input_port("__job__").token_list,
            ),
        ):
            retry_request = fault_tolerant_context.failure_manager.get_request(job_name)
            # The job is not restarted, so it has number of version = 1
            assert retry_request.version == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("value_type", ["primitive", "file", "list", "object", "job"])
async def test_token_recoverable(value_type: str) -> None:
    """Test recoverable property of tokens."""
    if value_type == "primitive":
        token_cls = Token
        value = 101
    elif value_type == "file":
        token_cls = BaseFileToken
        value = "/path1"
    elif value_type == "list":
        token_cls = ListToken
        value = [Token(value=i, recoverable=True) for i in range(101, 104)]
    elif value_type == "object":
        token_cls = ObjectToken
        value = {f"key{i}": Token(value=i, recoverable=True) for i in range(101, 104)}
    elif value_type == "job":
        token_cls = JobToken
        value = Job(
            name=random_job_name(),
            workflow_id=1,
            inputs={},
            input_directory=None,
            tmp_directory=None,
            output_directory=None,
        )
    else:
        raise NotImplementedError
    token = token_cls(
        value=value,
        tag="0.0",
        recoverable=value_type not in ["list", "object"],
    )
    token = token.update(value=value)
    assert token.recoverable
    token.retag("0.1")
    assert token.recoverable
    token = token.update(value=value)
    token.recoverable = False
    assert not token.recoverable
    token = token.update(value=value)
    token.recoverable = True
    assert token.recoverable
