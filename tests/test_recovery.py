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
from streamflow.core.workflow import Job, Token
from streamflow.data.remotepath import StreamFlowPath
from streamflow.main import build_context
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.step import GatherStep, ScatterStep
from streamflow.workflow.token import (
    FileToken,
    JobToken,
    ListToken,
    ObjectToken,
    TerminationToken,
)
from tests.utils.deployment import get_deployment_config, get_location
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
        assert input_value.get("checksum") == f"sha1${await path.checksum()}"
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
    context: StreamFlowContext, location: ExecutionLocation
) -> MutableMapping[str, Any]:
    path = StreamFlowPath(
        tempfile.gettempdir() if location.local else "/tmp",
        utils.random_name(),
        context=context,
        location=location,
    )
    await path.write_text("StreamFlow fault tolerance")
    path = await path.resolve()
    return {
        "basename": os.path.basename(path),
        "checksum": f"sha1${await path.checksum()}",
        "class": "File",
        "path": str(path),
        "size": await path.size(),
    }


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
    if token_type == "primitive":
        token_value = 100
    elif token_type == "file":
        token_value = await _create_file(fault_tolerant_context, execution_location)
    elif token_type == "list":
        token_value = await asyncio.gather(
            *(
                asyncio.create_task(
                    _create_file(fault_tolerant_context, execution_location)
                )
                for _ in range(3)
            )
        )
    elif token_type == "object":
        token_value = dict(
            zip(
                ("a", "b", "c"),
                await asyncio.gather(
                    *(
                        asyncio.create_task(
                            _create_file(fault_tolerant_context, execution_location)
                        )
                        for _ in range(3)
                    )
                ),
                strict=True,
            )
        )

    else:
        raise RuntimeError(f"Unknown token type: {token_type}")
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
async def test_scatter(fault_tolerant_context: StreamFlowContext):
    num_of_failures = 1
    deployment_t = "local-fs-volatile"
    workflow = next(iter(await create_workflow(fault_tolerant_context, num_port=0)))
    translator = RecoveryTranslator(workflow)
    deployment_config = await get_deployment_config(
        fault_tolerant_context, deployment_t
    )
    execution_location = await get_location(fault_tolerant_context, deployment_t)
    translator.deployment_configs = {deployment_config.name: deployment_config}
    files = await asyncio.gather(
        *(
            asyncio.create_task(
                _create_file(fault_tolerant_context, execution_location)
            )
            for _ in range(4)
        )
    )
    input_name = f"test_in_{utils.random_name()}"
    output_name = f"test_out_{utils.random_name()}"
    injector_step = translator.get_base_injector_step(
        [deployment_t], input_name, posixpath.join(posixpath.sep, input_name), workflow
    )
    injector_step.get_input_port(input_name).put(Token(files, recoverable=True))
    injector_step.get_input_port(input_name).put(TerminationToken())
    # ExecuteStep
    step = translator.get_execute_pipeline(
        command=f"lambda x : ('copy', 'list', x['{input_name}'].value)",
        deployment_names=[deployment_t],
        input_ports={input_name: injector_step.get_output_port(input_name)},
        outputs={output_name: "list"},
        step_name=os.path.join(posixpath.sep, utils.random_name()),
        workflow=workflow,
    )
    # ScatterStep
    scatter_step = workflow.create_step(
        cls=ScatterStep, name=f"{utils.random_name()}-scatter"
    )
    scatter_step.add_input_port(output_name, step.get_output_port(output_name))
    scatter_step.add_output_port(output_name, workflow.create_port())
    # ExecuteStep
    step = translator.get_execute_pipeline(
        command=f"lambda x : ('copy', 'list', x['{output_name}'].value)",
        deployment_names=[deployment_t],
        input_ports={output_name: scatter_step.get_output_port(output_name)},
        outputs={output_name: "file"},
        step_name=os.path.join(posixpath.sep, utils.random_name()),
        workflow=workflow,
    )
    # GatherStep
    gather_step = workflow.create_step(
        cls=GatherStep,
        name=f"{utils.random_name()}-gather",
        size_port=scatter_step.get_size_port(),
    )
    gather_step.add_input_port(output_name, step.get_output_port(output_name))
    gather_step.add_output_port(output_name, workflow.create_port())
    # ExecuteStep
    step = translator.get_execute_pipeline(
        command=f"lambda x : ('copy', 'list', x['{output_name}'].value)",
        deployment_names=[deployment_t],
        input_ports=gather_step.get_output_ports(),
        outputs={output_name: "list"},
        step_name=os.path.join(posixpath.sep, utils.random_name()),
        workflow=workflow,
        failure_step="execute",
        failure_tags={"0": num_of_failures},
        failure_type=InjectorFailureCommand.FAIL_STOP,
    )
    # Run
    await workflow.save(fault_tolerant_context)
    executor = StreamFlowExecutor(workflow)
    _ = await executor.run()
    result_token = step.get_output_port(output_name).token_list
    assert len(result_token) == 2
    await _assert_token_result(
        input_value=files,
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
