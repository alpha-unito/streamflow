import itertools
import os
import posixpath
import tempfile
from collections.abc import AsyncGenerator
from typing import Any, cast

import pytest
import pytest_asyncio

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import ExecutionLocation
from streamflow.core.workflow import Token
from streamflow.data.remotepath import StreamFlowPath
from streamflow.main import build_context
from streamflow.recovery.failure_manager import DefaultFailureManager
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.token import FileToken, JobToken, TerminationToken
from tests.utils.deployment import get_deployment_config, get_location
from tests.utils.workflow import (
    InjectorFailureCommand,
    RecoveryTranslator,
    create_workflow,
)

STEP_T = ["execute", "transfer"]
NUM_STEPS = {"single_step": 1, "pipeline": 4}
NUM_FAILURES = {"one_failure": 1, "two_failures_in_row": 2}
TOKENS = ["default", "file"]


async def _assert_token_result(
    input_value: Any,
    output_token: Token,
    context: StreamFlowContext,
    location: ExecutionLocation,
) -> None:
    if isinstance(output_token, FileToken):
        path = StreamFlowPath(output_token.value, context=context, location=location)
        assert await path.is_file()
        assert input_value["basename"] == path.parts[-1]
        assert input_value.get("checksum") == f"sha1${await path.checksum()}"
        assert input_value.get("size") == await path.size()
    else:
        assert input_value == output_token.value


@pytest_asyncio.fixture(scope="module")
async def fault_tolerant_context(
    chosen_deployment_types,
) -> AsyncGenerator[StreamFlowContext, Any]:
    _context = build_context(
        {
            "failureManager": {
                "type": "default",
                "config": {"max_retries": 3, "retry_delay": 0},
            },
            "database": {"type": "default", "config": {"connection": ":memory:"}},
            "path": os.getcwd(),
        },
    )
    for deployment_t in (*chosen_deployment_types, "parameterizable_hardware"):
        config = await get_deployment_config(_context, deployment_t)
        await _context.deployment_manager.deploy(config)
    yield _context
    await _context.deployment_manager.undeploy_all()
    await _context.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "step_t,num_of_steps,num_of_failures,token_t",
    itertools.product(STEP_T, NUM_STEPS.values(), NUM_FAILURES.values(), TOKENS),
    ids=[
        f"{step_t}-{n_step}-{n_failure}-{t}"
        for step_t, n_step, n_failure, t in itertools.product(
            STEP_T, NUM_STEPS.keys(), NUM_FAILURES.keys(), TOKENS
        )
    ],
)
async def test_execute(
    fault_tolerant_context: StreamFlowContext,
    step_t: str,
    num_of_steps: int,
    num_of_failures: int,
    token_t: str,
):
    deployment_t = "local"
    workflow = next(iter(await create_workflow(fault_tolerant_context, num_port=0)))
    translator = RecoveryTranslator(workflow)
    translator.deployment_configs = {
        "local": await get_deployment_config(fault_tolerant_context, deployment_t)
    }
    input_ports = {}
    if token_t == "default":
        token_value = 100
        token = Token(token_value)
    elif token_t == "file":
        location = await get_location(fault_tolerant_context, deployment_t)
        path = StreamFlowPath(
            tempfile.gettempdir() if location.local else "/tmp",
            utils.random_name(),
            context=fault_tolerant_context,
            location=location,
        )
        await path.write_text("StreamFlow fault tolerance")
        token_value = {"class": "File", "path": str(path)}
        token = Token(dict(token_value))
        token_value["basename"] = os.path.basename(path)
        token_value["checksum"] = f"sha1${await path.checksum()}"
        token_value["size"] = await path.size()
    else:
        # TODO list, object
        raise RuntimeError(f"Unknown token type: {token_t}")
    for input_ in ("test",):
        injector_step = translator.get_base_injector_step(
            ["local"], input_, posixpath.join(posixpath.sep, input_), workflow
        )
        injector_step.recoverable = True
        input_ports[input_] = injector_step.get_output_port(input_)
        injector_step.get_input_port(input_).put(token)
        injector_step.get_input_port(input_).put(TerminationToken())
    execute_steps = []
    for _ in range(num_of_steps):
        execute_steps.append(
            translator.get_execute_pipeline(
                workflow,
                os.path.join(posixpath.sep, utils.random_name()),
                ["local"],
                input_ports,
                ["test1"],
                transfer_failures=num_of_failures if step_t == "transfer" else 0,
            )
        )
        execute_steps[-1].command = InjectorFailureCommand(
            execute_steps[-1], {"0": num_of_failures} if step_t == "execute" else {}
        )
        input_ports = execute_steps[-1].get_output_ports()
    await workflow.save(fault_tolerant_context)
    executor = StreamFlowExecutor(workflow)
    _ = await executor.run()
    for step in execute_steps:
        result_token = step.get_output_port("test1").token_list
        assert len(result_token) == 2
        await _assert_token_result(
            input_value=token_value,
            output_token=result_token[0],
            context=fault_tolerant_context,
            location=await get_location(fault_tolerant_context, deployment_t),
        )
        assert isinstance(result_token[1], TerminationToken)

        for job in (
            t.value
            for t in step.get_input_port("__job__").token_list
            if isinstance(t, JobToken)
        ):
            assert (
                job.name
                in cast(
                    DefaultFailureManager, fault_tolerant_context.failure_manager
                ).retry_requests
            )
            retry_request = cast(
                DefaultFailureManager, fault_tolerant_context.failure_manager
            ).retry_requests[job.name]
            assert retry_request.version == num_of_failures + 1


# TODO
# test_ephemeral_volumes
# test_scatter
# test_loop
# test_sync
# test_conditional
