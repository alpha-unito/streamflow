import itertools
import os
import posixpath
from typing import cast

import pytest

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Token
from streamflow.recovery.failure_manager import DefaultFailureManager
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.token import JobToken, TerminationToken
from tests.utils.deployment import get_deployment_config
from tests.utils.workflow import (
    InjectorFailureCommand,
    RecoveryTranslator,
    create_workflow,
)

NUM_STEPS = {"single_step": 1, "pipeline": 4}
NUM_FAILURES = {"one_failure": 1, "two_failure_in_row": 2}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "num_of_steps,num_of_failures",
    itertools.product(NUM_STEPS.values(), NUM_FAILURES.values()),
    ids=[
        f"{n_step}-{n_failure}"
        for n_step, n_failure in itertools.product(
            NUM_STEPS.keys(), NUM_FAILURES.keys()
        )
    ],
)
async def test_execute(
    context: StreamFlowContext, num_of_steps: int, num_of_failures: int
):
    workflow = next(iter(await create_workflow(context, num_port=0)))
    translator = RecoveryTranslator(workflow)
    translator.deployment_configs = {
        "local": await get_deployment_config(workflow.context, "local")
    }
    input_ports = {}
    for input_ in ("test",):
        injector_step = translator.get_base_injector_step(
            ["local"], input_, posixpath.join(posixpath.sep, input_), workflow
        )
        input_ports[input_] = injector_step.get_output_port(input_)
        injector_step.get_input_port(input_).put(Token(100))
        injector_step.get_input_port(input_).put(TerminationToken())
    execute_steps = []
    for _ in range(num_of_steps):
        execute_steps.append(
            translator.get_exec_sub_workflow(
                workflow,
                os.path.join(posixpath.sep, utils.random_name()),
                ["local"],
                input_ports,
                ["test1"],
            )
        )
        execute_steps[-1].command = InjectorFailureCommand(
            execute_steps[-1], {"0": num_of_failures}
        )
        input_ports = execute_steps[-1].get_output_ports()
    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    _ = await executor.run()
    for step in execute_steps:
        result_token = step.get_output_port("test1").token_list
        assert len(result_token) == 2
        assert result_token[0].value == 100
        assert isinstance(result_token[1], TerminationToken)

        for job in (
            t.value
            for t in step.get_input_port("__job__").token_list
            if isinstance(t, JobToken)
        ):
            assert (
                job.name
                in cast(DefaultFailureManager, context.failure_manager).retry_requests
            )
            retry_request = cast(
                DefaultFailureManager, context.failure_manager
            ).retry_requests[job.name]
            assert retry_request.version == num_of_failures + 1


# TODO
# test_transfer
# test_pipeline / ephemeral volumes
# test_scatter
# test_loop
# test_sync
