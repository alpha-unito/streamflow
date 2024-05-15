import pytest

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.workflow import Token
from streamflow.cwl.command import CWLCommand
from streamflow.cwl.translator import _create_command_output_processor_base
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.step import ExecuteStep
from tests.test_provenance import _put_tokens
from tests.utils.workflow import create_deploy_step, create_schedule_step, create_workflow


@pytest.mark.asyncio
async def test_execute_step(context: StreamFlowContext):
    """Test execute an ExecuteStep with multiple jobs which will fail"""
    workflow, (in_port_schedule, in_port, out_port) = await create_workflow(
        context, num_port=3
    )
    deploy_step = create_deploy_step(workflow)
    schedule_step = create_schedule_step(workflow, [deploy_step])

    in_port_name = "in-1"
    out_port_name = "out-1"
    token_value = "Hello"

    execute_step = workflow.create_step(
        cls=ExecuteStep,
        name=utils.random_name(),
        job_port=schedule_step.get_output_port(),
    )
    execute_step.command = CWLCommand(
        step=execute_step,
        base_command=["exit", "1"],
    )
    execute_step.add_output_port(
        out_port_name,
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
    token_list = [Token(token_value) for _ in range(2)]

    execute_step.add_input_port(in_port_name, in_port)
    await _put_tokens(token_list, in_port, context)

    schedule_step.add_input_port(in_port_name, in_port_schedule)
    await _put_tokens(token_list, in_port_schedule, context)

    await workflow.save(context)
    executor = StreamFlowExecutor(workflow)
    with pytest.raises(WorkflowExecutionException) as exc_info:
        await executor.run()
    pass


