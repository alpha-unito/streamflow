import pytest

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.workflow.step import ExecuteStep
from tests.utils.workflow import create_workflow


@pytest.mark.asyncio
async def test_get_steps_queries(context: StreamFlowContext):
    """Test get_input_steps and get_output_steps queries"""
    workflow, (port_a, job_port, job_port_2, port_b, port_c) = await create_workflow(
        context, num_port=5
    )
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=job_port
    )
    step_2 = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=job_port_2
    )
    step.add_input_port("in", port_a)
    step.add_output_port("out", port_b)
    step_2.add_input_port("in2", port_b)
    step_2.add_output_port("out2", port_c)
    await workflow.save(context)

    input_steps_port_a = await context.database.get_input_steps(port_a.persistent_id)
    assert len(input_steps_port_a) == 0
    output_steps_port_a = await context.database.get_output_steps(port_a.persistent_id)
    assert len(output_steps_port_a) == 1
    assert output_steps_port_a[0]["step"] == step.persistent_id

    input_steps_port_b = await context.database.get_input_steps(port_b.persistent_id)
    assert len(input_steps_port_b) == 1
    assert input_steps_port_b[0]["step"] == step.persistent_id
    output_steps_port_b = await context.database.get_output_steps(port_b.persistent_id)
    assert len(output_steps_port_b) == 1
    assert output_steps_port_b[0]["step"] == step_2.persistent_id

    input_steps_port_c = await context.database.get_input_steps(port_c.persistent_id)
    assert len(input_steps_port_c) == 1
    assert input_steps_port_c[0]["step"] == step_2.persistent_id
    output_steps_port_c = await context.database.get_output_steps(port_c.persistent_id)
    assert len(output_steps_port_c) == 0
