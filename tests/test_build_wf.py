from typing import cast

import pytest

from streamflow.core import utils
from streamflow.core.config import BindingConfig
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import FilterConfig, LocalTarget
from streamflow.core.workflow import Port
from streamflow.cwl.command import CWLCommand, CWLCommandTokenProcessor
from streamflow.cwl.translator import create_command_output_processor_base
from streamflow.cwl.workflow import CWLWorkflow
from streamflow.persistence.loading_context import WorkflowBuilder
from streamflow.workflow.port import ConnectorPort, JobPort
from streamflow.workflow.step import ExecuteStep, GatherStep, ScatterStep
from tests.conftest import are_equals
from tests.utils.cwl import get_cwl_parser
from tests.utils.utils import (
    check_combinators,
    check_persistent_id,
    duplicate_and_test,
    duplicate_elements,
    inject_workflow_combinator,
    set_attributes_to_none,
)
from tests.utils.workflow import (
    CWL_VERSION,
    create_deploy_step,
    create_schedule_step,
    create_workflow,
    get_combinator_step,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "combinator_t",
    [
        "cartesian_product_combinator",
        "dot_combinator",
        "loop_combinator",
        "loop_termination_combinator",
        "nested_crossproduct",
    ],
)
async def test_combinator_step(context: StreamFlowContext, combinator_t: str):
    """Test saving CombinatorStep on database and re-load it in a new Workflow"""
    workflow, (in_port, out_port, in_port_2, out_port_2) = await create_workflow(
        context, num_port=4
    )
    step = get_combinator_step(workflow, combinator_t)
    inject_workflow_combinator(step.combinator, workflow)
    port_name = "test"
    step.add_input_port(port_name, in_port)
    step.add_output_port(port_name, out_port)

    port_name_2 = f"{port_name}_2"
    step.add_input_port(port_name_2, in_port_2)
    step.add_output_port(port_name_2, out_port_2)

    await workflow.save(context)
    new_workflow, new_step = await duplicate_elements(step, workflow, context)
    check_persistent_id(workflow, new_workflow, step, new_step)

    set_attributes_to_none(step, set_id=True, set_wf=True)
    set_attributes_to_none(new_step, set_id=True, set_wf=True)
    check_combinators(step.combinator, new_step.combinator)
    inject_workflow_combinator(step.combinator, None)
    inject_workflow_combinator(new_step.combinator, None)
    assert are_equals(step, new_step)


@pytest.mark.asyncio
async def test_deploy_step(context: StreamFlowContext):
    """Test saving DeployStep on database and re-load it in a new Workflow"""
    workflow, _ = await create_workflow(context, num_port=0)
    step = create_deploy_step(workflow)
    await workflow.save(context)
    new_workflow, new_step = await duplicate_elements(step, workflow, context)
    check_persistent_id(workflow, new_workflow, step, new_step)


@pytest.mark.asyncio
async def test_execute_step(context: StreamFlowContext):
    """Test saving ExecuteStep on database and re-load it in a new Workflow"""
    workflow, (job_port, in_port, out_port) = await create_workflow(context, num_port=3)

    in_port_name = "in-1"
    out_port_name = "out-1"
    step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=cast(JobPort, job_port)
    )
    step.command = CWLCommand(
        step=step,
        base_command=["echo"],
        processors=[CWLCommandTokenProcessor(name=in_port_name, expression=None)],
    )
    step.add_output_port(
        out_port_name,
        out_port,
        create_command_output_processor_base(
            port_name=out_port.name,
            workflow=cast(CWLWorkflow, workflow),
            port_target=None,
            port_type="string",
            cwl_element=get_cwl_parser(CWL_VERSION).CommandOutputParameter(
                type_="string"
            ),
            context={"hints": {}, "requirements": {}, "version": CWL_VERSION},
        ),
    )
    step.add_input_port(in_port_name, in_port)
    await workflow.save(context)
    new_workflow, new_step = await duplicate_elements(step, workflow, context)
    check_persistent_id(workflow, new_workflow, step, new_step)

    assert step.command.step.persistent_id != new_step.command.step.persistent_id
    step.command.step = None
    new_step.command.step = None
    for original_processor, new_processor in zip(
        step.output_processors.values(),
        new_step.output_processors.values(),
        strict=True,
    ):
        assert (
            original_processor.workflow.persistent_id
            != new_processor.workflow.persistent_id
        )
        set_attributes_to_none(original_processor, set_wf=True)
        set_attributes_to_none(new_processor, set_wf=True)
    set_attributes_to_none(step, set_id=True, set_wf=True)
    set_attributes_to_none(new_step, set_id=True, set_wf=True)
    assert are_equals(step, new_step)


@pytest.mark.asyncio
async def test_gather_step(context: StreamFlowContext):
    """Test saving GatherStep on database and re-load it in a new Workflow"""
    workflow, (port,) = await create_workflow(context, num_port=1)
    await duplicate_and_test(
        workflow,
        GatherStep,
        {"name": utils.random_name() + "-gather", "depth": 1, "size_port": port},
        context,
    )


@pytest.mark.asyncio
async def test_scatter_step(context: StreamFlowContext):
    """Test saving ScatterStep on database and re-load it in a new Workflow"""
    workflow, _ = await create_workflow(context, num_port=0)
    await duplicate_and_test(
        workflow, ScatterStep, {"name": utils.random_name() + "-scatter"}, context
    )


@pytest.mark.asyncio
async def test_schedule_step(context: StreamFlowContext):
    """Test saving ScheduleStep on database and re-load it in a new Workflow"""
    workflow, _ = await create_workflow(context, num_port=0)
    deploy_step = create_deploy_step(workflow)
    nof_deployments = 2
    step = create_schedule_step(
        workflow,
        deploy_steps=[deploy_step for _ in range(nof_deployments)],
        binding_config=BindingConfig(
            targets=[LocalTarget() for _ in range(nof_deployments)],
            filters=[
                FilterConfig(config={}, name=utils.random_name(), type="shuffle")
                for _ in range(nof_deployments)
            ],
        ),
    )
    await workflow.save(context)
    new_workflow, new_step = await duplicate_elements(step, workflow, context)
    check_persistent_id(workflow, new_workflow, step, new_step)

    for original_filter, new_filter in zip(
        step.binding_config.filters, new_step.binding_config.filters, strict=True
    ):
        # Config are read-only so workflows can share the same
        assert original_filter.persistent_id == new_filter.persistent_id
        set_attributes_to_none(original_filter, set_id=True, set_wf=False)
        set_attributes_to_none(new_filter, set_id=True, set_wf=False)
    set_attributes_to_none(step, set_id=True, set_wf=True)
    set_attributes_to_none(new_step, set_id=True, set_wf=True)
    assert are_equals(step, new_step)


@pytest.mark.asyncio
@pytest.mark.parametrize("port_cls", [Port, JobPort, ConnectorPort])
async def test_port(context: StreamFlowContext, port_cls: type[Port]):
    """Test saving Port on database and re-load it in a new Workflow"""
    workflow, ports = await create_workflow(context)
    port = workflow.create_port(port_cls)
    await workflow.save(context)
    assert workflow.persistent_id
    assert port.persistent_id

    loading_context = WorkflowBuilder(deep_copy=False)
    new_workflow = await loading_context.load_workflow(context, workflow.persistent_id)
    new_port = await loading_context.load_port(context, port.persistent_id)
    await new_workflow.save(context)
    assert len(new_workflow.ports) == 1
    check_persistent_id(workflow, new_workflow, port, new_port)
    set_attributes_to_none(port, set_id=True, set_wf=True)
    set_attributes_to_none(new_port, set_id=True, set_wf=True)
    assert are_equals(port, new_port)


@pytest.mark.asyncio
@pytest.mark.parametrize("copy_strategy", ["deep_copy", "copy", "manual_copy"])
async def test_workflow(context: StreamFlowContext, copy_strategy: str):
    """Test saving Workflow on database and load its elements in a new Workflow"""
    workflow, (job_port, in_port, out_port) = await create_workflow(context, num_port=3)

    in_port_name = "in-1"
    out_port_name = "out-1"
    exec_step = workflow.create_step(
        cls=ExecuteStep, name=utils.random_name(), job_port=cast(JobPort, job_port)
    )
    exec_step.command = CWLCommand(
        step=exec_step,
        base_command=["echo"],
        processors=[CWLCommandTokenProcessor(name=in_port_name, expression=None)],
    )
    exec_step.add_output_port(
        out_port_name,
        out_port,
        create_command_output_processor_base(
            port_name=out_port.name,
            workflow=cast(CWLWorkflow, workflow),
            port_target=None,
            port_type="string",
            cwl_element=get_cwl_parser(CWL_VERSION).CommandOutputParameter(
                type_="string"
            ),
            context={"hints": {}, "requirements": {}, "version": CWL_VERSION},
        ),
    )
    exec_step.add_input_port(in_port_name, in_port)
    await workflow.save(context)

    builder = WorkflowBuilder(copy_strategy == "deep_copy")
    new_workflow = await builder.load_workflow(context, workflow.persistent_id)
    assert new_workflow.name == workflow.name

    if copy_strategy == "manual_copy":
        assert len(new_workflow.steps) == len(new_workflow.ports) == 0
        await builder.load_step(context, exec_step.persistent_id)
        assert len(new_workflow.steps) == 1 and exec_step.name in new_workflow.steps
        assert len(new_workflow.ports) == len(exec_step.input_ports) + len(
            exec_step.output_ports
        )

    if copy_strategy in ("deep_copy", "manual_copy"):
        # Test that every object has the correct workflow reference
        for step in new_workflow.steps.values():
            assert step.workflow == new_workflow
        for port in new_workflow.steps.values():
            assert port.workflow == new_workflow

        for original_processor, new_processor in zip(
            exec_step.output_processors.values(),
            cast(
                ExecuteStep, new_workflow.steps[exec_step.name]
            ).output_processors.values(),
            strict=True,
        ):
            assert original_processor.workflow == workflow
            assert new_processor.workflow == new_workflow
            set_attributes_to_none(original_processor, set_wf=True)
            set_attributes_to_none(new_processor, set_wf=True)

        # Set some attributes in the `new_workflow` instance to none
        for new_step in new_workflow.steps.values():
            set_attributes_to_none(new_step, set_id=True, set_wf=True)
        for new_port in new_workflow.ports.values():
            set_attributes_to_none(new_port, set_id=True, set_wf=True)

        # Set some attributes in the `workflow` instance to none
        for step in workflow.steps.values():
            set_attributes_to_none(step, set_id=True, set_wf=True)
        for port in workflow.ports.values():
            set_attributes_to_none(port, set_id=True, set_wf=True)
    elif copy_strategy == "copy":
        workflow.steps = {}
        workflow.ports = {}
    # Test two workflows are the same (i.e. same steps and ports)
    set_attributes_to_none(workflow, set_id=True)
    assert are_equals(workflow, new_workflow)
