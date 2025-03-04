from __future__ import annotations

import os
import posixpath
from collections.abc import MutableSequence
from typing import TYPE_CHECKING

from streamflow.core import utils
from streamflow.core.config import BindingConfig
from streamflow.core.deployment import DeploymentConfig, Target
from streamflow.core.scheduling import HardwareRequirement
from streamflow.core.workflow import Port, Workflow
from streamflow.cwl.hardware import CWLHardwareRequirement
from streamflow.cwl.step import CWLScheduleStep
from streamflow.cwl.workflow import CWLWorkflow
from streamflow.workflow.combinator import (
    CartesianProductCombinator,
    DotProductCombinator,
    LoopCombinator,
    LoopTerminationCombinator,
)
from streamflow.workflow.port import ConnectorPort
from streamflow.workflow.step import (
    CombinatorStep,
    DeployStep,
    LoopCombinatorStep,
    ScheduleStep,
)
from tests.utils.deployment import get_docker_deployment_config

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext

CWL_VERSION = "v1.2"


def create_deploy_step(
    workflow: Workflow, deployment_config: DeploymentConfig | None = None
) -> DeployStep:
    connector_port = workflow.create_port(cls=ConnectorPort)
    if deployment_config is None:
        deployment_config = get_docker_deployment_config()
    return workflow.create_step(
        cls=DeployStep,
        name=posixpath.join("__deploy__", deployment_config.name),
        deployment_config=deployment_config,
        connector_port=connector_port,
    )


def create_schedule_step(
    workflow: Workflow,
    deploy_steps: MutableSequence[DeployStep],
    binding_config: BindingConfig = None,
    hardware_requirement: HardwareRequirement = None,
    name_prefix: str | None = None,
) -> ScheduleStep:
    # It is necessary to pass in the correct order biding_config.targets and deploy_steps for the mapping
    if not binding_config:
        binding_config = BindingConfig(
            targets=[
                Target(
                    deployment=deploy_step.deployment_config,
                )
                for deploy_step in deploy_steps
            ]
        )
    name_prefix = name_prefix or utils.random_name()
    return workflow.create_step(
        cls=(
            CWLScheduleStep
            if isinstance(hardware_requirement, CWLHardwareRequirement)
            else ScheduleStep
        ),
        name=posixpath.join(name_prefix, "__schedule__"),
        job_prefix=name_prefix,
        connector_ports={
            target.deployment.name: deploy_step.get_output_port()
            for target, deploy_step in zip(binding_config.targets, deploy_steps)
        },
        binding_config=binding_config,
        hardware_requirement=hardware_requirement,
    )


async def create_workflow(
    context: StreamFlowContext,
    num_port: int = 2,
    type_: str = "cwl",
) -> tuple[Workflow, tuple[Port, ...]]:
    if type_ == "cwl":
        workflow = CWLWorkflow(
            context=context,
            name=utils.random_name(),
            config={},
            cwl_version=CWL_VERSION,
        )
    else:
        workflow = Workflow(context=context, name=utils.random_name(), config={})
    ports = []
    for _ in range(num_port):
        ports.append(workflow.create_port())
    await workflow.save(context)
    return workflow, tuple(ports)


def get_cartesian_product_combinator(
    workflow: Workflow, name: str | None = None
) -> CartesianProductCombinator:
    return CartesianProductCombinator(
        name=name or utils.random_name(), workflow=workflow
    )


def get_combinator_step(workflow: Workflow, combinator_type: str) -> CombinatorStep:
    combinator_step_cls = CombinatorStep
    name = utils.random_name()
    if combinator_type == "cartesian_product_combinator":
        combinator = get_cartesian_product_combinator(workflow, name)
    elif combinator_type == "dot_combinator":
        combinator = get_dot_combinator(workflow, name)
    elif combinator_type == "loop_combinator":
        combinator_step_cls = LoopCombinatorStep
        combinator = get_loop_combinator(workflow, name)
    elif combinator_type == "loop_termination_combinator":
        combinator = get_loop_terminator_combinator(workflow, name)
    elif combinator_type == "nested_crossproduct":
        combinator = get_nested_crossproduct(workflow, name)
    else:
        raise ValueError(
            f"Invalid input combinator type: {combinator_type} is not supported"
        )
    return workflow.create_step(
        cls=combinator_step_cls,
        name=name + "-combinator",
        combinator=combinator,
    )


def get_dot_combinator(
    workflow: Workflow, name: str | None = None
) -> DotProductCombinator:
    return DotProductCombinator(name=name or utils.random_name(), workflow=workflow)


def get_loop_combinator(workflow: Workflow, name: str | None = None) -> LoopCombinator:
    return LoopCombinator(name=name or utils.random_name(), workflow=workflow)


def get_loop_terminator_combinator(
    workflow: Workflow, name: str | None = None
) -> LoopTerminationCombinator:
    c = LoopTerminationCombinator(name=name or utils.random_name(), workflow=workflow)
    c.add_output_item("test1")
    c.add_output_item("test2")
    return c


def get_nested_crossproduct(
    workflow: Workflow, name: str | None = None
) -> DotProductCombinator:
    combinator = DotProductCombinator(
        name=name or utils.random_name(), workflow=workflow
    )
    c1 = CartesianProductCombinator(name=name or utils.random_name(), workflow=workflow)
    c1.add_item("ext")
    c1.add_item("inn")
    items = c1.get_items(False)
    combinator.add_combinator(c1, items)
    return combinator


def random_job_name(step_name: str | None = None):
    step_name = step_name or utils.random_name()
    return os.path.join(posixpath.sep, step_name, "0.0")
