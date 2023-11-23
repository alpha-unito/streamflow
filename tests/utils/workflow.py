from __future__ import annotations

import posixpath
from typing import MutableSequence, cast, TYPE_CHECKING

from streamflow.core import utils
from streamflow.core.deployment import Target
from streamflow.core.config import BindingConfig
from streamflow.workflow.port import ConnectorPort
from streamflow.core.workflow import Workflow, Port
from streamflow.workflow.step import DeployStep, ScheduleStep
from tests.utils.deployment import get_docker_deployment_config

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext


def create_deploy_step(workflow, deployment_config=None):
    connector_port = workflow.create_port(cls=ConnectorPort)
    if not deployment_config:
        deployment_config = get_docker_deployment_config()
    return workflow.create_step(
        cls=DeployStep,
        name=posixpath.join("__deploy__", deployment_config.name),
        deployment_config=deployment_config,
        connector_port=connector_port,
    )


def create_schedule_step(workflow, deploy_step, binding_config=None):
    if not binding_config:
        binding_config = BindingConfig(
            targets=[
                Target(
                    deployment=deploy_step.deployment_config,
                    workdir=utils.random_name(),
                )
            ]
        )
    return workflow.create_step(
        cls=ScheduleStep,
        name=posixpath.join(utils.random_name(), "__schedule__"),
        job_prefix="something",
        connector_ports={
            binding_config.targets[0].deployment.name: deploy_step.get_output_port()
        },
        binding_config=binding_config,
    )


async def create_workflow(
    context: StreamFlowContext, num_port: int = 2
) -> tuple[Workflow, tuple[Port]]:
    workflow = Workflow(
        context=context, type="cwl", name=utils.random_name(), config={}
    )
    ports = []
    for _ in range(num_port):
        ports.append(workflow.create_port())
    await workflow.save(context)
    return workflow, tuple(cast(MutableSequence[Port], ports))
