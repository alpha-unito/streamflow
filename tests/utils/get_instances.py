import asyncio
import os
import posixpath
import tempfile
from jinja2 import Template
from typing import MutableSequence, cast

import asyncssh
import asyncssh.public_key
import pkg_resources

from streamflow.core import utils
from streamflow.core.config import BindingConfig
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import Target, DeploymentConfig, LOCAL_LOCATION
from streamflow.core.workflow import Workflow, Port
from streamflow.workflow.combinator import (
    DotProductCombinator,
    LoopTerminationCombinator,
    CartesianProductCombinator,
)
from streamflow.workflow.port import ConnectorPort
from streamflow.workflow.step import DeployStep, ScheduleStep


def get_docker_deployment_config():
    return DeploymentConfig(
        name="alpine-docker",
        type="docker",
        config={"image": "alpine:3.16.2"},
        external=False,
        lazy=False,
    )


async def get_deployment_config(
    _context: StreamFlowContext, deployment_t: str
) -> DeploymentConfig:
    if deployment_t == "local":
        return get_local_deployment_config()
    elif deployment_t == "docker":
        return get_docker_deployment_config()
    elif deployment_t == "kubernetes":
        return get_kubernetes_deployment_config()
    elif deployment_t == "singularity":
        return get_singularity_deployment_config()
    elif deployment_t == "ssh":
        return await get_ssh_deployment_config(_context)
    else:
        raise Exception(f"{deployment_t} deployment type not supported")


def get_service(_context: StreamFlowContext, deployment_t: str) -> str | None:
    if deployment_t == "local":
        return None
    elif deployment_t == "docker":
        return None
    elif deployment_t == "kubernetes":
        return "sf-test"
    elif deployment_t == "singularity":
        return None
    elif deployment_t == "ssh":
        return None
    else:
        raise Exception(f"{deployment_t} deployment type not supported")


def get_kubernetes_deployment_config():
    with open(pkg_resources.resource_filename(__name__, "pod.jinja2")) as t:
        template = Template(t.read())
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        template.stream(name=utils.random_name()).dump(f.name)
    return DeploymentConfig(
        name="alpine-kubernetes",
        type="kubernetes",
        config={"files": [f.name]},
        external=False,
        lazy=False,
    )


def get_singularity_deployment_config():
    return DeploymentConfig(
        name="alpine-singularity",
        type="singularity",
        config={"image": "docker://alpine:3.16.2"},
        external=False,
        lazy=False,
    )


async def get_ssh_deployment_config(_context: StreamFlowContext):
    skey = asyncssh.public_key.generate_private_key(
        alg_name="ssh-rsa",
        comment="streamflow-test",
        key_size=4096,
    )
    public_key = skey.export_public_key().decode("utf-8")
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        skey.write_private_key(f.name)
    docker_config = DeploymentConfig(
        name="linuxserver-ssh-docker",
        type="docker",
        config={
            "image": "lscr.io/linuxserver/openssh-server",
            "env": [f"PUBLIC_KEY={public_key}"],
            "init": False,
            "publish": ["2222:2222"],
        },
        external=False,
        lazy=False,
    )
    await _context.deployment_manager.deploy(docker_config)
    await asyncio.sleep(5)
    return DeploymentConfig(
        name="linuxserver-ssh",
        type="ssh",
        config={
            "nodes": [
                {
                    "checkHostKey": False,
                    "hostname": "127.0.0.1:2222",
                    "sshKey": f.name,
                    "username": "linuxserver.io",
                }
            ],
            "maxConcurrentSessions": 10,
        },
        external=False,
        lazy=False,
    )


def get_local_deployment_config():
    return DeploymentConfig(
        name=LOCAL_LOCATION,
        type="local",
        config={},
        external=True,
        lazy=False,
        workdir=os.path.realpath(tempfile.gettempdir()),
    )


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


def create_schedule_step(
    workflow: Workflow,
    deploy_steps: MutableSequence[DeployStep],
    binding_config: BindingConfig = None,
):
    """It is necessary to pass in the correct order biding_config.targets and deploy_steps for the mapping"""
    if not binding_config:
        binding_config = BindingConfig(
            targets=[
                Target(
                    deployment=deploy_step.deployment_config,
                    workdir=utils.random_name(),
                )
                for deploy_step in deploy_steps
            ]
        )
    return workflow.create_step(
        cls=ScheduleStep,
        name=posixpath.join(utils.random_name(), "__schedule__"),
        job_prefix="something",
        connector_ports={
            target.deployment.name: deploy_step.get_output_port()
            for target, deploy_step in zip(binding_config.targets, deploy_steps)
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


def get_dot_combinator():
    return DotProductCombinator(name=utils.random_name(), workflow=None)


def get_cartesian_product_combinator():
    return CartesianProductCombinator(name=utils.random_name(), workflow=None)


def get_loop_terminator_combinator():
    c = LoopTerminationCombinator(name=utils.random_name(), workflow=None)
    c.add_output_item("test1")
    c.add_output_item("test2")
    return c


def get_nested_crossproduct():
    combinator = DotProductCombinator(name=utils.random_name(), workflow=None)
    c1 = CartesianProductCombinator(name=utils.random_name(), workflow=None)
    c1.add_item("ext")
    c1.add_item("inn")
    items = c1.get_items(False)
    combinator.add_combinator(c1, items)
    return combinator
