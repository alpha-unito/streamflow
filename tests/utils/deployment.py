from __future__ import annotations

import asyncio
import os
import tempfile

import asyncssh
import asyncssh.public_key
from importlib_resources import files
from jinja2 import Template

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import (
    DeploymentConfig,
    LOCAL_LOCATION,
    Location,
    WrapsConfig,
)
from tests.utils.data import get_data_path


async def get_deployment_config(
    _context: StreamFlowContext, deployment_t: str
) -> DeploymentConfig:
    if deployment_t == "local":
        return get_local_deployment_config()
    elif deployment_t == "docker":
        return get_docker_deployment_config()
    elif deployment_t == "docker-compose":
        return get_docker_compose_deployment_config()
    elif deployment_t == "kubernetes":
        return get_kubernetes_deployment_config()
    elif deployment_t == "singularity":
        return get_singularity_deployment_config()
    elif deployment_t == "slurm":
        return await get_slurm_deployment_config(_context)
    elif deployment_t == "ssh":
        return await get_ssh_deployment_config(_context)
    else:
        raise Exception(f"{deployment_t} deployment type not supported")


def get_docker_deployment_config():
    return DeploymentConfig(
        name="alpine-docker",
        type="docker",
        config={"image": "alpine:3.16.2"},
        external=False,
        lazy=False,
    )


def get_docker_compose_deployment_config():
    return DeploymentConfig(
        name="alpine-docker-compose",
        type="docker-compose",
        config={
            "files": [
                str(get_data_path("deployment", "docker-compose", "docker-compose.yml"))
            ]
        },
        external=False,
        lazy=False,
    )


def get_kubernetes_deployment_config():
    template = Template(files(__package__).joinpath("pod.jinja2").read_text("utf-8"))
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        template.stream(name=utils.random_name()).dump(f.name)
    return DeploymentConfig(
        name="alpine-kubernetes",
        type="kubernetes",
        config={"files": [f.name]},
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


def get_deployment(_context: StreamFlowContext, deployment_t: str) -> str:
    if deployment_t == "local":
        return LOCAL_LOCATION
    elif deployment_t == "docker":
        return "alpine-docker"
    elif deployment_t == "docker-compose":
        return "alpine-docker-compose"
    elif deployment_t == "kubernetes":
        return "alpine-kubernetes"
    elif deployment_t == "singularity":
        return "alpine-singularity"
    elif deployment_t == "slurm":
        return "docker-slurm"
    elif deployment_t == "ssh":
        return "linuxserver-ssh"
    else:
        raise Exception(f"{deployment_t} deployment type not supported")


async def get_location(_context: StreamFlowContext, deployment_t: str) -> Location:
    deployment = get_deployment(_context, deployment_t)
    service = get_service(_context, deployment_t)
    connector = _context.deployment_manager.get_connector(deployment)
    locations = await connector.get_available_locations(service=service)
    return Location(
        deployment=deployment,
        service=service,
        name=next(iter(locations.keys())),
    )


def get_service(_context: StreamFlowContext, deployment_t: str) -> str | None:
    if deployment_t == "local":
        return None
    elif deployment_t == "docker":
        return None
    elif deployment_t == "docker-compose":
        return "alpine"
    elif deployment_t == "kubernetes":
        return "sf-test"
    elif deployment_t == "singularity":
        return None
    elif deployment_t == "slurm":
        return "test"
    elif deployment_t == "ssh":
        return None
    else:
        raise Exception(f"{deployment_t} deployment type not supported")


def get_singularity_deployment_config():
    return DeploymentConfig(
        name="alpine-singularity",
        type="singularity",
        config={"image": "docker://alpine:3.16.2"},
        external=False,
        lazy=False,
    )


async def get_slurm_deployment_config(_context: StreamFlowContext):
    docker_compose_config = DeploymentConfig(
        name="docker-compose-slurm",
        type="docker-compose",
        config={
            "files": [str(get_data_path("deployment", "slurm", "docker-compose.yml"))],
            "projectName": "slurm",
        },
        external=False,
    )
    await _context.deployment_manager.deploy(docker_compose_config)
    return DeploymentConfig(
        name="docker-slurm",
        type="slurm",
        config={
            "services": {
                "test": {"partition": "docker", "nodes": 2, "ntasksPerNode": 1}
            }
        },
        external=False,
        lazy=False,
        wraps=WrapsConfig(deployment="docker-compose-slurm", service="slurmctld"),
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
