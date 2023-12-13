from __future__ import annotations

import asyncio
import os
import tempfile
from importlib_resources import files

from jinja2 import Template

import asyncssh
import asyncssh.public_key

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import (
    DeploymentConfig,
    LOCAL_LOCATION,
    Location,
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


def get_docker_deployment_config():
    return DeploymentConfig(
        name="alpine-docker",
        type="docker",
        config={"image": "alpine:3.16.2"},
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


async def get_location(_context: StreamFlowContext, deployment_t: str) -> Location:
    if deployment_t == "local":
        return Location(deployment=LOCAL_LOCATION, name=LOCAL_LOCATION)
    elif deployment_t == "docker":
        connector = _context.deployment_manager.get_connector("alpine-docker")
        locations = await connector.get_available_locations()
        return Location(deployment="alpine-docker", name=next(iter(locations.keys())))
    elif deployment_t == "kubernetes":
        connector = _context.deployment_manager.get_connector("alpine-kubernetes")
        locations = await connector.get_available_locations(service="sf-test")
        return Location(
            deployment="alpine-kubernetes",
            service="sf-test",
            name=next(iter(locations.keys())),
        )
    elif deployment_t == "singularity":
        connector = _context.deployment_manager.get_connector("alpine-singularity")
        locations = await connector.get_available_locations()
        return Location(
            deployment="alpine-singularity", name=next(iter(locations.keys()))
        )
    elif deployment_t == "ssh":
        connector = _context.deployment_manager.get_connector("linuxserver-ssh")
        locations = await connector.get_available_locations()
        return Location(deployment="linuxserver-ssh", name=next(iter(locations.keys())))
    else:
        raise Exception(f"{deployment_t} location type not supported")


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
