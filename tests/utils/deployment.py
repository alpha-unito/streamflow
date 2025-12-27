from __future__ import annotations

import asyncio
import json
import os
import socket
import tempfile
from collections.abc import MutableSequence
from importlib.resources import files
from typing import cast

import asyncssh
import asyncssh.public_key
from jinja2 import Template

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import (
    BindingFilter,
    Connector,
    DeploymentConfig,
    DeploymentManager,
    ExecutionLocation,
    Target,
    WrapsConfig,
)
from streamflow.core.utils import random_name
from streamflow.core.workflow import Job
from streamflow.deployment import DefaultDeploymentManager
from tests.utils.data import get_data_path


def _get_free_tcp_port() -> int:
    """
    Return a free TCP port to be used for publishing services.
    """
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.bind(("", 0))
    addr, port = tcp.getsockname()
    tcp.close()
    return port


def get_aiotar_deployment_config(tar_format: str) -> DeploymentConfig:
    workdir = os.path.join(
        os.path.realpath(tempfile.gettempdir()), "streamflow-test", random_name()
    )
    os.makedirs(workdir, exist_ok=True)
    return DeploymentConfig(
        name=f"aiotar-{tar_format}",
        type="aiotar",
        config={"tar_format": tar_format, "transferBufferSize": 16},
        external=False,
        lazy=False,
        workdir=workdir,
    )


def get_deployment(_context: StreamFlowContext, deployment_t: str) -> str:
    match deployment_t:
        case "aiotar":
            return "aiotar"
        case "docker":
            return "alpine-docker"
        case "docker-wrapper":
            return "alpine-docker-wrapper"
        case "docker-compose":
            return "alpine-docker-compose"
        case "kubernetes":
            return "alpine-kubernetes"
        case "local":
            return "__LOCAL__"
        case "local-fs-volatile":
            return "local-fs-volatile"
        case "parameterizable_hardware":
            return "custom-hardware"
        case "singularity":
            return "alpine-singularity"
        case "slurm":
            return "docker-slurm"
        case "ssh":
            return "linuxserver-ssh"
        case _:
            raise Exception(f"{deployment_t} deployment type not supported")


async def get_deployment_config(
    _context: StreamFlowContext, deployment_t: str
) -> DeploymentConfig:
    match deployment_t:
        case "docker":
            return get_docker_deployment_config()
        case "docker-compose":
            return get_docker_compose_deployment_config()
        case "docker-wrapper":
            return await get_docker_wrapper_deployment_config(_context)
        case "kubernetes":
            return get_kubernetes_deployment_config()
        case "local":
            return get_local_deployment_config()
        case "local-fs-volatile":
            return get_local_deployment_config(
                name="local-fs-volatile",
                workdir=os.path.join(
                    os.path.realpath(tempfile.gettempdir()),
                    "streamflow-test",
                    random_name(),
                    "test-fs-volatile",
                ),
            )
        case "docker":
            return get_docker_deployment_config()
        case "docker-compose":
            return get_docker_compose_deployment_config()
        case "docker-wrapper":
            return await get_docker_wrapper_deployment_config(_context)
        case "kubernetes":
            return get_kubernetes_deployment_config()
        case "parameterizable-hardware":
            return get_parameterizable_hardware_deployment_config()
        case "singularity":
            return get_singularity_deployment_config()
        case "slurm":
            return await get_slurm_deployment_config(_context)
        case "ssh":
            return await get_ssh_deployment_config(_context)
        case _:
            raise Exception(f"{deployment_t} deployment type not supported")


def get_docker_compose_deployment_config():
    return DeploymentConfig(
        name="alpine-docker-compose",
        type="docker-compose",
        config={
            "files": [
                str(get_data_path("deployment", "docker-compose", "docker-compose.yml"))
            ],
            "projectName": random_name(),
        },
        external=False,
        lazy=False,
    )


def get_docker_deployment_config():
    return DeploymentConfig(
        name="alpine-docker",
        type="docker",
        config={
            "image": "alpine:3.16.2",
            "volume": [
                f"{get_local_deployment_config().workdir}:/tmp/streamflow",
                f"{get_local_deployment_config().workdir}:/home/output",
            ],
        },
        external=False,
        lazy=False,
        workdir="/tmp/streamflow",
    )


async def get_docker_wrapper_deployment_config(_context: StreamFlowContext):
    docker_dind_deployment = DeploymentConfig(
        name="docker-dind",
        type="docker",
        config={"image": "docker:27.3.1-dind-alpine3.20", "privileged": True},
        external=False,
        lazy=False,
    )
    await _context.deployment_manager.deploy(docker_dind_deployment)
    await asyncio.sleep(5)
    return DeploymentConfig(
        name="alpine-docker-wrapper",
        type="docker",
        config={"image": "alpine:3.16.2"},
        external=False,
        lazy=False,
        wraps=WrapsConfig(deployment="docker-dind"),
    )


def get_failure_deployment_config():
    return DeploymentConfig(
        name="failure-test",
        type="failure-connector",
        config={"transferBufferSize": 0},
        external=False,
        lazy=True,
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


def get_local_deployment_config(
    name: str | None = None, workdir: str | None = None
) -> DeploymentConfig:
    workdir = workdir or os.path.join(
        os.path.realpath(tempfile.gettempdir()), "streamflow-test", random_name()
    )
    os.makedirs(workdir, exist_ok=True)
    return DeploymentConfig(
        name=name or "__LOCAL__",
        type="local",
        config={},
        external=True,
        lazy=False,
        workdir=workdir,
    )


async def get_location(
    _context: StreamFlowContext, deployment_t: str
) -> ExecutionLocation:
    deployment = get_deployment(_context, deployment_t)
    service = get_service(_context, deployment_t)
    connector = _context.deployment_manager.get_connector(deployment)
    locations = await connector.get_available_locations(service=service)
    return next(iter(locations.values())).location


def get_parameterizable_hardware_deployment_config():
    workdir = os.path.join(
        os.path.realpath(tempfile.gettempdir()), "streamflow-test", random_name()
    )
    os.makedirs(workdir, exist_ok=True)
    return DeploymentConfig(
        name="custom-hardware",
        type="parameterizable-hardware",
        config={},
        external=True,
        lazy=False,
        workdir=workdir,
    )


def get_service(_context: StreamFlowContext, deployment_t: str) -> str | None:
    match deployment_t:
        case (
            "aiotar"
            | "docker"
            | "docker-wrapper"
            | "local"
            | "local-fs-volatile"
            | "parameterizable-hardware"
            | "singularity"
            | "ssh"
        ):
            return None
        case "docker-compose":
            return "alpine"
        case "kubernetes":
            return "sf-test"
        case "slurm":
            return "test"
        case _:
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
            "projectName": random_name(),
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
    if config := cast(
        DefaultDeploymentManager, _context.deployment_manager
    ).config_map.get("linuxserver-ssh"):
        return config
    skey = asyncssh.public_key.generate_private_key(
        alg_name="ssh-rsa",
        comment="streamflow-test",
        key_size=4096,
    )
    public_key = skey.export_public_key().decode("utf-8")
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        skey.write_private_key(f.name)
    ssh_port = _get_free_tcp_port()
    docker_config = DeploymentConfig(
        name="linuxserver-ssh-docker",
        type="docker",
        config={
            "image": "lscr.io/linuxserver/openssh-server",
            "env": [f"PUBLIC_KEY={public_key}"],
            "init": False,
            "publish": [f"{ssh_port}:2222"],
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
                    "hostname": f"127.0.0.1:{ssh_port}",
                    "sshKey": f.name,
                    "username": "linuxserver.io",
                    "retries": 2,
                    "retryDelay": 5,
                }
            ],
            "maxConcurrentSessions": 10,
        },
        workdir="/tmp",
        external=False,
        lazy=False,
    )


class CustomDeploymentManager(DeploymentManager):
    def __init__(self, context: StreamFlowContext, my_arg: str) -> None:
        super().__init__(context)
        self.my_arg: str = my_arg

    async def close(self) -> None:
        raise NotImplementedError

    @classmethod
    def get_schema(cls) -> str:
        return json.dumps(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$id": "https://streamflow.di.unito.it/schemas/tests/utils/deployment/custom_deployment_manager.json",
                "type": "object",
                "properties": {
                    "my_arg": {"type": "string", "description": "No description"}
                },
                "additionalProperties": False,
            }
        )

    async def deploy(self, deployment_config: DeploymentConfig) -> None:
        raise NotImplementedError

    def get_connector(self, deployment_name: str) -> Connector | None:
        raise NotImplementedError

    async def undeploy(self, deployment_name: str) -> None:
        raise NotImplementedError

    async def undeploy_all(self) -> None:
        raise NotImplementedError


class ReverseTargetsBindingFilter(BindingFilter):
    async def get_targets(
        self, job: Job, targets: MutableSequence[Target]
    ) -> MutableSequence[Target]:
        return targets[::-1]

    @classmethod
    def get_schema(cls) -> str:
        return json.dumps(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$id": "https://streamflow.di.unito.it/schemas/tests/utils/deployment/reverse_targets.json",
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            }
        )
