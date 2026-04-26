from __future__ import annotations

import pathlib
from collections.abc import AsyncGenerator

import pytest_asyncio
from kubernetes_asyncio import client
from kubernetes_asyncio.client import ApiClient, ApiException
from kubernetes_asyncio.config import load_kube_config

from streamflow.main import main

_MPI_DIR = pathlib.Path(__file__).parent.parent / "source" / "examples" / "mpi"


def _run(streamflow_file: pathlib.Path) -> None:
    """Validate and execute the workflow described by the StreamFlow file."""
    assert main(["run", str(streamflow_file)]) == 0


@pytest_asyncio.fixture(scope="module")
async def k8s_ssh_secret() -> AsyncGenerator[None]:
    """Create the ``streamflow-ssh-key`` Kubernetes secret; delete on teardown."""
    key_dir = _MPI_DIR / "environment"
    await load_kube_config()
    async with ApiClient() as api_client:
        try:
            v1 = client.CoreV1Api(api_client=api_client)
            secret = client.V1Secret(
                metadata=client.V1ObjectMeta(name="streamflow-ssh-key"),
                string_data={
                    "id_rsa": (key_dir / "id_rsa").read_text(),
                    "id_rsa.pub": (key_dir / "id_rsa.pub").read_text(),
                    "authorized_keys": (key_dir / "id_rsa.pub").read_text(),
                },
            )
            await v1.create_namespaced_secret("default", secret)
            yield
        finally:
            v1 = client.CoreV1Api(api_client=api_client)
            try:
                await v1.delete_namespaced_secret("streamflow-ssh-key", "default")
            except ApiException as e:
                if e.status != 404:
                    raise


def test_mpi_docker_compose() -> None:
    """Run the MPI example workflow using the Docker Compose backend."""
    _run(_MPI_DIR / "streamflow.yml")


def test_mpi_kubernetes(k8s_ssh_secret: None) -> None:
    """Run the MPI example workflow using the Kubernetes backend."""
    _run(_MPI_DIR / "streamflow-k8s.yml")


def test_mpi_helm(k8s_ssh_secret: None) -> None:
    """Run the MPI example workflow using the Helm backend."""
    _run(_MPI_DIR / "streamflow-helm.yml")
