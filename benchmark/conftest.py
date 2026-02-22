from __future__ import annotations

import argparse
import os
import platform
import tempfile
from collections.abc import AsyncGenerator, Callable, Collection
from contextlib import suppress
from typing import Any, cast

import pytest
import pytest_asyncio

from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import ExecutionLocation
from streamflow.main import build_context
from tests.utils.deployment import get_deployment_config, get_location


def all_deployment_types() -> list[str]:
    """Get all deployment types based on platform."""
    deployments = ["local", "docker"]
    if platform.system() == "Linux":
        deployments.extend(["kubernetes", "singularity", "ssh"])
    return deployments


def csvtype(choices: Collection[str]) -> Callable[[str], list[str]]:
    """Return a function that splits and checks comma-separated values."""

    def splitarg(arg: str) -> list[str]:
        values = arg.split(",")
        for value in values:
            if value not in choices:
                raise argparse.ArgumentTypeError(
                    "invalid choice: {!r} (choose from {})".format(
                        value, ", ".join(map(repr, choices))
                    )
                )
        return values

    return splitarg


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add command line options for benchmarks."""
    group = parser.getgroup("benchmark deployment")
    group.addoption(
        "--deploys",
        type=csvtype(all_deployment_types()),
        default=None,
        help="List of deployments to benchmark. Use comma as delimiter e.g. --deploys "
        "local,docker. Defaults to all available deployments if no deployment flag is "
        "specified. Cannot be used with --local or --remote.",
    )
    group.addoption(
        "--local",
        action="store_true",
        default=False,
        help="Run benchmarks only on the local connector. "
        "Cannot be used with --deploys or --remote.",
    )
    group.addoption(
        "--remote",
        action="store_true",
        default=False,
        help="Run benchmarks on all connectors except local. "
        "Cannot be used with --deploys or --local.",
    )


def pytest_configure(config: pytest.Config) -> None:
    """Configure pytest-benchmark and add custom markers."""
    config.option.benchmark_sort = "name"
    config.option.benchmark_columns = ["mean", "min", "max", "stddev"]
    config.option.benchmark_name = "short"
    config.option.benchmark_group_by = "param:deployment"


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Remove [connector] suffix from test names for cleaner benchmark display."""
    for item in items:
        if "[" in item.name and hasattr(item, "callspec"):
            base_name = item.name.split("[")[0]
            params = item.name.split("[")[1].split("]")[0]
            item.name = (
                f"{base_name}[{params.split('-')[1]}]" if "-" in params else base_name
            )
            item._nodeid = item.nodeid.replace(f"[{item.callspec.id}]", "")


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Generate deployment parametrization based on platform and deployment flags."""
    if "deployment" in metafunc.fixturenames:
        deploys = metafunc.config.getoption("--deploys")
        local_flag = metafunc.config.getoption("--local")
        remote_flag = metafunc.config.getoption("--remote")

        flags_set = sum([deploys is not None, local_flag, remote_flag])
        if flags_set > 1:
            raise pytest.UsageError(
                "Only one of --deploys, --local, or --remote can be specified"
            )

        if local_flag:
            deployments = ["local"]
        elif remote_flag:
            all_deploys = all_deployment_types()
            deployments = [d for d in all_deploys if d != "local"]
        elif deploys is not None:
            deployments = deploys
        else:
            deployments = all_deployment_types()

        metafunc.parametrize("deployment", deployments, scope="session")


@pytest_asyncio.fixture(scope="session")
async def location(context: StreamFlowContext, deployment: str) -> ExecutionLocation:
    """
    Get execution location for deployment type.

    Gracefully skip if deployment is not available.
    """
    try:
        return await get_location(context, deployment)
    except Exception as e:
        pytest.skip(f"Deployment {deployment} not available: {e}")


@pytest.fixture(scope="session")
def connector_type(context: StreamFlowContext, location: ExecutionLocation) -> str:
    """
    Get connector type for the deployment.

    Returns the type string from the deployment config (e.g., 'local', 'docker', 'ssh').
    """
    from streamflow.deployment.manager import DefaultDeploymentManager

    manager = cast(DefaultDeploymentManager, context.deployment_manager)
    config = manager.config_map.get(location.deployment)
    return config.type if config else "unknown"


@pytest.fixture(scope="function")
def benchmark_tmpdir(location: ExecutionLocation) -> str:
    """
    Create isolated temp directory for each benchmark.

    For local deployments, uses /tmp/streamflow-benchmark.
    For remote deployments, uses /tmp directly since the container
    might not have /tmp/streamflow-benchmark.
    """
    if location.local:
        tmpdir = os.path.join(tempfile.gettempdir(), "streamflow-benchmark")
        os.makedirs(tmpdir, exist_ok=True)
        return tmpdir
    else:
        return "/tmp"


@pytest_asyncio.fixture(scope="session")
async def context(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[StreamFlowContext, Any]:
    """Create StreamFlowContext for benchmarks (replicates tests/conftest.py)."""
    _context = build_context(
        {
            "database": {"type": "default", "config": {"connection": ":memory:"}},
            "path": os.getcwd(),
        },
    )
    # Determine which deployments to deploy based on flags
    deploys = request.config.getoption("--deploys")
    local_flag = request.config.getoption("--local")
    remote_flag = request.config.getoption("--remote")

    if local_flag:
        deployments = ["local"]
    elif remote_flag:
        deployments = [d for d in all_deployment_types() if d != "local"]
    elif deploys is not None:
        deployments = deploys
    else:
        deployments = all_deployment_types()

    for deployment_t in deployments:
        with suppress(Exception):
            config = await get_deployment_config(_context, deployment_t)
            await _context.deployment_manager.deploy(config)

    yield _context
    await _context.deployment_manager.undeploy_all()
    await _context.close()
