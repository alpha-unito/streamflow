from __future__ import annotations

import asyncio
import os

import asyncssh
import pytest
import pytest_asyncio

from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import Connector, Location
from streamflow.core.exception import WorkflowExecutionException
from streamflow.deployment.connector import SSHConnector

from streamflow.deployment.future import FutureConnector
from tests.utils.connector import FailureConnector, FailureConnectorException
from tests.utils.deployment import (
    get_failure_deployment_config,
    get_location,
    get_ssh_deployment_config,
)


def _get_task(connector: Connector, method: str) -> asyncio.Task:
    loc = Location("test-location", "failure-test")
    if method == "copy_local_to_remote":
        return asyncio.create_task(
            connector.copy_local_to_remote("test_src", "test_dst", [loc])
        )
    elif method == "copy_remote_to_local":
        return asyncio.create_task(
            connector.copy_remote_to_local("test_src", "test_dst", [loc])
        )
    elif method == "copy_remote_to_remote":
        return asyncio.create_task(
            connector.copy_remote_to_remote("test_src", "test_dst", [loc], loc)
        )
    elif method == "get_available_locations":
        return asyncio.create_task(connector.get_available_locations())
    elif method == "get_stream_reader":
        return asyncio.create_task(connector.get_stream_reader(loc, "test_src"))
    elif method == "run":
        return asyncio.create_task(connector.run(loc, ["ls"]))
    else:
        raise pytest.fail(f"Unknown method: {method}")


@pytest_asyncio.fixture(scope="module")
async def curr_location(context, deployment_src) -> Location:
    return await get_location(context, deployment_src)


@pytest.fixture(scope="module")
def curr_connector(context, curr_location) -> Connector:
    return context.deployment_manager.get_connector(curr_location.deployment)


@pytest.mark.asyncio
async def test_connector_run_command(
    context: StreamFlowContext, curr_connector: Connector, curr_location: Location
) -> None:
    """Test connector run method"""
    stdout, returncode = await curr_connector.run(
        location=curr_location,
        command=["ls"],
        capture_output=True,
        # job_name="job_test" # todo: fix SlurmConnector
    )
    assert returncode == 0


@pytest.mark.asyncio
async def test_deployment_manager_deploy_fails(context: StreamFlowContext) -> None:
    """Test DeploymentManager deploy method with multiple requests but they fail"""
    deployment_config = get_failure_deployment_config()
    deployment_config.lazy = False
    for result in await asyncio.gather(
        *(context.deployment_manager.deploy(deployment_config) for _ in range(3)),
        return_exceptions=True,
    ):
        assert isinstance(result, FailureConnectorException) or (
            isinstance(result, WorkflowExecutionException)
            and result.args[0] == "Deploying of failure-test failed"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method",
    [
        "copy_local_to_remote",
        "copy_remote_to_local",
        "copy_remote_to_remote",
        "get_available_locations",
        "get_stream_reader",
        "run",
    ],
)
async def test_future_connector_multiple_request_fail(
    context: StreamFlowContext, method: str
) -> None:
    """Test FutureConnector with multiple requests but the deployment fails"""
    deployment_name = "failure-test"
    deployment_config = get_failure_deployment_config()
    connector = FutureConnector(
        name=deployment_name,
        config_dir=os.path.dirname(context.config["path"]),
        connector_type=FailureConnector,
        external=deployment_config.external,
        **deployment_config.config,
    )

    for result in await asyncio.gather(
        *(_get_task(connector, method) for _ in range(3)),
        return_exceptions=True,
    ):
        assert isinstance(result, FailureConnectorException) or (
            isinstance(result, WorkflowExecutionException)
            and result.args[0] == "Deploying of failure-test failed"
        )


@pytest.mark.asyncio
async def test_ssh_connector_multiple_request_fail(context: StreamFlowContext) -> None:
    """Test SSHConnector with multiple requests but the deployment fails"""
    deployment_config = await get_ssh_deployment_config(context)
    # changed username to get an exception for the test
    deployment_config.config["nodes"][0]["username"] = "test"
    connector = SSHConnector(
        deployment_name=deployment_config.name,
        config_dir=os.path.dirname(context.config["path"]),
        **deployment_config.config,
    )

    for result in await asyncio.gather(
        *(connector.get_available_locations() for _ in range(3)),
        return_exceptions=True,
    ):
        assert isinstance(result, (ConnectionError, asyncssh.Error)) or (
            isinstance(result, WorkflowExecutionException)
            and result.args[0] == "Impossible to connect to 127.0.0.1:2222"
        )
