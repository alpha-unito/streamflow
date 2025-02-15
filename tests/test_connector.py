from __future__ import annotations

import asyncio
import logging
import os
import re
from collections.abc import Callable, MutableSequence
from typing import Any

import pytest
import pytest_asyncio

from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import Connector, ExecutionLocation
from streamflow.core.exception import WorkflowExecutionException
from streamflow.deployment.connector import SSHConnector
from streamflow.deployment.future import FutureConnector
from streamflow.log_handler import logger
from tests.conftest import get_class_callables
from tests.utils.connector import (
    FailureConnector,
    FailureConnectorException,
    SSHChannelErrorConnector,
)
from tests.utils.deployment import (
    get_failure_deployment_config,
    get_location,
    get_ssh_deployment_config,
)


def _get_future_connector_methods() -> MutableSequence[Callable]:
    methods = get_class_callables(FutureConnector)
    unnecessary_methods = ("_safe_deploy_event_wait", "get_schema", "undeploy")
    return [method for method in methods if method.__name__ not in unnecessary_methods]


def _get_connector_method_params(method_name: str) -> MutableSequence[Any]:
    loc = ExecutionLocation("test-location", "failure-test")
    if method_name in ("copy_remote_to_local", "copy_local_to_remote"):
        return ["test_src", "test_dst", [loc]]
    elif method_name in ("deploy", "undeploy"):
        return [False]
    elif method_name == "copy_remote_to_remote":
        return ["test_src", "test_dst", [loc], loc]
    elif method_name == "get_available_locations":
        return []
    elif method_name in ("get_stream_reader", "get_stream_writer"):
        return [["test_command"], loc]
    elif method_name == "run":
        return [loc, ["ls"]]
    else:
        raise pytest.fail(f"Unknown method_name: {method_name}")


@pytest_asyncio.fixture(scope="module")
async def curr_location(context, deployment_src) -> ExecutionLocation:
    return await get_location(context, deployment_src)


@pytest.fixture(scope="module")
def curr_connector(context, curr_location) -> Connector:
    return context.deployment_manager.get_connector(curr_location.deployment)


@pytest.mark.asyncio
async def test_connector_run_command(
    context: StreamFlowContext,
    curr_connector: Connector,
    curr_location: ExecutionLocation,
) -> None:
    """Test connector run method"""
    _, returncode = await curr_connector.run(
        location=curr_location, command=["ls"], capture_output=True, job_name="job_test"
    )
    assert returncode == 0


@pytest.mark.asyncio
async def test_connector_run_command_fails(
    curr_connector: Connector, curr_location: ExecutionLocation
):
    """Test connector run method on a job with an invalid command"""
    _, returncode = await curr_connector.run(
        curr_location, ["ls -2"], capture_output=True, job_name="job_test"
    )
    assert returncode != 0


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
            and result.args[0] == f"FAILED deployment of {deployment_config.name}"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method",
    _get_future_connector_methods(),
)
async def test_future_connector_multiple_request_fail(
    context: StreamFlowContext, method: Callable
) -> None:
    """Test FutureConnector with multiple requests but the deployment fails"""
    deployment_config = get_failure_deployment_config()
    connector = FutureConnector(
        name=deployment_config.name,
        config_dir=os.path.dirname(context.config["path"]),
        connector_type=FailureConnector,
        external=deployment_config.external,
        **deployment_config.config,
    )

    for result in await asyncio.gather(
        *(
            asyncio.create_task(
                method(connector, *_get_connector_method_params(method.__name__))
            )
            for _ in range(3)
        ),
        return_exceptions=True,
    ):
        assert isinstance(result, FailureConnectorException) or (
            isinstance(result, WorkflowExecutionException)
            and result.args[0] == f"FAILED deployment of {deployment_config.name}"
        )


@pytest.mark.asyncio
async def test_ssh_connector_channel_open_error(
    caplog, context: StreamFlowContext
) -> None:
    """
    Test SSHConnector on a channel open error which close the ssh connection.
    The SSHConnector retry mechanism will retry on a new ssh connection
    """
    caplog.set_level(logging.WARNING)
    caplog_handler = caplog.handler
    logger.addHandler(caplog_handler)
    try:
        deployment_config = await get_ssh_deployment_config(context)
        connector = SSHChannelErrorConnector(
            deployment_name=deployment_config.name,
            config_dir=os.path.dirname(context.config["path"]),
            **deployment_config.config,
        )
        await connector.get_available_locations()
        assert "Error ChannelOpenError opening SSH session to" in caplog.text
    finally:
        logger.removeHandler(caplog_handler)


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
        assert (
            re.match(
                r"Hosts \[.*] have no more available contexts: terminating.",
                result.args[0],
            )
            is not None
        )
