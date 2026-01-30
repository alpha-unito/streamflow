from __future__ import annotations

import asyncio
import logging
import os
import re
from collections.abc import Callable, MutableSequence
from typing import Any

import pytest
import pytest_asyncio
from pytest import LogCaptureFixture

from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import Connector, ExecutionLocation
from streamflow.core.exception import (
    WorkflowDefinitionException,
    WorkflowExecutionException,
)
from streamflow.deployment.connector import SSHConnector
from streamflow.deployment.future import FutureConnector
from streamflow.deployment.template import CommandTemplateMap
from tests.conftest import get_class_callables
from tests.utils.connector import (
    FailureConnector,
    FailureConnectorException,
    SSHChannelErrorConnector,
)
from tests.utils.deployment import (
    get_deployment,
    get_failure_deployment_config,
    get_location,
    get_ssh_deployment_config,
)
from tests.utils.utils import InjectPlugin, caplog_streamflow


def _get_future_connector_methods() -> MutableSequence[Callable]:
    methods = get_class_callables(FutureConnector)
    unnecessary_methods = ("_safe_deploy_event_wait", "get_schema", "undeploy")
    return [method for method in methods if method.__name__ not in unnecessary_methods]


def _get_connector_method_params(method_name: str) -> MutableSequence[Any]:
    loc = ExecutionLocation("test-location", "failure-test")
    match method_name:
        case "copy_remote_to_local" | "copy_local_to_remote":
            return ["test_src", "test_dst", [loc]]
        case "deploy" | "undeploy":
            return [False]
        case "copy_remote_to_remote":
            return ["test_src", "test_dst", [loc], loc]
        case "get_available_locations":
            return []
        case "get_stream_reader" | "get_stream_writer":
            return [["test_command"], loc]
        case "run":
            return [loc, ["ls"]]
        case _:
            raise pytest.fail(f"Unknown method_name: {method_name}")


@pytest_asyncio.fixture(scope="session")
async def curr_location(context, deployment_src) -> ExecutionLocation:
    return await get_location(context, deployment_src)


@pytest.fixture(scope="session")
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
) -> None:
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
    with InjectPlugin("failure-connector"):
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
    caplog: LogCaptureFixture,
    chosen_deployment_types: MutableSequence[str],
    context: StreamFlowContext,
) -> None:
    """
    Test SSHConnector on a channel open error which close the ssh connection.
    The SSHConnector retry mechanism will retry on a new ssh connection
    """
    if "ssh" not in chosen_deployment_types:
        pytest.skip("Deployment ssh was not activated")

    with caplog_streamflow(caplog=caplog, level=logging.WARNING):
        deployment_config = await get_ssh_deployment_config(context)
        connector = SSHChannelErrorConnector(
            deployment_name=deployment_config.name,
            config_dir=os.path.dirname(context.config["path"]),
            **deployment_config.config,
        )
        await connector.get_available_locations()
        assert "Error ChannelOpenError opening SSH session to" in caplog.text


@pytest.mark.asyncio
async def test_ssh_connector_multiple_request_fail(
    chosen_deployment_types: MutableSequence[str], context: StreamFlowContext
) -> None:
    """Test SSHConnector with multiple requests but the deployment fails"""
    if "ssh" not in chosen_deployment_types:
        pytest.skip("Deployment ssh was not activated")
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


@pytest.mark.asyncio
@pytest.mark.parametrize("deployment_t", ["ssh", "slurm"])
async def test_templates(
    chosen_deployment_types: MutableSequence[str],
    context: StreamFlowContext,
    deployment_t: str,
) -> None:
    """Test command templates"""
    if deployment_t not in chosen_deployment_types:
        pytest.skip(f"Deployment {deployment_t} was not activated")
    deployment = get_deployment(deployment_t)
    service = "test_template"
    connector = context.deployment_manager.get_connector(deployment)
    locations = await connector.get_available_locations(service=service)
    location = next(iter(locations.values())).location
    conn = context.deployment_manager.get_connector(location.deployment)
    stdout, retcode = await conn.run(
        location, ["true"], capture_output=True, job_name="job_test"
    )
    assert retcode == 0
    assert stdout == f"I am a {deployment_t} service"


def test_command_template():
    """Test command template validation"""
    t0 = CommandTemplateMap(default="#!/bin/sh\n\n{{streamflow_command}}")
    assert t0.is_empty()
    template_map = {
        "service_a": "module load python\n{{streamflow_command}}",
        "service_b": "spack load python\ncd {{ streamflow_workdir }}\n{{ streamflow_environment }}\n{{streamflow_command}}",
        "service_c": "spack load python\n{{ custom_key }}\n{{streamflow_command}}",
    }
    t1 = CommandTemplateMap(
        default="#!/bin/sh\n\n{{streamflow_command}}", template_map=template_map
    )
    assert not t1.is_empty()
    # Test command
    assert (
        t1.get_command(
            "cat /path/to/file", environment={"APP_NAME": "app1"}, workdir="/tmp/abc"
        )
        == "#!/bin/sh\n\ncat /path/to/file"
    )
    # Test workdir, environment, command
    assert (
        t1.get_command(
            "cat /path/to/file",
            template="service_b",
            environment={"APP_NAME": "app1"},
            workdir="/tmp/abc",
        )
        == 'spack load python\ncd /tmp/abc\nexport APP_NAME="app1"\ncat /path/to/file'
    )
    # Test custom keys
    assert (
        t1.get_command(
            "cat /path/to/file",
            template="service_c",
            environment={"APP_NAME": "app1"},
            workdir="/tmp/abc",
            custom_key="hostname",
        )
        == "spack load python\nhostname\ncat /path/to/file"
    )

    # Test validation of default and service templates
    with pytest.raises(WorkflowDefinitionException):
        CommandTemplateMap(default="", template_map=template_map)
    with pytest.raises(WorkflowDefinitionException):
        CommandTemplateMap(
            default="#!/bin/sh\n\n{{streamflow_command}}",
            template_map={
                "service_a": "module load python\n{{streamflow_command}}",
                "service_b": "module load python",
            },
        )
