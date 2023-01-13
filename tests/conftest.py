import asyncio
import tempfile

import pytest
import pytest_asyncio

from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import DeploymentConfig, LOCAL_LOCATION, Location
from streamflow.main import build_context


async def get_location(
    context: StreamFlowContext, request: pytest.FixtureRequest
) -> Location:
    if request.param == "local":
        return Location(deployment=LOCAL_LOCATION, name=LOCAL_LOCATION)
    elif request.param == "docker":
        connector = context.deployment_manager.get_connector("alpine")
        locations = await connector.get_available_locations()
        return Location(deployment="alpine", name=next(iter(locations.keys())))
    else:
        raise Exception(f"{request.param} location type not supported")


def get_docker_deploy_config():
    return DeploymentConfig(
        name="alpine",
        type="docker",
        config={"image": "alpine:3.16.2"},
        external=False,
        lazy=False,
    )


@pytest_asyncio.fixture(scope="session")
async def context() -> StreamFlowContext:
    context = build_context(tempfile.gettempdir(), {})
    await context.deployment_manager.deploy(
        DeploymentConfig(
            name=LOCAL_LOCATION,
            type="local",
            config={},
            external=True,
            lazy=False,
            workdir=tempfile.gettempdir(),
        )
    )
    await context.deployment_manager.deploy(get_docker_deploy_config())
    yield context
    await context.deployment_manager.undeploy_all()


@pytest.fixture(scope="session")
def event_loop():
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()
