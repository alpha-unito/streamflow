import asyncio
import tempfile

import pytest

from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import DeploymentConfig, LOCAL_LOCATION, Location
from streamflow.main import build_context


async def get_location(context: StreamFlowContext,
                       request: pytest.FixtureRequest) -> Location:
    if request.param == "local":
        return Location(deployment=LOCAL_LOCATION, name=LOCAL_LOCATION)
    elif request.param == "docker":
        connector = context.deployment_manager.get_connector("alpine")
        locations = await connector.get_available_locations()
        return Location(deployment="alpine", name=next(iter(locations.keys())))
    else:
        raise Exception("{} location type not supported".format(request.param))


@pytest.fixture(scope="session")
def context() -> StreamFlowContext:
    context = build_context(tempfile.gettempdir(), {})
    asyncio.run(context.deployment_manager.deploy(DeploymentConfig(
        name=LOCAL_LOCATION,
        type="local",
        config={},
        external=True,
        lazy=False,
        workdir=tempfile.gettempdir())))
    asyncio.run(context.deployment_manager.deploy(DeploymentConfig(
        name="alpine",
        type="docker",
        config={
            "image": "alpine:3.16.2"
        },
        external=False,
        lazy=False)))
    yield context
    asyncio.run(context.deployment_manager.undeploy_all())
