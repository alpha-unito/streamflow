import pytest
import pytest_asyncio

from streamflow.core import utils
from streamflow.core.data import DataType
from streamflow.core.deployment import Connector, Location
from streamflow.data import remotepath
from tests.conftest import get_location


@pytest_asyncio.fixture(scope="module", params=["local", "docker"])
async def src_location(context, request) -> Location:
    return await get_location(context, request)


@pytest.fixture(scope="module")
def src_connector(context, src_location) -> Connector:
    return context.deployment_manager.get_connector(src_location.deployment)


@pytest_asyncio.fixture(scope="module", params=["local", "docker"])
async def dst_location(context, request) -> Location:
    return await get_location(context, request)


@pytest.fixture(scope="module")
def dst_connector(context, dst_location) -> Connector:
    return context.deployment_manager.get_connector(dst_location.deployment)


@pytest.mark.asyncio
async def test_file(context, src_connector, src_location, dst_connector, dst_location):
    """Test transferring a file from one location to another."""
    src_path = utils.random_name()
    dst_path = utils.random_name()
    try:
        await remotepath.write(src_connector, src_location, src_path, "StreamFlow")
        src_path = await remotepath.follow_symlink(
            context, src_connector, src_location, src_path
        )
        src_digest = await remotepath.checksum(
            context, src_connector, src_location, src_path
        )
        context.data_manager.register_path(
            location=src_location,
            path=await remotepath.follow_symlink(
                context, src_connector, src_location, src_path
            ),
            relpath=src_path,
            data_type=DataType.PRIMARY,
        )
        await context.data_manager.transfer_data(
            src_locations=[src_location],
            src_path=src_path,
            dst_locations=[dst_location],
            dst_path=dst_path,
            writable=False,
        )
        assert await remotepath.exists(dst_connector, dst_location, dst_path)
        dst_digest = await remotepath.checksum(
            context, dst_connector, dst_location, dst_path
        )
        assert src_digest == dst_digest
    finally:
        await remotepath.rm(src_connector, src_location, src_path)
        await remotepath.rm(dst_connector, dst_location, dst_path)
