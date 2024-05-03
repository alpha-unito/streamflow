import os
import posixpath
import tempfile
from pathlib import Path

import pytest
import pytest_asyncio

from streamflow.core import utils
from streamflow.core.data import DataType
from streamflow.core.deployment import Connector, ExecutionLocation
from streamflow.data import remotepath
from streamflow.deployment.connector import LocalConnector
from tests.utils.deployment import get_location


@pytest_asyncio.fixture(scope="module")
async def src_location(context, deployment_src) -> ExecutionLocation:
    return await get_location(context, deployment_src)


@pytest.fixture(scope="module")
def src_connector(context, src_location) -> Connector:
    return context.deployment_manager.get_connector(src_location.deployment)


@pytest_asyncio.fixture(scope="module")
async def dst_location(context, deployment_dst) -> ExecutionLocation:
    return await get_location(context, deployment_dst)


@pytest.fixture(scope="module")
def dst_connector(context, dst_location) -> Connector:
    return context.deployment_manager.get_connector(dst_location.deployment)


@pytest.mark.asyncio
async def test_data_locations(
    context, src_connector, src_location, dst_connector, dst_location
):
    """Test the existence of data locations after the transfer data"""
    if isinstance(src_connector, LocalConnector):
        src_path = os.path.join(
            tempfile.gettempdir(), utils.random_name(), utils.random_name()
        )
    else:
        src_path = posixpath.join("/tmp", utils.random_name(), utils.random_name())
    if isinstance(dst_connector, LocalConnector):
        dst_path = os.path.join(tempfile.gettempdir(), utils.random_name())
    else:
        dst_path = posixpath.join("/tmp", utils.random_name())

    # Create working directories in src and dst locations
    await remotepath.mkdir(src_connector, [src_location], str(Path(src_path).parent))
    await remotepath.mkdir(dst_connector, [dst_location], dst_path)

    try:
        await remotepath.write(
            src_connector,
            src_location,
            src_path,
            "StreamFlow",
        )
        src_path = await remotepath.follow_symlink(
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

        # Check src data locations
        path = "/"
        for basename in src_path.split("/")[1:]:
            path = os.path.join(path, basename)
            data_locs = context.data_manager.get_data_locations(
                path, src_connector.deployment_name
            )
            assert len(data_locs) == 1
            assert data_locs[0].path == path
            assert data_locs[0].deployment == src_connector.deployment_name

        # Transfer data from src to dst
        await context.data_manager.transfer_data(
            src_location=src_location,
            src_path=src_path,
            dst_locations=[dst_location],
            dst_path=dst_path,
            writable=False,
        )

        # Check dst data locations
        path = "/"
        for basename in dst_path.split("/")[1:]:
            path = os.path.join(path, basename)
            data_locs = context.data_manager.get_data_locations(
                path, dst_connector.deployment_name
            )
            assert len(data_locs) > 0 and len(data_locs) < 3
            if len(data_locs) == 1:
                assert data_locs[0].path == path
                assert data_locs[0].deployment == dst_connector.deployment_name
            elif len(data_locs) == 2:
                # src and dst are on the same location. So dst will be a symbolic link
                assert src_connector.deployment_name == dst_connector.deployment_name
                assert data_locs[0].data_type == DataType.PRIMARY
                assert data_locs[0].deployment == src_connector.deployment_name
                assert data_locs[1].data_type == DataType.SYMBOLIC_LINK
                assert data_locs[1].deployment == dst_connector.deployment_name
                assert data_locs[1].path == path
    finally:
        await remotepath.rm(src_connector, src_location, src_path)
        await remotepath.rm(dst_connector, dst_location, dst_path)


@pytest.mark.asyncio
async def test_invalidate_location(context, src_connector, src_location):
    """Test the invalidation of a location"""
    src_path = posixpath.join("/tmp", utils.random_name(), utils.random_name())

    context.data_manager.register_path(
        location=src_location,
        path=src_path,
        relpath=src_path,
        data_type=DataType.PRIMARY,
    )

    # Check initial data location
    path = "/"
    for basename in src_path.split("/")[1:]:
        path = os.path.join(path, basename)
        data_locs = context.data_manager.get_data_locations(
            path, src_connector.deployment_name
        )
        assert len(data_locs) == 1
        assert data_locs[0].path == path
        assert data_locs[0].deployment == src_connector.deployment_name

    # Invalidate location
    root_data_loc = context.data_manager.get_data_locations(
        "/", src_connector.deployment_name
    )[0]
    context.data_manager.invalidate_location(root_data_loc, root_data_loc.path)

    # Check data manager has invalidated the location
    path = "/"
    for basename in src_path.split("/")[1:]:
        path = os.path.join(path, basename)
        data_locs = context.data_manager.get_data_locations(
            path, src_connector.deployment_name
        )
        assert len(data_locs) == 0
