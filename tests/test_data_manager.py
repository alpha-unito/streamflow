import os
import posixpath
import tempfile
from pathlib import PurePath

import pytest
import pytest_asyncio

from streamflow.core import utils
from streamflow.core.data import DataType
from streamflow.core.deployment import Connector, ExecutionLocation
from streamflow.data import remotepath
from streamflow.deployment.utils import get_path_processor
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
    src_path = (
        os.path.join(tempfile.gettempdir(), utils.random_name(), utils.random_name())
        if src_location.local
        else posixpath.join("/tmp", utils.random_name(), utils.random_name())
    )
    dst_path = (
        os.path.join(tempfile.gettempdir(), utils.random_name())
        if dst_location.local
        else posixpath.join("/tmp", utils.random_name())
    )

    # Create working directories in src and dst locations
    await remotepath.mkdir(
        src_connector, src_location, str(PurePath(src_path).parent)
    )
    await remotepath.mkdir(dst_connector, dst_location, dst_path)

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
        path = get_path_processor(src_connector).sep
        for basename in PurePath(src_path).parts:
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
        path = get_path_processor(dst_connector).sep
        for basename in PurePath(dst_path).parts:
            path = os.path.join(path, basename)
            data_locs = context.data_manager.get_data_locations(
                path, dst_connector.deployment_name
            )
            assert len(data_locs) in [1, 2]
            if len(data_locs) == 1:
                assert data_locs[0].path == path
                assert data_locs[0].deployment == dst_connector.deployment_name
            elif len(data_locs) == 2:
                # src and dst are on the same location. So dst will be a symbolic link
                assert src_connector.deployment_name == dst_connector.deployment_name
                assert (
                    len(
                        [
                            loc
                            for loc in data_locs
                            if loc.data_type == DataType.PRIMARY
                            and loc.deployment == src_connector.deployment_name
                        ]
                    )
                    == 1
                )
                assert (
                    len(
                        [
                            loc
                            for loc in data_locs
                            if loc.data_type == DataType.SYMBOLIC_LINK
                            and loc.deployment == dst_connector.deployment_name
                            and loc.path == path
                        ]
                    )
                    == 1
                )
    finally:
        await remotepath.rm(src_connector, src_location, src_path)
        await remotepath.rm(dst_connector, dst_location, dst_path)


@pytest.mark.asyncio
async def test_invalidate_location(context, src_connector, src_location):
    """Test the invalidation of a location"""
    path_processor = get_path_processor(src_connector)
    # Remote location are linux-like environments, so they have Posix paths
    src_path = path_processor.join(
        path_processor.sep, "tmp", utils.random_name(), utils.random_name()
    )

    context.data_manager.register_path(
        location=src_location,
        path=src_path,
        relpath=src_path,
        data_type=DataType.PRIMARY,
    )

    # Check initial data location
    path = path_processor.sep
    for basename in PurePath(src_path).parts:
        path = path_processor.join(path, basename)
        data_locs = context.data_manager.get_data_locations(
            path, src_connector.deployment_name
        )
        assert len(data_locs) == 1
        assert data_locs[0].path == path
        assert data_locs[0].deployment == src_connector.deployment_name

    # Invalidate location
    root_data_loc = context.data_manager.get_data_locations(
        path_processor.sep, src_connector.deployment_name
    )[0]
    context.data_manager.invalidate_location(root_data_loc.location, root_data_loc.path)

    # Check data manager has invalidated the location
    path = path_processor.sep
    for basename in PurePath(src_path).parts:
        path = path_processor.join(path, basename)
        data_locs = context.data_manager.get_data_locations(
            path, src_connector.deployment_name
        )
        # The data location of the root is not invalidated
        if basename == path_processor.sep:
            assert len(data_locs) == 1
        else:
            assert len(data_locs) == 0
