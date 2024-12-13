from __future__ import annotations

import tempfile

import pytest
import pytest_asyncio

from streamflow.core import utils
from streamflow.core.data import DataLocation, DataType
from streamflow.core.deployment import Connector, ExecutionLocation
from streamflow.data.remotepath import StreamFlowPath
from tests.utils.deployment import get_location


def _contains_location(
    searched_location: ExecutionLocation | DataLocation,
    execution_location: ExecutionLocation,
):
    """
    The `execution_location` object can wrap other `execution_location` object.
    This function checks whether the `execution_location` object, or one of its
    wrapped locations, contains the `searched_location`.

    :param searched_location: the location to be found, it can be a `ExecutionLocation` or a `DataLocation`,
                                identified by the name attribute
    :param execution_location: the `ExecutionLocation` object with the location information
    :return: a boolean which is true if the `execution_location` contains the `searched_location`
    """
    while execution_location is not None:
        if searched_location.name == execution_location.name:
            return True
        else:
            execution_location = execution_location.wraps
    return False


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
    src_path = StreamFlowPath(
        tempfile.gettempdir() if src_location.local else "/tmp",
        utils.random_name(),
        context=context,
        location=src_location,
    )
    dst_path = StreamFlowPath(
        tempfile.gettempdir() if src_location.local else "/tmp",
        utils.random_name(),
        context=context,
        location=dst_location,
    )

    # Create working directories in src and dst locations
    await src_path.parent.mkdir(mode=0o777, exist_ok=True)
    await dst_path.mkdir(mode=0o777, parents=True)

    try:
        await src_path.write_text("StreamFlow")
        src_path = await src_path.resolve()
        context.data_manager.register_path(
            location=src_location,
            path=str(src_path),
            relpath=str(src_path),
            data_type=DataType.PRIMARY,
        )

        # Check src data locations
        path = StreamFlowPath(context=context, location=src_location)
        for part in src_path.parts:
            path /= part
            data_locs = context.data_manager.get_data_locations(
                str(path), src_connector.deployment_name
            )
            assert len(data_locs) == 1
            assert data_locs[0].path == str(path)
            assert data_locs[0].deployment == src_connector.deployment_name

        # Transfer data from src to dst
        await context.data_manager.transfer_data(
            src_location=src_location,
            src_path=str(src_path),
            dst_locations=[dst_location],
            dst_path=str(dst_path),
            writable=False,
        )

        # Check dst data locations
        path = StreamFlowPath(context=context, location=src_location)
        for part in dst_path.parts:
            path /= part
            data_locs = context.data_manager.get_data_locations(
                str(path), dst_connector.deployment_name
            )
            assert len(data_locs) in [1, 2]
            if len(data_locs) == 1:
                assert data_locs[0].path == str(path)
                assert data_locs[0].deployment == dst_connector.deployment_name
            elif len(data_locs) == 2:
                # src and dst are on the same location. So dst will be a symbolic link
                assert _contains_location(dst_location, src_location)
                assert (
                    len(
                        [
                            loc
                            for loc in data_locs
                            if loc.data_type == DataType.PRIMARY
                            and _contains_location(loc, src_location)
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
                            and _contains_location(loc, dst_location)
                            and loc.path == str(path)
                        ]
                    )
                    == 1
                )
    finally:
        await src_path.rmtree()
        await dst_path.rmtree()


@pytest.mark.asyncio
async def test_invalidate_location(context, src_connector, src_location):
    """Test the invalidation of a location"""
    src_path = StreamFlowPath(
        tempfile.gettempdir() if src_location.local else "/tmp",
        utils.random_name(),
        utils.random_name(),
        context=context,
        location=src_location,
    )
    context.data_manager.register_path(
        location=src_location,
        path=str(src_path),
        relpath=str(src_path),
        data_type=DataType.PRIMARY,
    )

    # Check initial data location
    path = StreamFlowPath(context=context, location=src_location)
    for part in src_path.parts:
        path /= part
        data_locs = context.data_manager.get_data_locations(
            str(path), src_connector.deployment_name
        )
        assert len(data_locs) == 1
        assert data_locs[0].path == str(path)
        assert data_locs[0].deployment == src_connector.deployment_name

    # Invalidate location
    root_data_loc = next(
        iter(
            context.data_manager.get_data_locations(
                path.root, src_connector.deployment_name
            )
        )
    )
    context.data_manager.invalidate_location(root_data_loc.location, root_data_loc.path)

    # Check data manager has invalidated the location
    path = StreamFlowPath(context=context, location=src_location)
    for part in src_path.parts:
        path /= part
        data_locs = context.data_manager.get_data_locations(
            str(path), src_connector.deployment_name
        )
        # The data location of the root is not invalidated
        assert len(data_locs) == (1 if path == path.parent else 0)
