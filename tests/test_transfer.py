import asyncio
import os
import posixpath
import tempfile

import pytest
import pytest_asyncio

from streamflow.core import utils
from streamflow.core.data import DataType
from streamflow.core.deployment import Connector, Location
from streamflow.data import remotepath
from streamflow.deployment.connector import LocalConnector
from streamflow.deployment.utils import get_path_processor
from tests.conftest import deployment_types, get_location


@pytest_asyncio.fixture(scope="module", params=deployment_types())
async def src_location(context, request) -> Location:
    return await get_location(context, request)


@pytest.fixture(scope="module")
def src_connector(context, src_location) -> Connector:
    return context.deployment_manager.get_connector(src_location.deployment)


@pytest_asyncio.fixture(scope="module", params=deployment_types())
async def dst_location(context, request) -> Location:
    return await get_location(context, request)


@pytest.fixture(scope="module")
def dst_connector(context, dst_location) -> Connector:
    return context.deployment_manager.get_connector(dst_location.deployment)


@pytest.mark.asyncio
async def test_directory_to_directory(
    context, src_connector, src_location, dst_connector, dst_location
):
    """Test transferring a directory and its content from one location to another."""
    if isinstance(src_connector, LocalConnector):
        src_path = os.path.join(tempfile.gettempdir(), utils.random_name())
    else:
        src_path = posixpath.join("/tmp", utils.random_name())
    if isinstance(dst_connector, LocalConnector):
        dst_path = os.path.join(tempfile.gettempdir(), utils.random_name())
    else:
        dst_path = posixpath.join("/tmp", utils.random_name())
    path_processor = get_path_processor(src_connector)
    inner_file = utils.random_name()
    try:
        await remotepath.mkdir(src_connector, [src_location], src_path)
        await remotepath.write(
            src_connector,
            src_location,
            path_processor.join(src_path, inner_file),
            "StreamFlow",
        )
        src_path = await remotepath.follow_symlink(
            context, src_connector, src_location, src_path
        )
        src_digest = await remotepath.checksum(
            context,
            src_connector,
            src_location,
            path_processor.join(src_path, inner_file),
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
            src_location=src_location,
            src_path=src_path,
            dst_locations=[dst_location],
            dst_path=dst_path,
            writable=False,
        )
        path_processor = get_path_processor(dst_connector)
        assert await remotepath.exists(dst_connector, dst_location, dst_path)
        assert await remotepath.exists(
            dst_connector, dst_location, path_processor.join(dst_path, inner_file)
        )
        dst_digest = await remotepath.checksum(
            context,
            dst_connector,
            dst_location,
            path_processor.join(dst_path, inner_file),
        )
        assert src_digest == dst_digest
    finally:
        await remotepath.rm(src_connector, src_location, src_path)
        await remotepath.rm(dst_connector, dst_location, dst_path)


@pytest.mark.asyncio
async def test_file_to_directory(
    context, src_connector, src_location, dst_connector, dst_location
):
    """Test transferring a file from one location to a directory into another location."""
    if isinstance(src_connector, LocalConnector):
        src_path = os.path.join(tempfile.gettempdir(), utils.random_name())
    else:
        src_path = posixpath.join("/tmp", utils.random_name())
    if isinstance(dst_connector, LocalConnector):
        dst_path = os.path.join(tempfile.gettempdir(), utils.random_name())
    else:
        dst_path = posixpath.join("/tmp", utils.random_name())
    await remotepath.mkdir(dst_connector, [dst_location], dst_path)
    try:
        await remotepath.write(src_connector, src_location, src_path, "StreamFlow")
        src_path = await remotepath.follow_symlink(
            context, src_connector, src_location, src_path
        )
        src_digest = await remotepath.checksum(
            context, src_connector, src_location, src_path
        )
        src_name = get_path_processor(src_connector).basename(src_path)
        context.data_manager.register_path(
            location=src_location,
            path=await remotepath.follow_symlink(
                context, src_connector, src_location, src_path
            ),
            relpath=src_path,
            data_type=DataType.PRIMARY,
        )
        await context.data_manager.transfer_data(
            src_location=src_location,
            src_path=src_path,
            dst_locations=[dst_location],
            dst_path=dst_path,
            writable=False,
        )
        path_processor = get_path_processor(dst_connector)
        assert await remotepath.exists(
            dst_connector, dst_location, path_processor.join(dst_path, src_name)
        )
        dst_digest = await remotepath.checksum(
            context,
            dst_connector,
            dst_location,
            path_processor.join(dst_path, src_name),
        )
        assert src_digest == dst_digest
    finally:
        await remotepath.rm(src_connector, src_location, src_path)
        await remotepath.rm(dst_connector, dst_location, dst_path)


@pytest.mark.asyncio
async def test_file_to_file(
    context, src_connector, src_location, dst_connector, dst_location
):
    """Test transferring a file from one location to another."""
    if isinstance(src_connector, LocalConnector):
        src_path = os.path.join(tempfile.gettempdir(), utils.random_name())
    else:
        src_path = posixpath.join("/tmp", utils.random_name())
    if isinstance(dst_connector, LocalConnector):
        dst_path = os.path.join(tempfile.gettempdir(), utils.random_name())
    else:
        dst_path = posixpath.join("/tmp", utils.random_name())
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
            src_location=src_location,
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


@pytest.mark.asyncio
async def test_multiple_files(
    context, src_connector, src_location, dst_connector, dst_location
):
    """Test transferring multiple files simultaneously from one location to another."""
    await asyncio.gather(
        *(
            asyncio.create_task(
                test_file_to_file(
                    context, src_connector, src_location, dst_connector, dst_location
                )
            )
            for _ in range(20)
        )
    )
