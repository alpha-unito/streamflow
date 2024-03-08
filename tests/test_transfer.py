import asyncio
import os
import posixpath
import tempfile

import pytest
import pytest_asyncio

from streamflow.core import utils
from streamflow.core.data import DataType, FileType
from streamflow.core.deployment import Connector, ExecutionLocation
from streamflow.data import remotepath
from streamflow.deployment.connector import LocalConnector
from streamflow.deployment.utils import get_path_processor
from tests.utils.deployment import get_location


async def _compare_remote_dirs(
    context,
    src_connector,
    src_location,
    src_path,
    dst_connector,
    dst_location,
    dst_path,
):
    assert await remotepath.exists(dst_connector, dst_location, dst_path)
    src_path_processor = get_path_processor(src_connector)
    dst_path_processor = get_path_processor(dst_connector)

    # the two dirs must have the same elements order
    src_files, dst_files = await asyncio.gather(
        asyncio.create_task(
            remotepath.listdir(src_connector, src_location, src_path, FileType.FILE)
        ),
        asyncio.create_task(
            remotepath.listdir(dst_connector, dst_location, dst_path, FileType.FILE)
        ),
    )
    assert len(src_files) == len(dst_files)
    for src_file, dst_file in zip(sorted(src_files), sorted(dst_files)):
        checksums = await asyncio.gather(
            asyncio.create_task(
                remotepath.checksum(
                    context,
                    src_connector,
                    src_location,
                    src_path_processor.join(src_path, src_file),
                )
            ),
            asyncio.create_task(
                remotepath.checksum(
                    context,
                    dst_connector,
                    dst_location,
                    dst_path_processor.join(dst_path, dst_file),
                )
            ),
        )
        assert checksums[0] == checksums[1]

    src_dirs, dst_dirs = await asyncio.gather(
        asyncio.create_task(
            remotepath.listdir(
                src_connector, src_location, src_path, FileType.DIRECTORY
            )
        ),
        asyncio.create_task(
            remotepath.listdir(
                dst_connector, dst_location, dst_path, FileType.DIRECTORY
            )
        ),
    )
    assert len(src_dirs) == len(dst_dirs)
    tasks = []
    for src_dir, dst_dir in zip(sorted(src_dirs), sorted(dst_dirs)):
        assert os.path.basename(src_dir) == os.path.basename(dst_dir)
        tasks.append(
            asyncio.create_task(
                _compare_remote_dirs(
                    context,
                    src_connector,
                    src_location,
                    src_dir,
                    dst_connector,
                    dst_location,
                    dst_dir,
                )
            )
        )
    await asyncio.gather(*tasks)


async def _create_tmp_dir(context, connector, location, root=None, lvl=None, n_files=0):
    path_processor = get_path_processor(connector)
    dir_lvl = f"-{lvl}" if lvl else ""
    if isinstance(src_connector, LocalConnector):
        dir_path = os.path.join(
            root if root else tempfile.gettempdir(),
            f"dir{dir_lvl}-{utils.random_name()}",
        )
    else:
        dir_path = os.path.join(
            root if root else "/tmp", f"dir{dir_lvl}-{utils.random_name()}"
        )
    await remotepath.mkdir(connector, [location], dir_path)

    dir_path = await remotepath.follow_symlink(context, connector, location, dir_path)
    file_lvl = f"-{lvl}" if lvl else ""
    for i in range(n_files):
        file_name = f"file{file_lvl}-{i}-{utils.random_name()}"
        await remotepath.write(
            connector,
            location,
            path_processor.join(dir_path, file_name),
            f"Hello from {file_name}",
        )
    return dir_path


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
async def test_directory_to_directory(
    context, src_connector, src_location, dst_connector, dst_location
):
    """Test transferring a directory and its content from one location to another."""
    src_path = None
    dst_path = None
    # dir
    #   |- file_0
    #   |- file_1
    #   |- file_2
    #   |- file_3
    #   |- dir_0
    #   |   |- file_0_0
    #   |   |- file_0_1
    #   |   |- dir_0_0
    #   |   |   |- file_0_0_1
    #   |   |   |- file_0_0_2
    #   |- dir_1
    #   |   |- file_1_0
    #   |   |- file_1_1
    #   |   |- file_1_2
    #   |- dir_2
    #   |   |   empty
    try:
        # create src structure
        src_path = await _create_tmp_dir(
            context, src_connector, src_location, n_files=4
        )
        for i in range(3):
            inner_dir = await _create_tmp_dir(
                context,
                src_connector,
                src_location,
                root=src_path,
                n_files=2 + i if i < 2 else 0,
                lvl=f"{i}",
            )
            if i == 0:
                await _create_tmp_dir(
                    context,
                    src_connector,
                    src_location,
                    root=inner_dir,
                    n_files=2,
                    lvl=f"{i}-0",
                )
        src_path = await remotepath.follow_symlink(
            context, src_connector, src_location, src_path
        )

        # dst init
        if isinstance(dst_connector, LocalConnector):
            dst_path = os.path.join(tempfile.gettempdir(), utils.random_name())
        else:
            dst_path = posixpath.join("/tmp", utils.random_name())

        # save src_path into StreamFlow
        context.data_manager.register_path(
            location=src_location,
            path=src_path,
            relpath=src_path,
            data_type=DataType.PRIMARY,
        )

        # transfer src_path to dst_path
        await context.data_manager.transfer_data(
            src_location=src_location,
            src_path=src_path,
            dst_locations=[dst_location],
            dst_path=dst_path,
            writable=False,
        )

        # check if dst exists
        await remotepath.exists(dst_connector, dst_location, dst_path)

        # check that src and dst have the same sub dirs and files
        await _compare_remote_dirs(
            context,
            src_connector,
            src_location,
            src_path,
            dst_connector,
            dst_location,
            dst_path,
        )
    finally:
        if src_path:
            await remotepath.rm(src_connector, src_location, src_path)
        if dst_path:
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
