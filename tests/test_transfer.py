import asyncio
import os
import tempfile

import pytest

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataType
from streamflow.core.deployment import Connector, ExecutionLocation
from streamflow.data.remotepath import StreamFlowPath
from tests.utils.deployment import get_location


async def _compare_remote_dirs(
    context: StreamFlowContext,
    src_connector: Connector,
    src_location: ExecutionLocation,
    src_path: StreamFlowPath,
    dst_connector: Connector,
    dst_location: ExecutionLocation,
    dst_path: StreamFlowPath,
):
    assert await dst_path.exists()

    # the two dirs must have the same elements order
    src_path, src_dirs, src_files = await src_path.walk(
        follow_symlinks=True
    ).__anext__()
    dst_path, dst_dirs, dst_files = await dst_path.walk(
        follow_symlinks=True
    ).__anext__()
    assert len(src_files) == len(dst_files)
    for src_file, dst_file in zip(sorted(src_files), sorted(dst_files)):
        assert (
            await (src_path / src_file).checksum()
            == await (dst_path / dst_file).checksum()
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
                    src_path / src_dir,
                    dst_connector,
                    dst_location,
                    dst_path / dst_dir,
                )
            )
        )
    await asyncio.gather(*tasks)


async def _create_tmp_dir(
    context, connector, location, root=None, lvl=None, n_files=0
) -> StreamFlowPath:
    dir_lvl = f"-{lvl}" if lvl else ""
    dir_path = StreamFlowPath(
        (
            root
            if root is not None
            else tempfile.gettempdir() if location.local else "/tmp"
        ),
        f"dir{dir_lvl}-{utils.random_name()}",
        context=context,
        location=location,
    )
    await dir_path.mkdir(mode=0o777, parents=True)

    dir_path = await dir_path.resolve()
    file_lvl = f"-{lvl}" if lvl else ""
    for i in range(n_files):
        file_name = f"file{file_lvl}-{i}-{utils.random_name()}"
        await (dir_path / file_name).write_text(f"Hello from {file_name}")
    return dir_path


@pytest.mark.asyncio
async def test_directory_to_directory(
    context: StreamFlowContext, communication_pattern: tuple[str, str]
) -> None:
    """Test transferring a directory and its content from one location to another."""
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

    src_location = await get_location(context, communication_pattern[0])
    src_connector = context.deployment_manager.get_connector(src_location.deployment)
    src_path = None

    dst_location = await get_location(context, communication_pattern[1])
    dst_connector = context.deployment_manager.get_connector(dst_location.deployment)
    dst_path = None

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
        src_path = await src_path.resolve()

        # dst init
        dst_path = StreamFlowPath(
            tempfile.gettempdir() if dst_location.local else "/tmp",
            utils.random_name(),
            context=context,
            location=dst_location,
        )

        # save src_path into StreamFlow
        context.data_manager.register_path(
            location=src_location,
            path=str(src_path),
            relpath=str(src_path),
            data_type=DataType.PRIMARY,
        )

        # transfer src_path to dst_path
        await context.data_manager.transfer_data(
            src_location=src_location,
            src_path=str(src_path),
            dst_locations=[dst_location],
            dst_path=str(dst_path),
            writable=False,
        )

        # check if dst exists
        await dst_path.exists()

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
        await src_path.rmtree()
        await dst_path.rmtree()


@pytest.mark.asyncio
async def test_file_to_directory(
    context: StreamFlowContext, communication_pattern: tuple[str, str]
) -> None:
    """Test transferring a file from one location to a directory into another location."""
    src_location = await get_location(context, communication_pattern[0])
    dst_location = await get_location(context, communication_pattern[1])

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
    await dst_path.mkdir(mode=0o777, exist_ok=True)
    try:
        await src_path.write_text("StreamFlow")
        src_path = await src_path.resolve()
        context.data_manager.register_path(
            location=src_location,
            path=str(src_path),
            relpath=str(src_path),
            data_type=DataType.PRIMARY,
        )
        await context.data_manager.transfer_data(
            src_location=src_location,
            src_path=str(src_path),
            dst_locations=[dst_location],
            dst_path=str(dst_path),
            writable=False,
        )
        dst_file = dst_path / src_path.name
        assert await dst_file.exists()
        assert await src_path.checksum() == await dst_file.checksum()
    finally:
        await src_path.rmtree()
        await dst_path.rmtree()


@pytest.mark.asyncio
async def test_file_to_file(
    context: StreamFlowContext, communication_pattern: tuple[str, str]
):
    """Test transferring a file from one location to another."""
    src_location = await get_location(context, communication_pattern[0])
    dst_location = await get_location(context, communication_pattern[1])

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
    try:
        await src_path.write_text("StreamFlow")
        src_path = await src_path.resolve()
        context.data_manager.register_path(
            location=src_location,
            path=str(src_path),
            relpath=str(src_path),
            data_type=DataType.PRIMARY,
        )
        await context.data_manager.transfer_data(
            src_location=src_location,
            src_path=str(src_path),
            dst_locations=[dst_location],
            dst_path=str(dst_path),
            writable=False,
        )
        assert await dst_path.exists()
        assert await src_path.checksum() == await dst_path.checksum()
    finally:
        await src_path.rmtree()
        await dst_path.rmtree()


@pytest.mark.asyncio
async def test_multiple_files(
    context: StreamFlowContext, communication_pattern: tuple[str, str]
):
    """Test transferring multiple files simultaneously from one location to another."""
    await asyncio.gather(
        *(
            asyncio.create_task(test_file_to_file(context, communication_pattern))
            for _ in range(20)
        )
    )
