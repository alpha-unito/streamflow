from __future__ import annotations

import tempfile

import pytest
import pytest_asyncio

from streamflow.core import utils
from streamflow.core.deployment import Connector, ExecutionLocation
from streamflow.core.exception import WorkflowExecutionException
from streamflow.data import remotepath
from streamflow.data.remotepath import StreamFlowPath
from tests.utils.deployment import get_docker_deployment_config, get_location


@pytest_asyncio.fixture(scope="module")
async def location(context, deployment_src) -> ExecutionLocation:
    return await get_location(context, deployment_src)


@pytest.fixture(scope="module")
def connector(context, location) -> Connector:
    return context.deployment_manager.get_connector(location.deployment)


@pytest.mark.asyncio
async def test_directory(context, connector, location):
    """Test directory creation and deletion."""
    path = StreamFlowPath(utils.random_name(), context=context, location=location)
    try:
        await path.mkdir(mode=0o777)
        assert await path.exists()
        assert await path.is_dir()
        # ./
        #   file1.txt
        #   file2.csv
        #   dir1/
        #   dir2/
        await (path / "dir1").mkdir(mode=0o777)
        await (path / "dir2").mkdir(mode=0o777)
        await (path / "file1.txt").write_text("StreamFlow")
        await (path / "file2.csv").write_text("StreamFlow")
        async for dirpath, dirnames, filenames in path.walk(follow_symlinks=True):
            assert len(dirnames) == 2
            assert "dir1" in dirnames
            assert "dir2" in dirnames
            assert len(filenames) == 2
            assert "file1.txt" in filenames
            assert "file2.csv" in filenames
            break
        await path.rmtree()
        assert not await path.exists()
    finally:
        await path.rmtree()


@pytest.mark.asyncio
async def test_download(context, connector, location):
    """Test remote file download."""
    urls = [
        "https://raw.githubusercontent.com/alpha-unito/streamflow/master/LICENSE",
        "https://github.com/alpha-unito/streamflow/archive/refs/tags/0.1.6.zip",
    ]
    parent_dir = StreamFlowPath(
        tempfile.gettempdir() if location.local else "/tmp",
        context=context,
        location=location,
    )
    paths = [
        parent_dir / "LICENSE",
        parent_dir / "streamflow-0.1.6.zip",
    ]
    path = None
    for i, url in enumerate(urls):
        try:
            path = await remotepath.download(context, location, url, str(parent_dir))
            assert path == paths[i]
            assert await path.exists()
        finally:
            await path.rmtree()


@pytest.mark.asyncio
async def test_file(context, connector, location):
    """Test file creation, size, checksum and deletion."""
    path = StreamFlowPath(utils.random_name(), context=context, location=location)
    path2 = StreamFlowPath(utils.random_name(), context=context, location=location)
    try:
        await path.write_text("StreamFlow")
        assert await path.exists()
        assert await path.is_file()
        assert await path.size() == 10
        await path2.write_text("CWL")
        assert await path2.exists()
        assert await path.size() + await path2.size() == 13
        assert await path.checksum() == "e8abb7445e1c4061c3ef39a0e1690159b094d3b5"
        await path.rmtree()
        await path2.rmtree()
        assert not await path.exists()
        assert not await path2.exists()
    finally:
        await path.rmtree()
        await path2.rmtree()


@pytest.mark.asyncio
async def test_glob(context, connector, location):
    """Test glob resolution."""
    path = StreamFlowPath(utils.random_name(), context=context, location=location)
    await path.mkdir(mode=0o777)
    try:
        # ./
        #   file1.txt
        #   file2.csv
        #   dir1/
        #     file1.txt
        #     file2.csv
        #     dir2/
        #       file1.txt
        #       file2.csv
        await (path / "file1.txt").write_text("StreamFlow")
        await (path / "file2.csv").write_text("StreamFlow")
        await (path / "dir1" / "dir2").mkdir(mode=0o777, parents=True)
        await (path / "dir1" / "file1.txt").write_text("StreamFlow")
        await (path / "dir1" / "file2.csv").write_text("StreamFlow")
        await (path / "dir1" / "dir2" / "file1.txt").write_text("StreamFlow")
        await (path / "dir1" / "dir2" / "file2.csv").write_text("StreamFlow")
        # Test *.txt
        result = [p async for p in path.glob("*.txt")]
        assert len(result) == 1
        assert path / "file1.txt" in result
        # Test file*
        result = [p async for p in path.glob("file*")]
        assert len(result) == 2
        assert path / "file1.txt" in result
        assert path / "file2.csv" in result
        # Test */*.txt
        result = [p async for p in path.glob("*/*.txt")]
        assert len(result) == 1
        assert path / "dir1" / "file1.txt" in result
    finally:
        await path.rmtree()


@pytest.mark.asyncio
async def test_mkdir_failure(context):
    """Test on `mkdir` function failure"""
    deployment_config = get_docker_deployment_config()
    location = await get_location(context, deployment_config.type)

    # Create a file and try to create a directory with the same name
    path = StreamFlowPath(utils.random_name(), context=context, location=location)
    mode = 0o777
    await path.write_text("StreamFlow")
    with pytest.raises(WorkflowExecutionException) as err:
        await path.mkdir(mode=mode)
    expected_msg_err = f"1 Command 'mkdir -m {mode:o} {path}' on location {location}: mkdir: can't create directory '{path}': File exists"
    assert expected_msg_err in str(err.value)


@pytest.mark.asyncio
async def test_symlink(context, connector, location):
    """Test symlink creation, resolution and deletion."""
    src = StreamFlowPath(utils.random_name(), context=context, location=location)
    path = StreamFlowPath(utils.random_name(), context=context, location=location)
    try:
        # Test symlink to file
        await src.write_text("StreamFlow")
        await path.symlink_to(src)
        assert await path.exists()
        assert await path.is_symlink()
        assert (await path.resolve()).name == str(src)
        await path.rmtree()
        assert not await path.exists()
        await src.rmtree()
        # Test symlink to directory
        await src.mkdir(mode=0o777)
        await path.symlink_to(src, target_is_directory=True)
        assert await path.exists()
        assert await path.is_symlink()
        assert (await path.resolve()).name == str(src)
        await path.rmtree()
        assert not await path.exists()
    finally:
        await path.rmtree()
        await src.rmtree()
