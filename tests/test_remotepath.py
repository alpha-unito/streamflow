import posixpath
import tempfile

import pytest
import pytest_asyncio

from streamflow.core import utils
from streamflow.core.data import FileType
from streamflow.core.deployment import Connector, ExecutionLocation
from streamflow.data import remotepath
from streamflow.deployment.connector import LocalConnector
from streamflow.deployment.utils import get_path_processor
from tests.utils.deployment import get_location


@pytest_asyncio.fixture(scope="module")
async def location(context, deployment_src) -> ExecutionLocation:
    return await get_location(context, deployment_src)


@pytest.fixture(scope="module")
def connector(context, location) -> Connector:
    return context.deployment_manager.get_connector(location.deployment)


@pytest.mark.asyncio
async def test_directory(context, connector, location):
    """Test directory creation and deletion."""
    path = utils.random_name()
    try:
        await remotepath.mkdir(connector, [location], path)
        assert await remotepath.exists(connector, location, path)
        assert await remotepath.isdir(connector, location, path)
        # ./
        #   file1.txt
        #   file2.csv
        #   dir1/
        #   dir2/
        await remotepath.mkdirs(
            connector,
            [location],
            [posixpath.join(path, "dir1"), posixpath.join(path, "dir2")],
        )
        await remotepath.write(
            connector, location, posixpath.join(path, "file1.txt"), "StreamFlow"
        )
        await remotepath.write(
            connector, location, posixpath.join(path, "file2.csv"), "StreamFlow"
        )
        files = await remotepath.listdir(connector, location, path, FileType.FILE)
        assert len(files) == 2
        assert posixpath.join(path, "file1.txt") in files
        assert posixpath.join(path, "file2.csv") in files
        dirs = await remotepath.listdir(connector, location, path, FileType.DIRECTORY)
        assert len(dirs) == 2
        assert posixpath.join(path, "dir1") in dirs
        assert posixpath.join(path, "dir2") in dirs
        await remotepath.rm(connector, location, path)
        assert not await remotepath.exists(connector, location, path)
    finally:
        if await remotepath.exists(connector, location, path):
            await remotepath.rm(connector, location, path)


@pytest.mark.asyncio
async def test_download(context, connector, location):
    """Test remote file download."""
    urls = [
        "https://raw.githubusercontent.com/alpha-unito/streamflow/master/LICENSE",
        "https://github.com/alpha-unito/streamflow/archive/refs/tags/0.1.6.zip",
    ]
    parent_dir = (
        tempfile.gettempdir() if isinstance(connector, LocalConnector) else "/tmp"
    )
    path_processor = get_path_processor(connector)
    paths = [
        path_processor.join(parent_dir, "LICENSE"),
        path_processor.join(parent_dir, "streamflow-0.1.6.zip"),
    ]

    for i, url in enumerate(urls):
        try:
            path = await remotepath.download(connector, [location], url, parent_dir)
            assert path == paths[i]
            assert await remotepath.exists(connector, location, path)
        finally:
            if await remotepath.exists(connector, location, path):
                await remotepath.rm(connector, location, path)


@pytest.mark.asyncio
async def test_file(context, connector, location):
    """Test file creation, size, checksum and deletion."""
    path = utils.random_name()
    path2 = utils.random_name()
    try:
        await remotepath.write(connector, location, path, "StreamFlow")
        assert await remotepath.exists(connector, location, path)
        assert await remotepath.isfile(connector, location, path)
        assert await remotepath.size(connector, location, path) == 10
        await remotepath.write(connector, location, path2, "CWL")
        assert await remotepath.exists(connector, location, path2)
        assert await remotepath.size(connector, location, [path, path2]) == 13
        digest = await remotepath.checksum(context, connector, location, path)
        assert digest == "e8abb7445e1c4061c3ef39a0e1690159b094d3b5"
        await remotepath.rm(connector, location, [path, path2])
        assert not await remotepath.exists(connector, location, path)
        assert not await remotepath.exists(connector, location, path2)
    finally:
        if await remotepath.exists(connector, location, path):
            await remotepath.rm(connector, location, path)
        if await remotepath.exists(connector, location, path2):
            await remotepath.rm(connector, location, path2)


@pytest.mark.asyncio
async def test_resolve(context, connector, location):
    """Test glob resolution."""
    path_processor = get_path_processor(connector)
    path = utils.random_name()
    await remotepath.mkdir(connector, [location], path)
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
        await remotepath.write(
            connector, location, path_processor.join(path, "file1.txt"), "StreamFlow"
        )
        await remotepath.write(
            connector, location, path_processor.join(path, "file2.csv"), "StreamFlow"
        )
        await remotepath.mkdir(
            connector, [location], path_processor.join(path, "dir1", "dir2")
        )
        await remotepath.write(
            connector,
            location,
            path_processor.join(path, "dir1", "file1.txt"),
            "StreamFlow",
        )
        await remotepath.write(
            connector,
            location,
            path_processor.join(path, "dir1", "file2.csv"),
            "StreamFlow",
        )
        await remotepath.write(
            connector,
            location,
            path_processor.join(path, "dir1", "dir2", "file1.txt"),
            "StreamFlow",
        )
        await remotepath.write(
            connector,
            location,
            path_processor.join(path, "dir1", "dir2", "file2.csv"),
            "StreamFlow",
        )
        # Test *.txt
        result = await remotepath.resolve(
            connector, location, path_processor.join(path, "*.txt")
        )
        assert len(result) == 1
        assert path_processor.join(path, "file1.txt") in result
        # Test file*
        result = await remotepath.resolve(
            connector, location, path_processor.join(path, "file*")
        )
        assert len(result) == 2
        assert path_processor.join(path, "file1.txt") in result
        assert path_processor.join(path, "file2.csv") in result
        # Test */*.txt
        result = await remotepath.resolve(
            connector, location, path_processor.join(path, "*/*.txt")
        )
        assert len(result) == 1
        assert path_processor.join(path, "dir1", "file1.txt") in result
    finally:
        await remotepath.rm(connector, location, path)


@pytest.mark.asyncio
async def test_symlink(context, connector, location):
    """Test symlink creation, resolution and deletion."""
    src = utils.random_name()
    path = utils.random_name()
    path_processor = get_path_processor(connector)
    try:
        # Test symlink to file
        await remotepath.write(connector, location, src, "StreamFlow")
        await remotepath.symlink(connector, location, src, path)
        assert await remotepath.exists(connector, location, path)
        assert await remotepath.islink(connector, location, path)
        assert (
            path_processor.basename(
                await remotepath.follow_symlink(context, connector, location, path)
            )
            == src
        )
        await remotepath.rm(connector, location, path)
        assert not await remotepath.exists(connector, location, path)
        await remotepath.rm(connector, location, src)
        # Test symlink to directory
        await remotepath.mkdir(connector, [location], src)
        await remotepath.symlink(connector, location, src, path)
        assert await remotepath.exists(connector, location, path)
        assert await remotepath.islink(connector, location, path)
        assert (
            path_processor.basename(
                await remotepath.follow_symlink(context, connector, location, path)
            )
            == src
        )
        await remotepath.rm(connector, location, path)
        assert not await remotepath.exists(connector, location, path)
    finally:
        await remotepath.rm(connector, location, path)
        await remotepath.rm(connector, location, src)
