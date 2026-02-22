from __future__ import annotations

import pytest

from benchmark.utils import (
    FILE_SIZE_BYTES,
    benchmark_async,
    create_directory_structure,
    create_test_file,
)
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import ExecutionLocation
from streamflow.core.utils import random_name
from streamflow.data.remotepath import StreamFlowPath

# ============================================================================
# FILE QUERY OPERATIONS
# ============================================================================


def test_exists(
    benchmark,
    location: ExecutionLocation,
    connector_type: str,
    context: StreamFlowContext,
    benchmark_tmpdir: str,
) -> None:
    """Benchmark path.exists() operation."""
    path = StreamFlowPath(
        benchmark_tmpdir,
        random_name(),
        context=context,
        location=location,
    )

    async def setup():
        await create_test_file(path)

    async def target():
        return await path.exists()

    async def teardown():
        await path.rmtree()

    assert benchmark_async(benchmark, target, setup, teardown) is True


def test_is_dir(
    benchmark,
    location: ExecutionLocation,
    connector_type: str,
    context: StreamFlowContext,
    benchmark_tmpdir: str,
) -> None:
    """Benchmark path.is_dir() operation."""
    path = StreamFlowPath(
        benchmark_tmpdir,
        random_name(),
        context=context,
        location=location,
    )

    async def setup():
        await path.mkdir(parents=True, exist_ok=True)

    async def target():
        return await path.is_dir()

    async def teardown():
        await path.rmtree()

    assert benchmark_async(benchmark, target, setup, teardown) is True


def test_is_file(
    benchmark,
    location: ExecutionLocation,
    connector_type: str,
    context: StreamFlowContext,
    benchmark_tmpdir: str,
) -> None:
    """Benchmark path.is_file() operation."""
    path = StreamFlowPath(
        benchmark_tmpdir,
        random_name(),
        context=context,
        location=location,
    )

    async def setup():
        await create_test_file(path)

    async def target():
        return await path.is_file()

    async def teardown():
        await path.rmtree()

    assert benchmark_async(benchmark, target, setup, teardown) is True


def test_is_symlink(
    benchmark,
    location: ExecutionLocation,
    connector_type: str,
    context: StreamFlowContext,
    benchmark_tmpdir: str,
) -> None:
    """Benchmark path.is_symlink() operation."""
    src = StreamFlowPath(
        benchmark_tmpdir,
        random_name(),
        context=context,
        location=location,
    )
    link = StreamFlowPath(
        benchmark_tmpdir,
        random_name(),
        context=context,
        location=location,
    )

    async def setup():
        await create_test_file(src)
        await link.symlink_to(src)

    async def target():
        return await link.is_symlink()

    async def teardown():
        await link.rmtree()
        await src.rmtree()

    assert benchmark_async(benchmark, target, setup, teardown) is True


def test_size(
    benchmark,
    location: ExecutionLocation,
    connector_type: str,
    context: StreamFlowContext,
    benchmark_tmpdir: str,
) -> None:
    """Benchmark path.size() on 100KB file."""
    path = StreamFlowPath(
        benchmark_tmpdir,
        random_name(),
        context=context,
        location=location,
    )

    async def setup():
        await create_test_file(path)

    async def target():
        return await path.size()

    async def teardown():
        await path.rmtree()

    assert benchmark_async(benchmark, target, setup, teardown) == FILE_SIZE_BYTES


# ============================================================================
# I/O OPERATIONS
# ============================================================================


def test_checksum_small(
    benchmark,
    location: ExecutionLocation,
    connector_type: str,
    context: StreamFlowContext,
    benchmark_tmpdir: str,
) -> None:
    """Benchmark path.checksum() with 100KB file."""
    path = StreamFlowPath(
        benchmark_tmpdir,
        random_name(),
        context=context,
        location=location,
    )

    async def setup():
        await create_test_file(path, 2**10)

    async def target():
        return await path.checksum()

    async def teardown():
        await path.rmtree()

    assert benchmark_async(benchmark, target, setup, teardown) is not None


def test_checksum_large(
    benchmark,
    location: ExecutionLocation,
    connector_type: str,
    context: StreamFlowContext,
    benchmark_tmpdir: str,
) -> None:
    """Benchmark path.checksum() with 100KB file."""
    path = StreamFlowPath(
        benchmark_tmpdir,
        random_name(),
        context=context,
        location=location,
    )

    async def setup():
        await create_test_file(path)

    async def target():
        return await path.checksum()

    async def teardown():
        await path.rmtree()

    assert benchmark_async(benchmark, target, setup, teardown) is not None


def test_read_text(
    benchmark,
    location: ExecutionLocation,
    connector_type: str,
    context: StreamFlowContext,
    benchmark_tmpdir: str,
) -> None:
    """Benchmark path.read_text() with 100KB file."""
    path = StreamFlowPath(
        benchmark_tmpdir,
        random_name(),
        context=context,
        location=location,
    )

    async def setup():
        await create_test_file(path)

    async def target():
        return await path.read_text()

    async def teardown():
        await path.rmtree()

    assert len(benchmark_async(benchmark, target, setup, teardown)) == FILE_SIZE_BYTES


def test_write_text(
    benchmark,
    location: ExecutionLocation,
    connector_type: str,
    context: StreamFlowContext,
    benchmark_tmpdir: str,
) -> None:
    """Benchmark path.write_text() with 100KB file."""
    from benchmark.utils import generate_file_content

    # Create root directory for all iterations
    root_dir = StreamFlowPath(
        benchmark_tmpdir,
        random_name(),
        context=context,
        location=location,
    )

    # Generate content once (not benchmarked)
    content = generate_file_content(FILE_SIZE_BYTES)

    async def setup():
        await root_dir.mkdir()

    async def target():
        # Create unique file for each iteration
        path = root_dir / random_name()
        await path.write_text(content)

    async def teardown():
        # Cleanup all files at once
        await root_dir.rmtree()

    benchmark_async(benchmark, target, setup, teardown)


# ============================================================================
# FILESYSTEM OPERATIONS
# ============================================================================


def test_hardlink_to(
    benchmark,
    location: ExecutionLocation,
    connector_type: str,
    context: StreamFlowContext,
    benchmark_tmpdir: str,
) -> None:
    """Benchmark path.hardlink_to() operation."""
    # Create root directory for all iterations
    root_dir = StreamFlowPath(
        benchmark_tmpdir,
        random_name(),
        context=context,
        location=location,
    )

    src = root_dir / "source_file"

    async def setup():
        await root_dir.mkdir()
        await create_test_file(src)

    async def target():
        # Create unique link for each iteration
        link = root_dir / random_name()
        await link.hardlink_to(src)

    async def teardown():
        # Cleanup all files at once
        await root_dir.rmtree()

    benchmark_async(benchmark, target, setup, teardown)


def test_mkdir(
    benchmark,
    location: ExecutionLocation,
    connector_type: str,
    context: StreamFlowContext,
    benchmark_tmpdir: str,
) -> None:
    """Benchmark path.mkdir() operation."""
    # Create root directory for all iterations
    root_dir = StreamFlowPath(
        benchmark_tmpdir,
        random_name(),
        context=context,
        location=location,
    )

    async def setup():
        await root_dir.mkdir()

    async def target():
        # Create unique directory for each iteration
        path = root_dir / random_name()
        await path.mkdir(mode=0o777)

    async def teardown():
        # Cleanup all directories at once
        await root_dir.rmtree()

    benchmark_async(benchmark, target, setup, teardown)


def test_mkdir_parents(
    benchmark,
    location: ExecutionLocation,
    connector_type: str,
    context: StreamFlowContext,
    benchmark_tmpdir: str,
) -> None:
    """Benchmark path.mkdir(parents=True) operation."""
    # Create root directory for all iterations
    root_dir = StreamFlowPath(
        benchmark_tmpdir,
        random_name(),
        context=context,
        location=location,
    )

    async def setup():
        await root_dir.mkdir()

    async def target():
        # Create unique nested path for each iteration
        path = root_dir / random_name() / "nested" / "deep" / "directory"
        await path.mkdir(mode=0o777, parents=True)

    async def teardown():
        # Cleanup all nested directories at once
        await root_dir.rmtree()

    benchmark_async(benchmark, target, setup, teardown)


def test_resolve(
    benchmark,
    location: ExecutionLocation,
    connector_type: str,
    context: StreamFlowContext,
    benchmark_tmpdir: str,
) -> None:
    """Benchmark path.resolve() on symbolic link."""
    src = StreamFlowPath(
        benchmark_tmpdir,
        random_name(),
        context=context,
        location=location,
    )
    link = StreamFlowPath(
        benchmark_tmpdir,
        random_name(),
        context=context,
        location=location,
    )

    async def setup():
        await create_test_file(src)
        await link.symlink_to(src)

    async def target():
        return await link.resolve()

    async def teardown():
        await link.rmtree()
        await src.rmtree()

    assert benchmark_async(benchmark, target, setup, teardown) is not None


def test_rmtree(
    benchmark,
    location: ExecutionLocation,
    connector_type: str,
    context: StreamFlowContext,
    benchmark_tmpdir: str,
) -> None:
    """Benchmark path.rmtree() on 10 files in 3 dirs."""
    path = StreamFlowPath(
        benchmark_tmpdir,
        random_name(),
        context=context,
        location=location,
    )

    async def setup():
        await create_directory_structure(path)

    async def target():
        await path.rmtree()

    async def teardown():
        # Verify deletion succeeded
        assert not await path.exists()

    benchmark_async(benchmark, target, setup, teardown)


def test_symlink_to(
    benchmark,
    location: ExecutionLocation,
    connector_type: str,
    context: StreamFlowContext,
    benchmark_tmpdir: str,
) -> None:
    """Benchmark path.symlink_to() operation."""
    # Create root directory for all iterations
    root_dir = StreamFlowPath(
        benchmark_tmpdir,
        random_name(),
        context=context,
        location=location,
    )

    src = root_dir / "source_file"

    async def setup():
        await root_dir.mkdir()
        await create_test_file(src)

    async def target():
        # Create unique link for each iteration inside root_dir
        link = root_dir / random_name()
        await link.symlink_to(src)

    async def teardown():
        # Cleanup all links and source
        await root_dir.rmtree()

    benchmark_async(benchmark, target, setup, teardown)


# ============================================================================
# DIRECTORY TRAVERSAL
# ============================================================================


@pytest.mark.parametrize("pattern", ["**/*.txt", "*", "**/*"])
def test_glob(
    benchmark,
    pattern: str,
    location: ExecutionLocation,
    connector_type: str,
    context: StreamFlowContext,
    benchmark_tmpdir: str,
) -> None:
    """Benchmark path.glob(pattern) iteration with different patterns."""
    path = StreamFlowPath(
        benchmark_tmpdir,
        random_name(),
        context=context,
        location=location,
    )

    async def setup():
        await create_directory_structure(path)

    async def target():
        results = []
        async for p in path.glob(pattern):
            results.append(p)
        return results

    async def teardown():
        await path.rmtree()

    assert len(benchmark_async(benchmark, target, setup, teardown)) > 0


def test_walk(
    benchmark,
    location: ExecutionLocation,
    connector_type: str,
    context: StreamFlowContext,
    benchmark_tmpdir: str,
) -> None:
    """Benchmark path.walk() iteration on 10 files in 3 dirs."""
    path = StreamFlowPath(
        benchmark_tmpdir,
        random_name(),
        context=context,
        location=location,
    )

    async def setup():
        await create_directory_structure(path)

    async def target():
        results = []
        async for dirpath, dirnames, filenames in path.walk():
            results.append((dirpath, dirnames, filenames))
        return results

    async def teardown():
        await path.rmtree()

    assert len(benchmark_async(benchmark, target, setup, teardown)) > 0
