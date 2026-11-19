from __future__ import annotations

import asyncio
import random
import string
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from streamflow.data.remotepath import StreamFlowPath

# Constants for small/fast test scenario
FILE_SIZE_BYTES = 102400  # 100 KB
NUM_FILES = 10
NUM_SUBDIRS = 3
DIR_DEPTH = 2


def benchmark_async(
    benchmark: Any,
    target_func: Callable,
    setup_func: Callable | None = None,
    teardown_func: Callable | None = None,
) -> Any:
    """
    Wrapper to run async functions in pytest-benchmark with proper setup/teardown.

    pytest-benchmark doesn't natively support async functions,
    so we need to run them in the event loop. This wrapper ensures that:
    - setup_func runs once before benchmarking
    - target_func is benchmarked (repeated iterations)
    - teardown_func runs once after benchmarking

    Note: setup and teardown happen outside the benchmark timing.

    :param benchmark: pytest-benchmark fixture
    :param target_func: Async function to benchmark (called multiple times)
    :param setup_func: Optional async setup function (called once before timing)
    :param teardown_func: Optional async teardown function (called once after timing)
    :return: Result of the last target_func call
    """
    loop = asyncio.get_event_loop()
    last_result = None

    # Run setup once before benchmarking
    if setup_func:
        loop.run_until_complete(setup_func())

    # Define the sync wrapper for the benchmark target
    def sync_wrapper() -> Any:
        nonlocal last_result
        last_result = loop.run_until_complete(target_func())
        return last_result

    # Run the benchmark with configured iterations and rounds for statistical significance
    benchmark.pedantic(sync_wrapper, iterations=100, rounds=10, warmup_rounds=1)

    # Run teardown once after benchmarking
    if teardown_func:
        loop.run_until_complete(teardown_func())

    return last_result


def calculate_overhead_percent(baseline_time: float, measured_time: float) -> float:
    """
    Calculate overhead percentage compared to baseline.

    :param baseline_time: Baseline time (usually local deployment)
    :param measured_time: Measured time for the operation
    :return: Overhead percentage
    """
    if baseline_time <= 0:
        return 0
    return ((measured_time - baseline_time) / baseline_time) * 100


def calculate_throughput_items_per_sec(num_items: int, time_seconds: float) -> float:
    """
    Calculate items/s throughput.

    :param num_items: Number of items processed
    :param time_seconds: Time taken in seconds
    :return: Throughput in items/s
    """
    return num_items / time_seconds if time_seconds > 0 else 0


def calculate_throughput_mb_per_sec(size_bytes: int, time_seconds: float) -> float:
    """
    Calculate MB/s throughput.

    :param size_bytes: Size of data processed
    :param time_seconds: Time taken in seconds
    :return: Throughput in MB/s
    """
    mb = size_bytes / (1024 * 1024)
    return mb / time_seconds if time_seconds > 0 else 0


async def create_directory_structure(
    base_path: StreamFlowPath,
    num_files: int = NUM_FILES,
    num_subdirs: int = NUM_SUBDIRS,
) -> dict[str, Any]:
    """
    Create directory with files distributed across subdirectories.

    Structure: 10 files in 3 subdirectories (3-4 files per subdir)
    test_dir/
        subdir_0/
            file_0.txt
            file_1.txt
            file_2.txt
        subdir_1/
            file_3.txt
            ...

    :param base_path: Base directory path
    :param num_files: Total number of files to create
    :param num_subdirs: Number of subdirectories
    :return: Metadata dict with num_files, num_dirs, total_size_bytes, file_paths
    """
    await base_path.mkdir(parents=True, exist_ok=True)

    file_paths = []
    files_per_dir = num_files // num_subdirs

    for i in range(num_subdirs):
        subdir = base_path / f"subdir_{i}"
        await subdir.mkdir(parents=True, exist_ok=True)

        for j in range(files_per_dir):
            file_path = subdir / f"file_{i * files_per_dir + j}.txt"
            await create_test_file(file_path)
            file_paths.append(file_path)

    return {
        "num_files": num_files,
        "num_dirs": num_subdirs,
        "total_size_bytes": num_files * FILE_SIZE_BYTES,
        "file_paths": file_paths,
    }


async def create_test_file(
    path: StreamFlowPath, size_bytes: int = FILE_SIZE_BYTES
) -> None:
    """
    Create a test file with specified size.

    :param path: Path where to create the file
    :param size_bytes: Size of file content in bytes
    """
    content = generate_file_content(size_bytes)
    await path.write_text(content)


def generate_file_content(size_bytes: int = FILE_SIZE_BYTES) -> str:
    """
    Generate random ASCII text content of specified size.

    :param size_bytes: Size of content to generate in bytes
    :return: Random text string
    """
    return "".join(
        random.choices(string.ascii_letters + string.digits + "\n", k=size_bytes)
    )
