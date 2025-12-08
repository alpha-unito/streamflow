# import tarfile
# import sys
# import os
#
# # Define the supported formats for easy reference and validation
# TAR_FORMATS = {
#     "v7": tarfile.USTAR_FORMAT,  # V7 format (classic)
#     "ustar": tarfile.USTAR_FORMAT,  # POSIX.1-1988 format (default for older Python versions)
#     "gnu": tarfile.GNU_FORMAT,  # GNU tar format
#     "pax": tarfile.PAX_FORMAT,  # POSIX.1-2001 (pax) format (default in modern Python)
#     # "oldgnu": tarfile.NOGNU_FORMAT  # GNU tar format, but without the special type extensions
# }
#
#
# def tar_to_stdout(source_dir, format_name="pax"):
#     """
#     Creates a tar archive of a directory and streams the binary content
#     to standard output (sys.stdout.buffer) using a specified format.
#
#     Args:
#         source_dir (str): The directory to archive.
#         format_name (str): The desired tar format ('pax', 'gnu', 'ustar', etc.).
#     """
#
#     # 1. Input Validation and Format Mapping
#     if not os.path.isdir(source_dir):
#         print(f"Error: Source directory '{source_dir}' not found.", file=sys.stderr)
#         sys.exit(1)
#
#     format_name = format_name.lower()
#     format_constant = TAR_FORMATS.get(format_name)
#
#     if format_constant is None:
#         valid_formats = ", ".join(TAR_FORMATS.keys())
#         print(f"Error: Invalid format '{format_name}'. Must be one of: {valid_formats}", file=sys.stderr)
#         sys.exit(1)
#
#     print(f"--- Archiving directory '{source_dir}' using {format_name.upper()} format to stdout... ---",
#           file=sys.stderr)
#
#     # 2. Open the TarFile object with the specified format
#     # The 'format=format_constant' argument sets the tar encoding standard.
#     # 'w|' mode is used for non-seekable streams like stdout.
#     try:
#         with tarfile.open(
#             fileobj=sys.stdout.buffer,
#             mode='w|',
#             format=format_constant
#         ) as tar:
#             # 3. Add the directory content
#             tar.add(source_dir, arcname=os.path.basename(source_dir))
#
#     except tarfile.TarError as e:
#         print(f"Tar creation error: {e}", file=sys.stderr)
#         sys.exit(1)
#
#     print(f"--- Tar stream finished. ---", file=sys.stderr)
#
#
# # --- Example Usage ---
# if __name__ == "__main__":
#     # Setup temporary data
#     if not os.path.exists("test_data"):
#         os.makedirs("test_data")
#     with open("test_data/large_name_file_for_testing_formats.txt", "w") as f:
#         f.write("This file name is long to test ustar/v7 limits.")
#
#     # Example 1: Use the PAX format (default and most modern/flexible)
#     print("\n--- Testing PAX format (default) ---", file=sys.stderr)
#     tar_to_stdout("test_data", format_name="pax")
#
#     # # Example 2: Use the GNU format
#     # # This format is often necessary for compatibility with older systems or specific tar implementations.
#     # print("\n--- Testing GNU format ---", file=sys.stderr)
#     # tar_to_stdout("test_data", format_name="gnu")
#     #
#     # # Example 3: Use the USTAR format
#     # # If the file name is too long (>100 chars), USTAR will raise an error or truncate the name.
#     # print("\n--- Testing USTAR format ---", file=sys.stderr)
#     # # Note: USTAR might fail or truncate the filename above if it's too long,
#     # # depending on how tarfile handles it by default.
#     # tar_to_stdout("test_data", format_name="ustar")
import tempfile

import pytest

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataType
from streamflow.data.remotepath import StreamFlowPath
from tests.utils.deployment import (
    get_aiotar_deployment_config,
    get_local_deployment_config,
    get_location,
)


@pytest.mark.asyncio
async def test_aaaa(context: StreamFlowContext) -> None:
    deployment_config = get_aiotar_deployment_config()
    src_location = await get_location(context, deployment_config.type)
    deployment_config = get_local_deployment_config()
    dst_location = await get_location(context, deployment_config.type)

    # Create a file and try to create a directory with the same name
    src_path = StreamFlowPath(
        tempfile.gettempdir() if src_location.local else "/tmp",
        utils.random_name(),
        context=context,
        location=src_location,
    )
    dst_path = (src_path.parent / utils.random_name())
    await dst_path.mkdir()
    # await src_path.write_text("StreamFlow")
    await src_path.mkdir(parents=True, exist_ok=True)
    assert await src_path.exists()
    await (src_path / "base").write_text("Streamflow")
    await (src_path / "base1").symlink_to(src_path / "base")
    assert await src_path.exists()

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
