from __future__ import annotations

import asyncio
import base64
import glob
import hashlib
import os
import posixpath
import shutil
from collections.abc import MutableMapping, MutableSequence
from email.message import Message
from typing import TYPE_CHECKING

import aiohttp
from aiohttp import ClientResponse

from streamflow.core import utils
from streamflow.core.data import DataType, FileType
from streamflow.core.exception import WorkflowExecutionException

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.deployment import Connector, ExecutionLocation
    from streamflow.core.scheduling import Hardware


def _check_status(
    command: MutableSequence[str], location: ExecutionLocation, result: str, status: int
):
    if status != 0:
        raise WorkflowExecutionException(
            "{} Command '{}' on location {}: {}".format(
                status, " ".join(command), location, result
            )
        )


def _file_checksum_local(path: str) -> str:
    with open(path, "rb") as f:
        sha1_checksum = hashlib.new("sha1", usedforsecurity=False)
        while data := f.read(2**16):
            sha1_checksum.update(data)
        return sha1_checksum.hexdigest()


def _get_filename_from_response(response: ClientResponse, url: str):
    if cd_header := response.headers.get("Content-Disposition"):
        message = Message()
        message["content-disposition"] = cd_header
        if filename := message.get_param("filename", header="content-disposition"):
            return filename
    return url.rsplit("/", 1)[-1]


def _listdir_local(path: str, file_type: FileType | None) -> MutableSequence[str]:
    content = []
    dir_content = os.listdir(path)
    check = (
        (os.path.isfile if file_type == FileType.FILE else os.path.isdir)
        if file_type is not None
        else None
    )
    for element in dir_content:
        element_path = os.path.join(path, element)
        if check and check(element_path):
            content.append(element_path)
    return content


async def checksum(
    context: StreamFlowContext,
    connector: Connector,
    location: ExecutionLocation | None,
    path: str,
) -> str | None:
    if location.local:
        if os.path.isfile(path):
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                context.process_executor, _file_checksum_local, path
            )
        else:
            return None
    else:
        command = [f'test -f "{path}" && sha1sum "{path}" | awk \'{{print $1}}\'']
        result, status = await connector.run(
            location=location, command=command, capture_output=True
        )
        if status > 1:
            raise WorkflowExecutionException(
                "{} Command '{}' on location {}: {}".format(
                    status, command, location, result
                )
            )
        return result.strip()


async def download(
    connector: Connector,
    location: ExecutionLocation | None,
    url: str,
    parent_dir: str,
) -> str:
    await mkdir(connector, location, parent_dir)
    if location.local:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    filepath = os.path.join(
                        parent_dir, _get_filename_from_response(response, url)
                    )
                    with open(filepath, mode="wb") as f:
                        f.write(await response.read())
                else:
                    raise Exception(
                        f"Downloading {url} failed with status {response.status}:\n{response.content}"
                    )
    else:
        async with aiohttp.ClientSession() as session:
            async with session.head(url, allow_redirects=True) as response:
                if response.status == 200:
                    filepath = posixpath.join(
                        parent_dir, _get_filename_from_response(response, url)
                    )
                else:
                    raise Exception(
                        f"Downloading {url} failed with status {response.status}:\n{response.content}"
                    )
        await connector.run(
            location=location,
            command=[
                f'if [ command -v curl ]; then curl -L -o "{filepath}" "{url}"; '
                f'else wget -O "{filepath}" "{url}"; fi'
            ],
        )
    return filepath


async def exists(
    connector: Connector, location: ExecutionLocation | None, path: str
) -> bool:
    if location.local:
        return os.path.exists(path)
    else:
        command = [f'test -e "{path}"']
        result, status = await connector.run(
            location=location, command=command, capture_output=True
        )
        if status > 1:
            raise WorkflowExecutionException(
                "{} Command '{}' on location {}: {}".format(
                    status, command, location, result
                )
            )
        else:
            return not status


async def follow_symlink(
    context: StreamFlowContext,
    connector: Connector,
    location: ExecutionLocation | None,
    path: str,
) -> str | None:
    """
    Get resolved symbolic links or canonical file names

    :param context: the `StreamFlowContext` object with global application status.
    :param connector: the `Connector` object to communicate with the location
    :param location: the `ExecutionLocation` object with the location information
    :param path: the path to be resolved in the case of symbolic link
    :return: the path of the resolved symlink or `None` if the link points to a location that does not exist
    """
    if location.local:
        return os.path.realpath(path) if os.path.exists(path) else None
    else:
        # If at least one primary location is present on the site, return its path
        if locations := context.data_manager.get_data_locations(
            path=path,
            deployment=connector.deployment_name,
            location_name=location.name,
            data_type=DataType.PRIMARY,
        ):
            return next(iter(locations)).path
        # Otherwise, analyse the remote path
        command = [f'test -e "{path}" && readlink -f "{path}"']
        result, status = await connector.run(
            location=location, command=command, capture_output=True
        )
        if status > 1:
            raise WorkflowExecutionException(
                "{} Command '{}' on location {}: {}".format(
                    status, command, location, result
                )
            )
        return result.strip() if status == 0 else None


async def get_storage_usages(
    connector: Connector, location: ExecutionLocation, hardware: Hardware
) -> MutableMapping[str, int]:
    """
    Get the actual size of the hardware storage paths

    Warn. Storage keys are not mount points.
    Since the meaning of storage dictionary keys depends on each `HardwareRequirement` implementation,
    no assumption about the key meaning should be made.

    :param connector: the `Connector` object to communicate with the location
    :param location: the `ExecutionLocation` object with the location information
    :param hardware: the `Hardware` which contains the paths to discover size.
    :return: a map with the `key` of the `hardware` storage and the size of the paths in the `hardware` storage
    """

    # It is not an accurate snapshot of the resources used
    # Eventual follow links inside the Storage paths should be resolved but checking their mount points.
    return dict(
        zip(
            hardware.storage.keys(),
            await asyncio.gather(
                *(
                    asyncio.create_task(
                        size(
                            connector=connector,
                            location=location,
                            path=list(storage.paths),
                        )
                    )
                    for storage in hardware.storage.values()
                )
            ),
        )
    )


async def head(
    connector: Connector, location: ExecutionLocation | None, path: str, num_bytes: int
) -> str:
    if location.local:
        with open(path, "rb") as f:
            return f.read(num_bytes).decode("utf-8")
    else:
        command = ["head", "-c", str(num_bytes), path]
        result, status = await connector.run(
            location=location, command=command, capture_output=True
        )
        _check_status(command, location, result, status)
        return result.strip()


async def isdir(
    connector: Connector, location: ExecutionLocation | None, path: str
) -> bool:
    if location.local:
        return os.path.isdir(path)
    else:
        command = [f'test -d "{path}"']
        result, status = await connector.run(
            location=location, command=command, capture_output=True
        )
        if status > 1:
            raise WorkflowExecutionException(
                "{} Command '{}' on location {}: {}".format(
                    status, command, location, result
                )
            )
        else:
            return not status


async def isfile(
    connector: Connector, location: ExecutionLocation | None, path: str
) -> bool:
    if location.local:
        return os.path.isfile(path)
    else:
        command = [f'test -f "{path}"']
        result, status = await connector.run(
            location=location, command=command, capture_output=True
        )
        if status > 1:
            raise WorkflowExecutionException(
                "{} Command '{}' on location {}: {}".format(
                    status, command, location, result
                )
            )
        else:
            return not status


async def islink(
    connector: Connector, location: ExecutionLocation | None, path: str
) -> bool:
    if location.local:
        return os.path.islink(path)
    else:
        command = [f'test -L "{path}"']
        result, status = await connector.run(
            location=location, command=command, capture_output=True
        )
        if status > 1:
            raise WorkflowExecutionException(
                "{} Command '{}' on location {}: {}".format(
                    status, command, location, result
                )
            )
        else:
            return not status


async def listdir(
    connector: Connector,
    location: ExecutionLocation | None,
    path: str,
    file_type: FileType | None = None,
) -> MutableSequence[str]:
    if location.local:
        return _listdir_local(path, file_type)
    else:
        command = 'find -L "{path}" -mindepth 1 -maxdepth 1 {type}'.format(
            path=path,
            type=(
                "-type {type}".format(
                    type="d" if file_type == FileType.DIRECTORY else "f"
                )
                if file_type is not None
                else ""
            ),
        ).split()
        content, status = await connector.run(
            location=location, command=command, capture_output=True
        )
        _check_status(command, location, content, status)
        content = content.strip(" \n")
        return content.splitlines() if content else []


async def mkdir(
    connector: Connector,
    location: ExecutionLocation,
    path: str | MutableSequence[str],
) -> None:
    paths = path if isinstance(path, MutableSequence) else [path]
    if location.local:
        for path in paths:
            os.makedirs(path, exist_ok=True)
    else:
        command = ["mkdir", "-p"] + list(paths)
        result, status = await connector.run(
            location=location, command=command, capture_output=True
        )
        _check_status(command, location, result, status)


async def read(
    connector: Connector, location: ExecutionLocation | None, path: str
) -> str:
    if location.local:
        with open(path, "rb") as f:
            return f.read().decode("utf-8")
    else:
        command = ["cat", path]
        result, status = await connector.run(
            location=location, command=command, capture_output=True
        )
        _check_status(command, location, result, status)
        return result.strip()


async def resolve(
    connector: Connector, location: ExecutionLocation | None, pattern: str
) -> MutableSequence[str] | None:
    if location.local:
        return sorted(glob.glob(pattern))
    else:
        command = [
            "printf",
            '"%s\\0"',
            pattern,
            "|",
            "xargs",
            "-0",
            "-I{}",
            "sh",
            "-c",
            '"if [ -e \\"{}\\" ]; then echo \\"{}\\"; fi"',
            "|",
            "sort",
        ]
        result, status = await connector.run(
            location=location, command=command, capture_output=True
        )
        _check_status(command, location, result, status)
        return result.split()


async def rm(
    connector: Connector,
    location: ExecutionLocation | None,
    path: str | MutableSequence[str],
) -> None:
    if location.local:
        if isinstance(path, MutableSequence):
            for p in path:
                if os.path.exists(p):
                    if os.path.islink(p):
                        os.remove(p)
                    elif os.path.isdir(p):
                        shutil.rmtree(p)
                    else:
                        os.remove(p)
        else:
            if os.path.exists(path):
                if os.path.islink(path):
                    os.remove(path)
                elif os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
    else:
        if isinstance(path, MutableSequence):
            path = " ".join([f'"{p}"' for p in path])
        else:
            path = f'"{path}"'
        await connector.run(location=location, command=["".join(["rm -rf ", path])])


async def size(
    connector: Connector,
    location: ExecutionLocation | None,
    path: str | MutableSequence[str],
) -> int:
    """
    Get the data size.

    If data reside in the local location, Python functions are called to get the size.
    Indeed, Python functions are much faster than new processes calling shell commands,
    and the Python stack is more portable across different platforms.
    Otherwise, a Linux shell command is executed to get the data size from remote locations.

    :param connector: the `Connector` object to communicate with the location
    :param location: the `ExecutionLocation` object with the location information
    :return: the sum of all the input path sizes, expressed in bytes
    """
    if not path:
        return 0
    elif location.local:
        if not isinstance(path, MutableSequence):
            path = [path]
        return sum(utils.get_size(p) for p in path)
    else:
        command = [
            "".join(
                [
                    "find -L ",
                    (
                        " ".join([f'"{p}"' for p in path])
                        if isinstance(path, MutableSequence)
                        else f'"{path}"'
                    ),
                    " -type f -exec ls -ln {} \\+ | ",
                    "awk 'BEGIN {sum=0} {sum+=$5} END {print sum}'; ",
                ]
            )
        ]
        result, status = await connector.run(
            location=location, command=command, capture_output=True
        )
        _check_status(command, location, result, status)
        result = result.strip().strip("'\"")
        return int(result) if result.isdigit() else 0


async def write(
    connector: Connector, location: ExecutionLocation | None, path: str, content: str
) -> None:
    if location.local:
        with open(path, "w") as f:
            f.write(content)
    else:
        command = [
            "echo",
            base64.b64encode(content.encode("utf-8")).decode("utf-8"),
            "|",
            "base64",
            "-d",
        ]
        result, status = await connector.run(
            location=location, command=command, stdout=path, capture_output=True
        )
        _check_status(command, location, result, status)
