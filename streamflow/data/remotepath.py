from __future__ import annotations

import asyncio
import cgi
import errno
import glob
import os
import posixpath
from hashlib import sha1
from typing import TYPE_CHECKING, MutableSequence

import aiohttp

from streamflow.core import utils
from streamflow.core.data import FileType
from streamflow.core.exception import WorkflowExecutionException
from streamflow.log_handler import profile

if TYPE_CHECKING:
    from streamflow.core.deployment import Connector
    from typing import Union, Optional
    from typing_extensions import Text


def _check_status(result: Text, status: int):
    if status != 0:
        raise WorkflowExecutionException(result)


async def _file_checksum(connector: Optional[Connector], target: Optional[Text], path: Text):
    if connector is not None:
        result, status = await connector.run(
            resource=target,
            command=["sha1sum {path} | awk '{{print $1}}'".format(path=path)],
            capture_output=True)
        _check_status(result, status)
        return result.strip()
    else:
        with open(path, "rb") as f:
            sha1_checksum = sha1()
            while data := f.read(2**16):
                sha1_checksum.update(data)
            return sha1_checksum.hexdigest()


def _listdir_local(path: Text, file_type: FileType) -> MutableSequence[Text]:
    content = []
    dir_content = os.listdir(path)
    check = os.path.isfile if file_type == FileType.FILE else os.path.isdir
    for element in dir_content:
        element_path = os.path.join(path, element)
        if check(element_path):
            content.append(element_path)
    return content


@profile
async def checksum(connector: Optional[Connector], target: Optional[Text], path: Text) -> Text:
    if await isfile(connector, target, path):
        return await _file_checksum(connector, target, path)
    else:
        raise Exception("Checksum for folders is not implemented yet")


@profile
async def download(
        connector: Optional[Connector],
        targets: Optional[MutableSequence[Text]],
        url: Text, parent_dir: Text) -> Text:
    await mkdir(connector, targets, parent_dir)
    if connector is not None:
        async with aiohttp.ClientSession() as session:
            async with session.head(url, allow_redirects=True) as response:
                if response.status == 200:
                    _, params = cgi.parse_header(response.headers.get('Content-Disposition', ''))
                    filepath = posixpath.join(parent_dir, params['filename'])
        download_tasks = []
        for target in targets:
            download_tasks.append(asyncio.create_task(connector.run(
                resource=target,
                command=["if [ command -v curl ]; curl -L -o \"{path}\"; else wget -P \"{dir}\" {url}; fi".format(
                    dir=parent_dir,
                    path=filepath,
                    url=url)])))
        await asyncio.gather(*download_tasks)
    else:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    _, params = cgi.parse_header(response.headers.get('Content-Disposition', ''))
                    filepath = os.path.join(parent_dir, params['filename'])
                    content = await response.read()
                else:
                    raise Exception
        with open(filepath, mode="wb") as f:
            f.write(content)
    return filepath


@profile
async def exists(connector: Optional[Connector], target: Optional[Text], path: Text) -> bool:
    if connector is not None:
        result, status = await connector.run(
            resource=target,
            command=["test -e \"{path}\"".format(path=path)],
            capture_output=True)
        if status > 1:
            raise WorkflowExecutionException(result)
        else:
            return not status
    else:
        return os.path.exists(path)


@profile
async def follow_symlink(connector: Optional[Connector], target: Optional[Text], path: Text) -> Text:
    if connector is not None:
        result, status = await connector.run(
            resource=target,
            command=["readlink -f \"{path}\"".format(path=path)],
            capture_output=True)
        _check_status(result, status)
        return result.strip()
    else:
        return os.path.realpath(path)


@profile
async def head(connector: Optional[Connector], target: Optional[Text], path: Text, num_bytes: int) -> Text:
    if connector is not None:
        result, status = await connector.run(
            resource=target,
            command=["head", "-c", num_bytes, path],
            capture_output=True)
        _check_status(result, status)
        return result.strip()
    else:
        with open(path, "rb") as f:
            return f.read(num_bytes).decode('utf-8')


@profile
async def isdir(connector: Optional[Connector], target: Optional[Text], path: Text):
    if connector is not None:
        result, status = await connector.run(
            resource=target,
            command=["test -d \"{path}\"".format(path=path)],
            capture_output=True)
        if status > 1:
            raise WorkflowExecutionException(result)
        else:
            return not status
    else:
        return os.path.isdir(path)


@profile
async def isfile(connector: Optional[Connector], target: Optional[Text], path: Text):
    if connector is not None:
        result, status = await connector.run(
            resource=target,
            command=["test -f \"{path}\"".format(path=path)],
            capture_output=True)
        if status > 1:
            raise WorkflowExecutionException(result)
        else:
            return not status
    else:
        return os.path.isfile(path)


@profile
async def listdir(connector: Optional[Connector],
                  target: Optional[Text],
                  path: Text,
                  file_type: FileType) -> MutableSequence[Text]:
    if connector is not None:
        command = "find -L \"{path}\" -mindepth 1 -maxdepth 1 {type}".format(
            path=path,
            type="-type {type}".format(
                type="d" if file_type == FileType.DIRECTORY else "f") if file_type is not None else ""
        ).split()
        content, status = await connector.run(
            resource=target,
            command=command,
            capture_output=True)
        _check_status(content, status)
        content = content.strip(' \n')
        return content.split('\n') if content else []
    else:
        return _listdir_local(path, file_type)


@profile
async def mkdir(
        connector: Optional[Connector],
        targets: Optional[MutableSequence[Text]],
        path: Text) -> None:
    return await mkdirs(connector, targets, [path])


@profile
async def mkdirs(
        connector: Optional[Connector],
        targets: Optional[MutableSequence[Text]],
        paths: MutableSequence[Text]) -> None:
    if connector is not None:
        command = ["mkdir", "-p"]
        command.extend(paths)
        await asyncio.gather(*[asyncio.create_task(
            connector.run(resource=target, command=command)
        ) for target in targets])
    else:
        for path in paths:
            os.makedirs(path, exist_ok=True)


@profile
async def read(connector: Optional[Connector], target: Optional[Text], path: Text) -> Text:
    if connector is not None:
        result, status = await connector.run(
            resource=target,
            command=["cat", path],
            capture_output=True)
        _check_status(result, status)
        return result.strip()
    else:
        with open(path, "rb") as f:
            return f.read().decode('utf-8')


@profile
async def resolve(
        connector: Optional[Connector],
        target: Optional[Text],
        pattern: Text) -> Optional[MutableSequence[Text]]:
    if connector is not None:
        result, status = await connector.run(
            resource=target,
            command=["printf", "\"%s\\0\"", pattern, "|", "xargs", "-0", "-n1", "-I{}",
                     "sh", "-c", "\"if [ -e \\\"{}\\\" ]; then echo \\\"{}\\\"; fi\"", "|", "sort"],
            capture_output=True)
        _check_status(result, status)
        return result.split()
    else:
        return sorted(glob.glob(pattern))


@profile
async def size(
        connector: Optional[Connector],
        target: Optional[Text],
        path: Union[Text, MutableSequence[Text]]) -> int:
    if not path:
        return 0
    elif connector is not None:
        if isinstance(path, MutableSequence):
            path = ' '.join(["\"{path}\"".format(path=p) for p in path])
        else:
            path = "\"{path}\"".format(path=path)
        result, status = await connector.run(
            resource=target,
            command=[''.join([
                "find -L ", path, " -type f -exec ls -ln {} \\+ | ",
                "awk 'BEGIN {sum=0} {sum+=$5} END {print sum}'; "])],
            capture_output=True)
        _check_status(result, status)
        result = result.strip().strip("'\"")
        return int(result) if result.isdigit() else 0
    else:
        if isinstance(path, MutableSequence):
            weight = 0
            for p in path:
                weight += utils.get_size(p)
            return weight
        else:
            return utils.get_size(path)


@profile
async def symlink(connector: Optional[Connector], target: Optional[Text], src: Text, path: Text) -> None:
    if connector is not None:
        await connector.run(resource=target, command=["ln", "-snf", src, path])
    else:
        try:
            os.symlink(os.path.abspath(src), path, target_is_directory=os.path.isdir(path))
        except OSError as e:
            if not e.errno == errno.EEXIST:
                raise


@profile
async def write(connector: Optional[Connector], target: Optional[Text], path: Text, content: Text) -> None:
    if connector is not None:
        result, status = await connector.run(
            resource=target,
            command=["printf", "\"{content}\"".format(content=content)],
            stdout=path,
            capture_output=True)
        _check_status(result, status)
    else:
        with open(path, "w") as f:
            f.write(content)
