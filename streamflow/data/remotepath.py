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
from streamflow.core.context import StreamFlowContext
from streamflow.core.data import FileType
from streamflow.core.exception import WorkflowExecutionException
from streamflow.deployment.connector.local import LocalConnector
from streamflow.log_handler import profile

if TYPE_CHECKING:
    from streamflow.core.deployment import Connector
    from typing import Union, Optional


def _check_status(result: str, status: int):
    if status != 0:
        raise WorkflowExecutionException(result)


async def _file_checksum(context: StreamFlowContext,
                         connector: Optional[Connector],
                         resource: Optional[str],
                         path: str) -> str:
    if isinstance(connector, LocalConnector):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            context.process_executor,
            _file_checksum_local,
            path)
    else:
        result, status = await connector.run(
            resource=resource,
            command=["sha1sum {path} | awk '{{print $1}}'".format(path=path)],
            capture_output=True)
        _check_status(result, status)
        return result.strip()


def _file_checksum_local(path: str) -> str:
    with open(path, "rb") as f:
        sha1_checksum = sha1()
        while data := f.read(2 ** 16):
            sha1_checksum.update(data)
        return sha1_checksum.hexdigest()


def _listdir_local(path: str, file_type: FileType) -> MutableSequence[str]:
    content = []
    dir_content = os.listdir(path)
    check = os.path.isfile if file_type == FileType.FILE else os.path.isdir
    for element in dir_content:
        element_path = os.path.join(path, element)
        if check(element_path):
            content.append(element_path)
    return content


@profile
async def checksum(context: StreamFlowContext,
                   connector: Optional[Connector],
                   resource: Optional[str],
                   path: str) -> str:
    if await isfile(connector, resource, path):
        return await _file_checksum(context, connector, resource, path)
    else:
        raise Exception("Checksum for folders is not implemented yet")


@profile
async def download(
        connector: Optional[Connector],
        resources: Optional[MutableSequence[str]],
        url: str, parent_dir: str) -> str:
    await mkdir(connector, resources, parent_dir)
    if isinstance(connector, LocalConnector):
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
    else:
        async with aiohttp.ClientSession() as session:
            async with session.head(url, allow_redirects=True) as response:
                if response.status == 200:
                    _, params = cgi.parse_header(response.headers.get('Content-Disposition', ''))
                    filepath = posixpath.join(parent_dir, params['filename'])
        download_tasks = []
        for resource in resources:
            download_tasks.append(asyncio.create_task(connector.run(
                resource=resource,
                command=["if [ command -v curl ]; curl -L -o \"{path}\"; else wget -P \"{dir}\" {url}; fi".format(
                    dir=parent_dir,
                    path=filepath,
                    url=url)])))
        await asyncio.gather(*download_tasks)
    return filepath


@profile
async def exists(connector: Optional[Connector], resource: Optional[str], path: str) -> bool:
    if isinstance(connector, LocalConnector):
        return os.path.exists(path)
    else:
        result, status = await connector.run(
            resource=resource,
            command=["test -e \"{path}\"".format(path=path)],
            capture_output=True)
        if status > 1:
            raise WorkflowExecutionException(result)
        else:
            return not status


@profile
async def follow_symlink(connector: Optional[Connector], resource: Optional[str], path: str) -> str:
    if isinstance(connector, LocalConnector):
        return os.path.realpath(path)
    else:
        result, status = await connector.run(
            resource=resource,
            command=["readlink -f \"{path}\"".format(path=path)],
            capture_output=True)
        _check_status(result, status)
        return result.strip()


@profile
async def head(connector: Optional[Connector], resource: Optional[str], path: str, num_bytes: int) -> str:
    if isinstance(connector, LocalConnector):
        with open(path, "rb") as f:
            return f.read(num_bytes).decode('utf-8')
    else:
        result, status = await connector.run(
            resource=resource,
            command=["head", "-c", str(num_bytes), path],
            capture_output=True)
        _check_status(result, status)
        return result.strip()


@profile
async def isdir(connector: Optional[Connector], resource: Optional[str], path: str):
    if isinstance(connector, LocalConnector):
        return os.path.isdir(path)
    else:
        result, status = await connector.run(
            resource=resource,
            command=["test -d \"{path}\"".format(path=path)],
            capture_output=True)
        if status > 1:
            raise WorkflowExecutionException(result)
        else:
            return not status


@profile
async def isfile(connector: Optional[Connector], resource: Optional[str], path: str):
    if isinstance(connector, LocalConnector):
        return os.path.isfile(path)
    else:
        result, status = await connector.run(
            resource=resource,
            command=["test -f \"{path}\"".format(path=path)],
            capture_output=True)
        if status > 1:
            raise WorkflowExecutionException(result)
        else:
            return not status


@profile
async def listdir(connector: Optional[Connector],
                  resource: Optional[str],
                  path: str,
                  file_type: FileType) -> MutableSequence[str]:
    if isinstance(connector, LocalConnector):
        return _listdir_local(path, file_type)
    else:
        command = "find -L \"{path}\" -mindepth 1 -maxdepth 1 {type}".format(
            path=path,
            type="-type {type}".format(
                type="d" if file_type == FileType.DIRECTORY else "f") if file_type is not None else ""
        ).split()
        content, status = await connector.run(
            resource=resource,
            command=command,
            capture_output=True)
        _check_status(content, status)
        content = content.strip(' \n')
        return content.split('\n') if content else []


@profile
async def mkdir(
        connector: Optional[Connector],
        resources: Optional[MutableSequence[str]],
        path: str) -> None:
    return await mkdirs(connector, resources, [path])


@profile
async def mkdirs(
        connector: Optional[Connector],
        resources: Optional[MutableSequence[str]],
        paths: MutableSequence[str]) -> None:
    if isinstance(connector, LocalConnector):
        for path in paths:
            os.makedirs(path, exist_ok=True)
    else:
        command = ["mkdir", "-p"]
        command.extend(paths)
        await asyncio.gather(*(asyncio.create_task(
            connector.run(resource=resource, command=command)
        ) for resource in resources))


@profile
async def read(connector: Optional[Connector], resource: Optional[str], path: str) -> str:
    if isinstance(connector, LocalConnector):
        with open(path, "rb") as f:
            return f.read().decode('utf-8')
    else:
        result, status = await connector.run(
            resource=resource,
            command=["cat", path],
            capture_output=True)
        _check_status(result, status)
        return result.strip()


@profile
async def resolve(
        connector: Optional[Connector],
        resource: Optional[str],
        pattern: str) -> Optional[MutableSequence[str]]:
    if isinstance(connector, LocalConnector):
        return sorted(glob.glob(pattern))
    else:
        result, status = await connector.run(
            resource=resource,
            command=["printf", "\"%s\\0\"", pattern, "|", "xargs", "-0", "-n1", "-I{}",
                     "sh", "-c", "\"if [ -e \\\"{}\\\" ]; then echo \\\"{}\\\"; fi\"", "|", "sort"],
            capture_output=True)
        _check_status(result, status)
        return result.split()


@profile
async def rm(
        connector: Optional[Connector],
        resource: Optional[str],
        path: Union[str, MutableSequence[str]]) -> None:
    if isinstance(connector, LocalConnector):
        if isinstance(path, MutableSequence):
            for p in path:
                if os.path.exists(p):
                    os.remove(p)
        else:
            if os.path.exists(path):
                return os.remove(path)
    else:
        if isinstance(path, MutableSequence):
            path = ' '.join(["\"{path}\"".format(path=p) for p in path])
        else:
            path = "\"{path}\"".format(path=path)
        await connector.run(
            resource=resource,
            command=[''.join(["rm -rf ", path])])


@profile
async def size(
        connector: Optional[Connector],
        resource: Optional[str],
        path: Union[str, MutableSequence[str]]) -> int:
    if not path:
        return 0
    elif isinstance(connector, LocalConnector):
        if isinstance(path, MutableSequence):
            weight = 0
            for p in path:
                weight += utils.get_size(p)
            return weight
        else:
            return utils.get_size(path)
    else:
        if isinstance(path, MutableSequence):
            path = ' '.join(["\"{path}\"".format(path=p) for p in path])
        else:
            path = "\"{path}\"".format(path=path)
        result, status = await connector.run(
            resource=resource,
            command=[''.join([
                "find -L ", path, " -type f -exec ls -ln {} \\+ | ",
                "awk 'BEGIN {sum=0} {sum+=$5} END {print sum}'; "])],
            capture_output=True)
        _check_status(result, status)
        result = result.strip().strip("'\"")
        return int(result) if result.isdigit() else 0


@profile
async def symlink(connector: Optional[Connector], resource: Optional[str], src: str, path: str) -> None:
    if isinstance(connector, LocalConnector):
        try:
            os.symlink(os.path.abspath(src), path, target_is_directory=os.path.isdir(path))
        except OSError as e:
            if not e.errno == errno.EEXIST:
                raise
    else:
        await connector.run(resource=resource, command=["ln", "-snf", src, path])


@profile
async def write(connector: Optional[Connector], resource: Optional[str], path: str, content: str) -> None:
    if isinstance(connector, LocalConnector):
        with open(path, "w") as f:
            f.write(content)
    else:
        result, status = await connector.run(
            resource=resource,
            command=["printf", "\"{content}\"".format(content=content)],
            stdout=path,
            capture_output=True)
        _check_status(result, status)
