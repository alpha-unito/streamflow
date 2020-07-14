from __future__ import annotations

import cgi
import glob
import os
import posixpath
from typing import TYPE_CHECKING, List

import aiohttp

from streamflow.core import utils
from streamflow.core.data import FileType

if TYPE_CHECKING:
    from streamflow.core.deployment import Connector
    from typing import Union, Optional
    from typing_extensions import Text


async def download(connector: Optional[Connector], target: Optional[Text], url: Text, parent_dir: Text) -> Text:
    await mkdir(connector, target, parent_dir)
    if connector is not None:
        async with aiohttp.ClientSession() as session:
            async with session.head(url, allow_redirects=True) as response:
                if response.status == 200:
                    _, params = cgi.parse_header(response.headers.get('Content-Disposition', ''))
                    filepath = posixpath.join(parent_dir, params['filename'])
        await connector.run(
            resource=target,
            command=["if [ command -v curl ]; curl -L -o \"{path}\"; else wget -P \"{dir}\" {url}; fi".format(
                dir=parent_dir,
                path=filepath,
                url=url
            )]
        )
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


async def exists(connector: Optional[Connector], target: Optional[Text], path: Text) -> bool:
    if connector is not None:
        result, _ = await connector.run(
            resource=target,
            command=["if [ -e \"{path}\" ]; then echo \"{path}\"; fi".format(path=path)],
            capture_output=True
        )
        return result.strip()
    else:
        return os.path.exists(path)


async def follow_symlink(connector: Optional[Connector], target: Optional[Text], path: Text) -> Text:
    if connector is not None:
        result, _ = await connector.run(
            resource=target,
            command=["readlink -f \"{path}\"".format(path=path)],
            capture_output=True
        )
        return result.strip()
    else:
        return os.path.realpath(path)


async def head(connector: Optional[Connector], target: Optional[Text], path: Text, num_bytes: int) -> Text:
    if connector is not None:
        result, _ = await connector.run(
            resource=target,
            command=["head", "-c", num_bytes, path],
            capture_output=True
        )
        return result.strip()
    else:
        with open(path, "rb") as f:
            return f.read(num_bytes).decode('utf-8')


async def isdir(connector: Optional[Connector], target: Optional[Text], path: Text):
    if connector is not None:
        result, _ = await connector.run(
            resource=target,
            command=["if [ -d \"{path}\" ]; then echo \"{path}\"; fi".format(path=path)],
            capture_output=True
        )
        return result.strip()
    else:
        return os.path.isdir(path)


async def isfile(connector: Optional[Connector], target: Optional[Text], path: Text):
    if connector is not None:
        result, _ = await connector.run(
            resource=target,
            command=["if [ -f \"{path}\" ]; then echo \"{path}\"; fi".format(path=path)],
            capture_output=True
        )
        return result.strip()
    else:
        os.path.isfile(path)


async def listdir(connector: Optional[Connector],
                  target: Optional[Text],
                  path: Text,
                  recursive: bool = False,
                  file_type: Optional[FileType] = None) -> List[Text]:
    if connector is not None:
        command = "find -L \"{path}\" -mindepth 1 {maxdepth} {type}".format(
            path=path,
            maxdepth="-maxdepth 1" if not recursive else "",
            type="-type {type}".format(
                type="d" if file_type == FileType.DIRECTORY else "f") if file_type is not None else ""
        ).split()
        content, _ = await connector.run(
            resource=target,
            command=command,
            capture_output=True
        )
        content = content.strip(' \n')
        return content.split('\n') if content else []
    else:
        content = []
        dir_content = os.listdir(path)
        for element in dir_content:
            element_path = os.path.join(path, element)
            content.append(element_path)
            if recursive and os.path.isdir(element_path):
                content.extend(await listdir(connector, target, element_path, recursive))
        return content


async def mkdir(connector: Optional[Connector], target: Optional[Text], path: Text) -> None:
    if connector is not None:
        await connector.run(resource=target, command=["mkdir", "-p", path])
    else:
        os.makedirs(path, exist_ok=True)


async def resolve(connector: Optional[Connector], target: Optional[Text], pattern: Text) -> Optional[List[Text]]:
    if connector is not None:
        result, _ = await connector.run(
            resource=target,
            command=["printf", "\"%s\\n\"", pattern, "|", "xargs", "-d", "'\\n'", "-n1", "-I{}",
                     "sh", "-c", "\"if [ -e \\\"{}\\\" ]; then echo \\\"{}\\\"; fi\""],
            capture_output=True
        )
        return result.split()
    else:
        return glob.glob(pattern)


async def size(connector: Optional[Connector], target: Optional[Text], path: Union[Text, List[Text]]) -> int:
    if not path:
        return 0
    elif connector is not None:
        if isinstance(path, List):
            path = ' '.join(["\"{path}\"".format(path=p) for p in path])
        else:
            path = "\"{path}\"".format(path=path)
        result, _ = await connector.run(
            resource=target,
            command=[''.join([
                "find -L ", path, " -type f -exec ls -ln {} \\+ | ",
                "awk 'BEGIN {sum=0} {sum+=$5} END {print sum}'; "])],
            capture_output=True
        )
        result = result.strip().strip("'\"")
        return int(result) if result.isdigit() else 0
    else:
        if isinstance(path, List):
            weight = 0
            for p in path:
                weight += utils.get_size(p)
            return weight
        else:
            return utils.get_size(path)


async def symlink(connector: Optional[Connector], target: Optional[Text], src: Text, path: Text) -> None:
    if connector is not None:
        await connector.run(resource=target, command=["ln", "-s", src, path])
    else:
        os.symlink(os.path.abspath(src), path, target_is_directory=os.path.isdir(path))
