from __future__ import annotations

import asyncio
import base64
import glob
import hashlib
import io
import os
import posixpath
import shutil
import sys
import warnings
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, MutableMapping, MutableSequence
from email.message import Message
from pathlib import Path, PurePath, PurePosixPath
from typing import TYPE_CHECKING

import aiohttp
from aiohttp import ClientResponse

from streamflow.core.data import DataType
from streamflow.core.exception import WorkflowExecutionException
from streamflow.log_handler import logger

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


async def _size(
    context: StreamFlowContext,
    location: ExecutionLocation | None,
    path: str | MutableSequence[str],
) -> int:
    if not path:
        return 0
    elif location.local:
        if not isinstance(path, MutableSequence):
            path = [path]
        size = 0
        for p in path:
            size += await StreamFlowPath(p, context=context, location=location).size()
        return size
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
        connector = context.deployment_manager.get_connector(location.deployment)
        result, status = await connector.run(
            location=location, command=command, capture_output=True
        )
        _check_status(command, location, result, status)
        result = result.strip().strip("'\"")
        return int(result) if result.isdigit() else 0


class StreamFlowPath(PurePath, ABC):
    def __new__(
        cls, *args, context: StreamFlowContext, location: ExecutionLocation, **kwargs
    ):
        if cls is StreamFlowPath:
            cls = LocalStreamFlowPath if location.local else RemoteStreamFlowPath
        if sys.version_info < (3, 12):
            return cls._from_parts(args)
        else:
            return object.__new__(cls)

    @classmethod
    def _from_parts(cls, args):
        if sys.version_info < (3, 12):
            self = super()._from_parts(args)
            logger.info(f"Created object of type {str(type(self))} with MRO {type(self).__mro__}")
            return self
        else:
            warnings.warn(
                "streamflow.data.remotepath.StreamFlowPath._from_parts() "
                "is deprecated since Python 3.12.",
                DeprecationWarning,
                stacklevel=2,
            )
            return None

    @abstractmethod
    async def checksum(self) -> str | None: ...

    @abstractmethod
    async def exists(self, *, follow_symlinks=True) -> bool: ...

    @abstractmethod
    def glob(
        self, pattern, *, case_sensitive=None
    ) -> AsyncIterator[StreamFlowPath]: ...

    @abstractmethod
    async def is_dir(self) -> bool: ...

    @abstractmethod
    async def is_file(self) -> bool: ...

    @abstractmethod
    async def is_symlink(self) -> bool: ...

    @abstractmethod
    async def mkdir(self, mode=0o777, parents=False, exist_ok=False) -> None: ...

    @abstractmethod
    async def read_text(self, n=-1, encoding=None, errors=None) -> str: ...

    @abstractmethod
    async def resolve(self, strict=False) -> StreamFlowPath | None: ...

    @abstractmethod
    async def rmtree(self) -> None: ...

    @abstractmethod
    async def size(self) -> int: ...

    @abstractmethod
    async def symlink_to(self, target, target_is_directory=False) -> None: ...

    @abstractmethod
    def walk(
        self, top_down=True, on_error=None, follow_symlinks=False
    ) -> AsyncIterator[
        tuple[
            StreamFlowPath,
            MutableSequence[str],
            MutableSequence[str],
        ]
    ]: ...

    @abstractmethod
    async def write_text(
        self, data: str, encoding=None, errors=None, newline=None
    ) -> int: ...


class LocalStreamFlowPath(Path, StreamFlowPath):
    def __init__(
        self,
        *args,
        context: StreamFlowContext,
        location: ExecutionLocation | None = None,
    ):
        if sys.version_info < (3, 12):
            super().__init__()
        else:
            super().__init__(*args)
        self.context: StreamFlowContext = context

    async def checksum(self) -> str | None:
        if await self.is_file():
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                self.context.process_executor, _file_checksum_local, self.__str__()
            )
        else:
            return None

    async def exists(self, *, follow_symlinks=True) -> bool:
        return super().exists(follow_symlinks=follow_symlinks)

    async def glob(
        self, pattern, *, case_sensitive=None
    ) -> AsyncIterator[LocalStreamFlowPath]:
        for path in glob.glob(str(self / pattern)):
            yield LocalStreamFlowPath(path, context=self.context)

    async def is_dir(self) -> bool:
        return super().is_dir()

    async def is_file(self) -> bool:
        return super().is_file()

    async def is_symlink(self) -> bool:
        return super().is_symlink()

    async def mkdir(self, mode=0o777, parents=False, exist_ok=False) -> None:
        try:
            os.mkdir(self, mode)
        except FileNotFoundError:
            if not parents or self.parent == self:
                raise
            await self.parent.mkdir(parents=True, exist_ok=True)
            await self.mkdir(mode, parents=False, exist_ok=exist_ok)
        except OSError:
            # Cannot rely on checking for EEXIST, since the operating system
            # could give priority to other errors like EACCES or EROFS
            if not exist_ok or not await self.is_dir():
                raise

    async def read_text(self, n=-1, encoding=None, errors=None) -> str:
        encoding = io.text_encoding(encoding)
        with self.open(mode="r", encoding=encoding, errors=errors) as f:
            return f.read(n)

    async def resolve(self, strict=False) -> LocalStreamFlowPath | None:
        if await self.exists():
            return LocalStreamFlowPath(
                super().resolve(strict=strict), context=self.context
            )
        else:
            return None

    async def rmtree(self) -> None:
        if await self.exists():
            if await self.is_symlink():
                self.unlink()
            elif await self.is_dir():
                shutil.rmtree(self.__str__())
            else:
                self.unlink(missing_ok=True)

    async def size(self) -> int:
        if await self.is_file():
            return self.stat().st_size if not await self.is_symlink() else 0
        else:
            total_size = 0
            async for dirpath, _, filenames in self.walk():
                for f in filenames:
                    fp = dirpath / f
                    if not await fp.is_symlink():
                        total_size += fp.stat().st_size
            return total_size

    async def symlink_to(self, target, target_is_directory=False) -> None:
        return super().symlink_to(
            target=target, target_is_directory=target_is_directory
        )

    async def walk(
        self, top_down=True, on_error=None, follow_symlinks=False
    ) -> AsyncIterator[
        tuple[
            LocalStreamFlowPath,
            MutableSequence[str],
            MutableSequence[str],
        ]
    ]:
        for dirpath, dirnames, filenames in super().walk(
            top_down=top_down, on_error=on_error, follow_symlinks=follow_symlinks
        ):
            yield dirpath, dirnames, filenames

    def with_segments(self, *pathsegments):
        return type(self)(*pathsegments, context=self.context)

    async def write_text(
        self, data: str, encoding=None, errors=None, newline=None
    ) -> int:
        return super().write_text(
            data=data, encoding=encoding, errors=errors, newline=newline
        )


class RemoteStreamFlowPath(PurePosixPath, StreamFlowPath):
    __slots__ = ("context", "connector", "location")

    def __init__(self, *args, context: StreamFlowContext, location: ExecutionLocation):
        if sys.version_info < (3, 12):
            super().__init__()
        else:
            super().__init__(*args)
        self.context: StreamFlowContext = context
        self.connector: Connector = self.context.deployment_manager.get_connector(
            location.deployment
        )
        self.location: ExecutionLocation = location

    async def _test(self, command: list[str]) -> bool:
        command = ["test"] + command
        result, status = await self.connector.run(
            location=self.location, command=command, capture_output=True
        )
        if status > 1:
            raise WorkflowExecutionException(
                "{} Command '{}' on location {}: {}".format(
                    status, command, self.location, result
                )
            )
        else:
            return not status

    async def checksum(self):
        command = [
            "test",
            "-f",
            f"'{self.__str__()}'",
            "&&",
            "sha1sum",
            f"'{self.__str__()}'",
            "|",
            "awk",
            "'{print $1}'",
        ]
        result, status = await self.connector.run(
            location=self.location, command=command, capture_output=True
        )
        if status > 1:
            raise WorkflowExecutionException(
                "{} Command '{}' on location {}: {}".format(
                    status, command, self.location, result
                )
            )
        return result.strip()

    async def exists(self, *, follow_symlinks=True) -> bool:
        return await self._test(
            command=(
                ["-e", f"'{self.__str__()}'"]
                if follow_symlinks
                else ["-e", f"'{self.__str__()}'", "-o", "-L", f"'{self.__str__()}'"]
            )
        )

    async def glob(
        self, pattern, *, case_sensitive=None
    ) -> AsyncIterator[RemoteStreamFlowPath]:
        if not pattern:
            raise ValueError(f"Unacceptable pattern: {pattern!r}")
        command = [
            "printf",
            '"%s\\0"',
            str(self / pattern),
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
        result, status = await self.connector.run(
            location=self.location, command=command, capture_output=True
        )
        _check_status(command, self.location, result, status)
        for path in result.split():
            yield RemoteStreamFlowPath(
                path, context=self.context, location=self.location
            )

    async def is_dir(self) -> bool:
        return await self._test(command=["-d", f"'{self.__str__()}'"])

    async def is_file(self) -> bool:
        return await self._test(command=["-f", f"'{self.__str__()}'"])

    async def is_symlink(self) -> bool:
        return await self._test(command=["-L", f"'{self.__str__()}'"])

    async def mkdir(self, mode=0o777, parents=False, exist_ok=False) -> None:
        command = ["mkdir", "-m", f"{mode:o}"]
        if parents or exist_ok:
            command.append("-p")
        command.append(self.__str__())
        result, status = await self.connector.run(
            location=self.location, command=command, capture_output=True
        )
        _check_status(command, self.location, result, status)

    async def read_text(self, n=-1, encoding=None, errors=None) -> str:
        command = ["head", "-c", str(n)] if n >= 0 else ["cat"]
        command.append(self.__str__())
        result, status = await self.connector.run(
            location=self.location, command=command, capture_output=True
        )
        _check_status(command, self.location, result, status)
        return result.strip()

    async def resolve(self, strict=False) -> RemoteStreamFlowPath | None:
        # If at least one primary location is present on the site, return its path
        if locations := self.context.data_manager.get_data_locations(
            path=self.__str__(),
            deployment=self.connector.deployment_name,
            location_name=self.location.name,
            data_type=DataType.PRIMARY,
        ):
            return RemoteStreamFlowPath(
                next(iter(locations)).path,
                context=self.context,
                location=next(iter(locations)).location,
            )
        # Otherwise, analyse the remote path
        command = [
            "test",
            "-e",
            f"'{self.__str__()}'",
            "&&",
            "readlink",
            "-f",
            f"'{self.__str__()}'",
        ]
        result, status = await self.connector.run(
            location=self.location, command=command, capture_output=True
        )
        if status > 1:
            raise WorkflowExecutionException(
                "{} Command '{}' on location {}: {}".format(
                    status, command, self.location, result
                )
            )
        return (
            RemoteStreamFlowPath(
                result.strip(), context=self.context, location=self.location
            )
            if status == 0
            else None
        )

    async def rmtree(self) -> None:
        command = ["rm", "-rf ", self.__str__()]
        result, status = await self.connector.run(
            location=self.location, command=command, capture_output=True
        )
        _check_status(command, self.location, result, status)

    async def size(self) -> int:
        command = [
            "".join(
                [
                    "find -L ",
                    f'"{self.__str__()}"',
                    " -type f -exec ls -ln {} \\+ | ",
                    "awk 'BEGIN {sum=0} {sum+=$5} END {print sum}'; ",
                ]
            )
        ]
        result, status = await self.connector.run(
            location=self.location, command=command, capture_output=True
        )
        _check_status(command, self.location, result, status)
        result = result.strip().strip("'\"")
        return int(result) if result.isdigit() else 0

    async def symlink_to(self, target, target_is_directory=False) -> None:
        command = ["ln", "-snf", str(target), self.__str__()]
        result, status = await self.connector.run(
            location=self.location, command=command, capture_output=True
        )
        _check_status(command, self.location, result, status)

    async def walk(
        self, top_down=True, on_error=None, follow_symlinks=False
    ) -> AsyncIterator[
        tuple[
            LocalStreamFlowPath,
            MutableSequence[str],
            MutableSequence[str],
        ]
    ]:
        paths = [self]
        while paths:
            path = paths.pop()
            if isinstance(path, tuple):
                yield path
                continue
            command = ["find", f"'{str(path)}'", "-mindepth", "1", "-maxdepth", "1"]
            if follow_symlinks:
                command.append("-L")
            try:
                content, status = await self.connector.run(
                    location=self.location,
                    command=command + ["-type", "d"],
                    capture_output=True,
                )
                _check_status(command, self.location, content, status)
                content = content.strip(" \n")
                dirnames = (
                    [
                        str(
                            RemoteStreamFlowPath(
                                p, context=self.context, location=self.location
                            ).relative_to(path)
                        )
                        for p in content.splitlines()
                    ]
                    if content
                    else []
                )
                content, status = await self.connector.run(
                    location=self.location,
                    command=command + ["-type", "f"],
                    capture_output=True,
                )
                _check_status(command, self.location, content, status)
                content = content.strip(" \n")
                filenames = (
                    [
                        str(
                            RemoteStreamFlowPath(
                                p, context=self.context, location=self.location
                            ).relative_to(path)
                        )
                        for p in content.splitlines()
                    ]
                    if content
                    else []
                )
            except WorkflowExecutionException as error:
                if on_error is not None:
                    on_error(error)
                continue

            if top_down:
                yield path, dirnames, filenames
            else:
                paths.append((path, dirnames, filenames))
            paths += [path._make_child_relpath(d) for d in reversed(dirnames)]

    def with_segments(self, *pathsegments):
        return type(self)(*pathsegments, context=self.context, location=self.location)

    async def write_text(
        self, data: str, encoding=None, errors=None, newline=None
    ) -> int:
        if not isinstance(data, str):
            raise TypeError("data must be str, not %s" % data.__class__.__name__)
        command = [
            "echo",
            base64.b64encode(data.encode("utf-8")).decode("utf-8"),
            "|",
            "base64",
            "-d",
        ]
        result, status = await self.connector.run(
            location=self.location,
            command=command,
            stdout=self.__str__(),
            capture_output=True,
        )
        _check_status(command, self.location, result, status)
        return len(data)


async def download(
    context: StreamFlowContext,
    location: ExecutionLocation | None,
    url: str,
    parent_dir: str,
) -> StreamFlowPath:
    await StreamFlowPath(parent_dir, context=context, location=location).mkdir(
        mode=0o777, exist_ok=True
    )
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
        connector = context.deployment_manager.get_connector(location.deployment)
        await connector.run(
            location=location,
            command=[
                f'if [ command -v curl ]; then curl -L -o "{filepath}" "{url}"; '
                f'else wget -O "{filepath}" "{url}"; fi'
            ],
        )
    return StreamFlowPath(filepath, context=context, location=location)


async def get_storage_usages(
    context: StreamFlowContext, location: ExecutionLocation, hardware: Hardware
) -> MutableMapping[str, int]:
    """
    Get the actual size of the hardware storage paths

    Warn. Storage keys are not mount points.
    Since the meaning of storage dictionary keys depends on each `HardwareRequirement` implementation,
    no assumption about the key meaning should be made.

    :param context: the `StreamFlowContext` object with global application status
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
                        _size(
                            context=context,
                            location=location,
                            path=list(storage.paths),
                        )
                    )
                    for storage in hardware.storage.values()
                )
            ),
        )
    )
