from __future__ import annotations

import asyncio
import glob
import hashlib
import io
import os
import pathlib
import posixpath
import shutil
import sys
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, MutableMapping, MutableSequence
from email.message import Message
from pathlib import Path, PosixPath, PurePath, PurePosixPath, WindowsPath
from typing import TYPE_CHECKING, cast

import aiohttp
from aiohttp import ClientResponse
from typing_extensions import Self

from streamflow.core.data import DataType
from streamflow.core.exception import WorkflowExecutionException

if sys.version_info >= (3, 13):
    from pathlib import UnsupportedOperation
else:
    UnsupportedOperation = NotImplementedError

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.data import DataLocation
    from streamflow.core.deployment import Connector, ExecutionLocation
    from streamflow.core.scheduling import Hardware


def _check_status(
    command: MutableSequence[str], location: ExecutionLocation, result: str, status: int
) -> None:
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


def _get_filename_from_response(response: ClientResponse, url: str) -> str:
    if cd_header := response.headers.get("Content-Disposition"):
        message = Message()
        message["content-disposition"] = cd_header
        if filename := message.get_param("filename", header="content-disposition"):
            return filename
    return url.rsplit("/", 1)[-1]


def _get_outer_path(
    context: StreamFlowContext,
    location: ExecutionLocation,
    path: StreamFlowPath,
) -> StreamFlowPath:
    if (
        isinstance(path, LocalStreamFlowPath)
        and location.local
        or isinstance(path, RemoteStreamFlowPath)
        and path.location == location
    ):
        return path
    else:
        path = _get_outer_path(
            context=context,
            location=location.wraps,
            path=path,
        )
        for mnt_point, mount in sorted(
            location.mounts.items(), key=lambda item: item[1], reverse=True
        ):
            if path.is_relative_to(mount):
                return StreamFlowPath(
                    mnt_point, context=context, location=location
                ) / path.relative_to(mount)
        raise WorkflowExecutionException(
            f"Failed to find original location for path {str(path)}"
        )


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


def get_inner_path(
    path: StreamFlowPath, recursive: bool = False
) -> StreamFlowPath | None:
    if not isinstance(path, LocalStreamFlowPath) and path.location.wraps:
        path = cast(RemoteStreamFlowPath, path)
        for mount in sorted(path.location.mounts.keys(), reverse=True):
            if path.is_relative_to(mount):
                inner_path = StreamFlowPath(
                    path.location.mounts[mount],
                    context=path.context,
                    location=path.location.wraps,
                ) / path.relative_to(mount)
                return (
                    get_inner_path(inner_path, recursive) or inner_path
                    if recursive
                    else inner_path
                )
    return None


class StreamFlowPath(PurePath, ABC):
    def __new__(
        cls, *args, context: StreamFlowContext, location: ExecutionLocation, **kwargs
    ) -> Self:
        if cls is StreamFlowPath:
            cls = LocalStreamFlowPath if location.local else RemoteStreamFlowPath
        if sys.version_info < (3, 12):
            return cls._from_parts(args)
        else:
            return object.__new__(cls)

    @abstractmethod
    async def checksum(self) -> str | None: ...

    @abstractmethod
    async def exists(self) -> bool: ...

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
    async def hardlink_to(self, target) -> None: ...

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


class classinstancemethod(classmethod):
    def __get__(self, *args, **kwargs):
        return (super().__get__ if args[0] is None else self.__func__.__get__)(
            args[0], args[1]
        )


class __LegacyStreamFlowPath(StreamFlowPath, ABC):

    @classinstancemethod
    def _from_parts(self, args) -> StreamFlowPath:
        obj = (
            object.__new__(self)
            if isinstance(self, type)
            else type(self)(
                context=getattr(self, "context", None),
                location=getattr(self, "location", None),
            )
        )
        drv, root, parts = obj._parse_args(args)
        obj._drv = drv
        obj._root = root
        obj._parts = parts
        return obj

    @classinstancemethod
    def _from_parsed_parts(
        self,
        drv,
        root,
        parts,
    ):
        obj = (
            object.__new__(self)
            if isinstance(self, type)
            else type(self)(
                context=getattr(self, "context", None),
                location=getattr(self, "location", None),
            )
        )
        obj._drv = drv
        obj._root = root
        obj._parts = parts
        return obj

    def _scandir(self):
        return os.scandir(self)

    def walk(self, top_down=True, on_error=None, follow_symlinks=False):
        paths = [self]
        while paths:
            path = paths.pop()
            if isinstance(path, tuple):
                yield path
                continue
            try:
                scandir_it = path._scandir()
            except OSError as error:
                if on_error is not None:
                    on_error(error)
                continue

            with scandir_it:
                dirnames = []
                filenames = []
                for entry in scandir_it:
                    try:
                        is_dir = entry.is_dir(follow_symlinks=follow_symlinks)
                    except OSError:
                        is_dir = False
                    if is_dir:
                        dirnames.append(entry.name)
                    else:
                        filenames.append(entry.name)
            if top_down:
                yield path, dirnames, filenames
            else:
                paths.append((path, dirnames, filenames))
            paths += [path._make_child_relpath(d) for d in reversed(dirnames)]


class LocalStreamFlowPath(
    WindowsPath if os.name == "nt" else PosixPath,
    __LegacyStreamFlowPath if sys.version_info < (3, 12) else StreamFlowPath,
):
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

    async def exists(self) -> bool:
        return cast(Path, super()).exists()

    async def glob(
        self, pattern, *, case_sensitive=None
    ) -> AsyncIterator[LocalStreamFlowPath]:
        for path in glob.glob(str(self / pattern)):
            yield self.with_segments(path)

    async def is_dir(self) -> bool:
        return cast(Path, super()).is_dir()

    async def is_file(self) -> bool:
        return cast(Path, super()).is_file()

    async def is_symlink(self) -> bool:
        return cast(Path, super()).is_symlink()

    async def mkdir(self, mode=0o777, parents=False, exist_ok=False) -> None:
        try:
            os.mkdir(self, mode)
        except FileNotFoundError:
            if not parents or self.parent == self:
                raise
            await self.parent.mkdir(parents=True, exist_ok=True)
            await self.mkdir(mode, parents=False, exist_ok=exist_ok)
        except OSError:
            if not exist_ok or not await self.is_dir():
                raise

    @property
    def parents(self):
        if sys.version_info < (3, 12):

            class __PathParents(pathlib._PathParents):
                def __init__(self, path):
                    super().__init__(path)
                    self._path = path

                def __getitem__(self, idx):
                    if idx < 0 or idx >= len(self):
                        raise IndexError(idx)
                    return self._path._from_parsed_parts(
                        self._drv, self._root, self._parts[: -idx - 1]
                    )

            return __PathParents(self)
        else:
            return super().parents

    async def read_text(self, n=-1, encoding=None, errors=None) -> str:
        encoding = io.text_encoding(encoding)
        with self.open(mode="r", encoding=encoding, errors=errors) as f:
            return f.read(n)

    async def resolve(self, strict=False) -> LocalStreamFlowPath | None:
        if await self.exists():
            return self.with_segments(super().resolve(strict=strict))
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
        return cast(Path, super()).symlink_to(
            target=target, target_is_directory=target_is_directory
        )

    async def hardlink_to(self, target) -> None:
        try:
            return cast(Path, super()).hardlink_to(target=target)
        except UnsupportedOperation as err:
            raise WorkflowExecutionException(err)

    async def walk(
        self, top_down=True, on_error=None, follow_symlinks=False
    ) -> AsyncIterator[
        tuple[
            LocalStreamFlowPath,
            MutableSequence[str],
            MutableSequence[str],
        ]
    ]:
        for dirpath, dirnames, filenames in cast(Path, super()).walk(
            top_down=top_down, on_error=on_error, follow_symlinks=follow_symlinks
        ):
            yield dirpath, dirnames, filenames

    def with_segments(self, *pathsegments) -> Self:
        return type(self)(*pathsegments, context=self.context)

    async def write_text(self, data: str, **kwargs) -> int:
        return cast(Path, super()).write_text(data=data, **kwargs)


class RemoteStreamFlowPath(
    PurePosixPath,
    __LegacyStreamFlowPath if sys.version_info < (3, 12) else StreamFlowPath,
):
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
        self._inner_path: StreamFlowPath | None = None

    def _is_valid_inner_path(self, location: DataLocation) -> bool:
        return (
            # The data is valid in the location
            (
                self.connector.deployment_name == location.deployment
                and self.location == location.location
            )
            # The data is valid in the inner location
            or (
                isinstance(self._inner_path, RemoteStreamFlowPath)
                and self._inner_path.connector.deployment_name == location.deployment
                and self._inner_path.location == location.location
            )
            or (
                isinstance(self._inner_path, LocalStreamFlowPath)
                and location.location.local
            )
        )

    async def _get_inner_path(self) -> StreamFlowPath:
        if self._inner_path is None:
            # Recurse through mount points to find the innermost path (more efficient)
            self._inner_path = get_inner_path(path=self, recursive=True) or self
            if self._inner_path and (
                isinstance(self._inner_path, LocalStreamFlowPath)
                or (
                    isinstance(self._inner_path, RemoteStreamFlowPath)
                    and self._inner_path.location != self.location
                )
            ):
                path = self._inner_path
                while path != path.parent:
                    if locations := self.context.data_manager.get_data_locations(
                        path=path.__str__(),
                        data_type=DataType.PRIMARY,
                    ):
                        for loc in locations:
                            if self._is_valid_inner_path(loc):
                                await loc.available.wait()
                                # The inner location has access to the file
                                if get_inner_path(
                                    StreamFlowPath(
                                        loc.path,
                                        context=self.context,
                                        location=loc.location,
                                    )
                                ):
                                    break
                        else:
                            if real_path := await self.resolve():
                                self._inner_path = (
                                    get_inner_path(
                                        path=real_path,
                                        recursive=True,
                                    )
                                    or self
                                )
                            else:
                                self._inner_path = self
                        return self._inner_path
                    else:
                        path = path.parent
        return self._inner_path

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

    async def checksum(self) -> str | None:
        if (inner_path := await self._get_inner_path()) != self:
            return await inner_path.checksum()
        else:
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

    async def exists(self) -> bool:
        if (inner_path := await self._get_inner_path()) != self:
            return await inner_path.exists()
        else:
            return await self._test(command=(["-e", f"'{self.__str__()}'"]))

    async def glob(
        self, pattern, *, case_sensitive=None
    ) -> AsyncIterator[RemoteStreamFlowPath]:
        if (inner_path := await self._get_inner_path()) != self:
            async for path in inner_path.glob(pattern, case_sensitive=case_sensitive):
                yield _get_outer_path(
                    context=self.context,
                    location=self.location,
                    path=path,
                )
        else:
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
                yield self.with_segments(path)

    async def is_dir(self) -> bool:
        if (inner_path := await self._get_inner_path()) != self:
            return await inner_path.is_dir()
        else:
            return await self._test(command=["-d", f"'{self.__str__()}'"])

    async def is_file(self) -> bool:
        if (inner_path := await self._get_inner_path()) != self:
            return await inner_path.is_file()
        else:
            return await self._test(command=["-f", f"'{self.__str__()}'"])

    async def is_symlink(self) -> bool:
        return await self._test(command=["-L", f"'{self.__str__()}'"])

    async def mkdir(self, mode=0o777, parents=False, exist_ok=False) -> None:
        if (inner_path := await self._get_inner_path()) != self:
            await inner_path.mkdir(mode=mode, parents=parents, exist_ok=exist_ok)
        else:
            command = ["mkdir", "-m", f"{mode:o}"]
            if parents or exist_ok:
                command.append("-p")
            command.append(self.__str__())
            result, status = await self.connector.run(
                location=self.location, command=command, capture_output=True
            )
            _check_status(command, self.location, result, status)

    async def read_text(self, n=-1, encoding=None, errors=None) -> str:
        if (inner_path := await self._get_inner_path()) != self:
            return await inner_path.read_text(n=n, encoding=encoding, errors=errors)
        else:
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
        ):
            for loc in locations:
                await loc.available.wait()
                if loc.data_type == DataType.PRIMARY:
                    return self.with_segments(loc.path)
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
        return self.with_segments(result.strip()) if status == 0 else None

    async def rmtree(self) -> None:
        if (inner_path := await self._get_inner_path()) != self:
            await inner_path.rmtree()
        else:
            command = ["rm", "-rf ", self.__str__()]
            result, status = await self.connector.run(
                location=self.location, command=command, capture_output=True
            )
            _check_status(command, self.location, result, status)

    async def size(self) -> int:
        if (inner_path := await self._get_inner_path()) != self:
            return await inner_path.size()
        else:
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
        if (inner_path := await self._get_inner_path()) != self:
            await inner_path.symlink_to(target, target_is_directory=target_is_directory)
        else:
            command = ["ln", "-snf", str(target), self.__str__()]
            result, status = await self.connector.run(
                location=self.location, command=command, capture_output=True
            )
            _check_status(command, self.location, result, status)

    async def hardlink_to(self, target) -> None:
        if (inner_path := await self._get_inner_path()) != self:
            await inner_path.hardlink_to(target)
        else:
            command = ["ln", "-nf", str(target), self.__str__()]
            result, status = await self.connector.run(
                location=self.location, command=command, capture_output=True
            )
            _check_status(command, self.location, result, status)

    async def walk(
        self, top_down=True, on_error=None, follow_symlinks=False
    ) -> AsyncIterator[
        tuple[
            RemoteStreamFlowPath,
            MutableSequence[str],
            MutableSequence[str],
        ]
    ]:
        if (inner_path := await self._get_inner_path()) != self:
            async for path, dirnames, filenames in inner_path.walk(
                top_down=top_down, on_error=on_error, follow_symlinks=follow_symlinks
            ):
                yield _get_outer_path(
                    context=self.context, location=self.location, path=path
                ), dirnames, filenames
        else:
            paths = [self]
            while paths:
                path = paths.pop()
                if isinstance(path, tuple):
                    yield path
                    continue
                command = ["find"]
                if follow_symlinks:
                    command.append("-L")
                command.extend([f"'{str(path)}'", "-mindepth", "1", "-maxdepth", "1"])
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
                            str(self.with_segments(p).relative_to(path))
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
                            str(self.with_segments(p).relative_to(path))
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

    def with_segments(self, *pathsegments) -> Self:
        return type(self)(*pathsegments, context=self.context, location=self.location)

    async def write_text(self, data: str, **kwargs) -> int:
        if (inner_path := await self._get_inner_path()) != self:
            return await inner_path.write_text(data, **kwargs)
        else:
            if not isinstance(data, str):
                raise TypeError("data must be str, not %s" % data.__class__.__name__)
            async with await self.connector.get_stream_writer(
                command=["tee", str(self), ">", "/dev/null"], location=self.location
            ) as writer:
                reader = io.BytesIO(data.encode("utf-8"))
                while content := reader.read(self.connector.transferBufferSize):
                    await writer.write(content)
                reader.close()
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
            strict=True,
        )
    )
