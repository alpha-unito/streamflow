from __future__ import annotations

import asyncio.subprocess
from abc import ABC
from types import TracebackType
from typing import Any, AsyncContextManager

from streamflow.core.data import StreamWrapper


class BaseStreamWrapper(StreamWrapper):
    def __init__(self, stream) -> None:
        super().__init__(stream)
        self.closed = False

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        await self.stream.close()

    async def read(self, size: int | None = None) -> bytes:
        return await self.stream.read(size)

    async def write(self, data: Any) -> None:
        return await self.stream.write(data)


class StreamReaderWrapper(StreamWrapper):
    async def close(self) -> None:
        pass

    async def read(self, size: int | None = None) -> bytes:
        return await self.stream.read(size)

    async def write(self, data: Any) -> None:
        raise NotImplementedError


class StreamWriterWrapper(StreamWrapper):
    async def close(self) -> None:
        self.stream.close()
        await self.stream.wait_closed()

    async def read(self, size: int | None = None) -> bytes:
        raise NotImplementedError

    async def write(self, data: Any) -> None:
        self.stream.write(data)
        await self.stream.drain()


class SubprocessStreamWrapperContextManager(AsyncContextManager[StreamWrapper], ABC):
    def __init__(self, coro) -> None:
        self.coro = coro
        self.proc: asyncio.subprocess.Process | None = None
        self.stream: StreamWrapper | None = None


class SubprocessStreamReaderWrapperContextManager(
    SubprocessStreamWrapperContextManager
):
    async def __aenter__(self) -> StreamReaderWrapper:
        self.proc = await self.coro
        self.stream = StreamReaderWrapper(self.proc.stdout)
        return self.stream

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.proc.wait()
        if self.stream:
            await self.stream.close()


class SubprocessStreamWriterWrapperContextManager(
    SubprocessStreamWrapperContextManager
):
    async def __aenter__(self) -> StreamWriterWrapper:
        self.proc = await self.coro
        self.stream = StreamWriterWrapper(self.proc.stdin)
        return self.stream

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self.stream:
            await self.stream.close()
        await self.proc.wait()
