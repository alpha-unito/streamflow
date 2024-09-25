from __future__ import annotations

import asyncio.subprocess
from abc import ABC
from collections.abc import Coroutine
from typing import Any

from streamflow.core.data import StreamWrapper, StreamWrapperContextManager


class BaseStreamWrapper(StreamWrapper):
    def __init__(self, stream):
        super().__init__(stream)
        self.closed = False

    async def close(self):
        if self.closed:
            return
        self.closed = True
        await self.stream.close()

    async def read(self, size: int | None = None):
        return await self.stream.read(size)

    async def write(self, data: Any):
        return await self.stream.write(data)


class StreamReaderWrapper(StreamWrapper):
    async def close(self):
        pass

    async def read(self, size: int | None = None):
        return await self.stream.read(size)

    async def write(self, data: Any):
        raise NotImplementedError


class StreamWriterWrapper(StreamWrapper):
    async def close(self):
        self.stream.write_eof()
        self.stream.close()
        await self.stream.wait_closed()

    async def read(self, size: int | None = None):
        raise NotImplementedError

    async def write(self, data: Any):
        self.stream.write(data)
        await self.stream.drain()


class SubprocessStreamWrapperContextManager(StreamWrapperContextManager, ABC):
    def __init__(self, coro: Coroutine):
        self.coro: Coroutine = coro
        self.proc: asyncio.subprocess.Process | None = None
        self.stream: StreamWrapper | None = None


class SubprocessStreamReaderWrapperContextManager(
    SubprocessStreamWrapperContextManager
):
    async def __aenter__(self):
        self.proc = await self.coro
        self.stream = StreamReaderWrapper(self.proc.stdout)
        return self.stream

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.proc.wait()
        if self.stream:
            await self.stream.close()


class SubprocessStreamWriterWrapperContextManager(
    SubprocessStreamWrapperContextManager
):
    async def __aenter__(self):
        self.proc = await self.coro
        self.stream = StreamWriterWrapper(self.proc.stdin)
        return self.stream

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.stream:
            await self.stream.close()
        await self.proc.wait()
