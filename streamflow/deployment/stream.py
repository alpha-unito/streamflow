from __future__ import annotations

from typing import Any

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
