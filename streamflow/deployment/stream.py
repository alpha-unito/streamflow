from __future__ import annotations

import asyncio
from typing import Any

from streamflow.core.data import StreamWrapper


class BaseStreamWrapper(StreamWrapper):
    def __init__(self, stream) -> None:
        super().__init__(stream)
        self.closed = False
        self._closing: asyncio.Event | None = None

    async def _close(self) -> None:
        await self.stream.close()

    async def close(self) -> None:
        if self.closed:
            return
        if self._closing is not None:
            await self._closing.wait()
        else:
            self._closing = asyncio.Event()
            await self._close()
            self.closed = True
            self._closing.set()

    async def read(self, size: int | None = None) -> bytes:
        return await self.stream.read(size)

    async def write(self, data: Any) -> None:
        await self.stream.write(data)
