from __future__ import annotations

import asyncio
import codecs
import logging
import shlex
from abc import ABC, abstractmethod
from collections.abc import MutableMapping, MutableSequence

from streamflow.core.data import StreamWrapper
from streamflow.core.deployment import Shell
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.utils import random_name
from streamflow.log_handler import logger


def _build_shell_command(
    end_marker: str,
    command: MutableSequence[str],
    shell_class: str,
    shell_cmd: MutableSequence[str],
    environment: MutableMapping[str, str] | None = None,
    workdir: str | None = None,
) -> str:
    if environment or workdir:
        subshell_parts = []
        if workdir:
            subshell_parts.append(f"cd {shlex.quote(workdir)}")
        if environment:
            for key, value in environment.items():
                subshell_parts.append(f"export {key}={shlex.quote(value)}")
        subshell_parts.append(" ".join(command))
        cmd = f"sh -c {shlex.quote('; '.join(subshell_parts))} 2>&1"
    else:
        cmd = f"{' '.join(command)} 2>&1"
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            f"EXECUTING command {cmd} on {shell_class} "
            f"with command `{' '.join(shell_cmd)}`"
        )
    return f'{cmd}\necho "{end_marker}:$?"\n'


class BaseShell(Shell, ABC):
    __slots__ = ("_closed", "_closing", "_lock", "_reader", "_writer", "_decoder")

    def __init__(
        self,
        command: MutableSequence[str],
        buffer_size: int,
    ):
        super().__init__(command=command, buffer_size=buffer_size)
        self._closed: bool = False
        self._closing: asyncio.Event | None = None
        self._lock: asyncio.Lock = asyncio.Lock()
        self._reader: StreamWrapper | None = None
        self._writer: StreamWrapper | None = None
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")

    @abstractmethod
    async def _close(self) -> None: ...

    async def _read_with_output(
        self, end_marker: str, timeout: int | None
    ) -> tuple[str, int]:
        output = ""
        while True:
            try:
                chunk = await asyncio.wait_for(
                    self._reader.read(self.buffer_size), timeout=timeout
                )
            except asyncio.TimeoutError:
                raise WorkflowExecutionException("Timeout waiting for command output")
            if not chunk:
                raise WorkflowExecutionException(
                    "Shell process terminated unexpectedly"
                )
            decoded_chunk = self._decoder.decode(chunk, final=False)
            output += decoded_chunk
            if (marker_pos := output.find(f"{end_marker}:")) != -1 and (
                newline_pos := output.find("\n", marker_pos)
            ) != -1:
                returncode_str = output[marker_pos + len(end_marker) + 1 : newline_pos]
                try:
                    returncode = int(returncode_str)
                except ValueError:
                    raise WorkflowExecutionException(
                        f"Invalid return code: {returncode_str}"
                    )
                final_output = output[:marker_pos].strip()
                self._decoder.reset()
                return final_output, returncode

    async def _read_without_output(self, end_marker: str, timeout: int | None) -> None:
        output = ""
        while True:
            try:
                chunk = await asyncio.wait_for(
                    self._reader.read(self.buffer_size), timeout=timeout
                )
            except asyncio.TimeoutError:
                raise WorkflowExecutionException("Timeout discarding output")
            if not chunk:
                raise WorkflowExecutionException(
                    "Shell process terminated unexpectedly"
                )
            output += self._decoder.decode(chunk, final=False)
            if (marker_pos := output.find(f"{end_marker}:")) != -1 and output.find(
                "\n", marker_pos
            ) != -1:
                self._decoder.reset()
                return None

    async def close(self) -> None:
        if self._closed:
            return
        if self._closing is not None:
            await self._closing.wait()
        else:
            self._closing = asyncio.Event()
            await self._close()
            self._closed = True
            self._closing.set()

    async def closed(self) -> bool:
        if self._closing is not None:
            await self._closing.wait()
        return self._closed

    async def execute(
        self,
        command: MutableSequence[str],
        environment: MutableMapping[str, str] | None = None,
        workdir: str | None = None,
        capture_output: bool = False,
        timeout: int | None = None,
    ) -> tuple[str, int] | None:
        async with self._lock:
            if self._closed:
                raise WorkflowExecutionException("Shell process is terminated")
            end_marker = f"SF_CMD_END_{random_name()}"
            shell_command = _build_shell_command(
                end_marker=end_marker,
                command=command,
                shell_class=self.__class__.__name__,
                shell_cmd=self.command,
                environment=environment,
                workdir=workdir,
            )
            try:
                await self._writer.write(shell_command.encode())
                if capture_output:
                    return await self._read_with_output(end_marker, timeout)
                else:
                    return await self._read_without_output(end_marker, timeout)
            except (BrokenPipeError, ConnectionResetError) as e:
                await self.close()
                raise WorkflowExecutionException(f"Shell pipe broken: {e}") from e
            except asyncio.TimeoutError as e:
                raise WorkflowExecutionException(
                    f"Command timeout after {timeout}s"
                ) from e
