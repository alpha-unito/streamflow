from __future__ import annotations

import asyncio
import base64
import datetime
import importlib
import itertools
import os
import posixpath
import shlex
import uuid
from collections.abc import Iterable, MutableMapping, MutableSequence
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any

from streamflow.core.exception import ProcessorTypeError, WorkflowExecutionException
from streamflow.core.persistence import PersistableEntity

if TYPE_CHECKING:
    from typing import TypeVar

    from streamflow.core.deployment import Connector, ExecutionLocation
    from streamflow.core.workflow import Token

    T = TypeVar("T")


class NamesStack:
    def __init__(self) -> None:
        self.stack: MutableSequence[set[str]] = [set()]

    def add_scope(self) -> None:
        self.stack.append(set())

    def add_name(self, name: str) -> None:
        self.stack[-1].add(name)

    def delete_scope(self) -> None:
        self.stack.pop()

    def delete_name(self, name: str) -> None:
        self.stack[-1].remove(name)

    def global_names(self) -> set[str]:
        names = self.stack[0].copy()
        if len(self.stack) > 1:
            for scope in self.stack[1:]:
                names = names.difference(scope)
        return names

    def __contains__(self, name: str) -> bool:
        for scope in self.stack:
            if name in scope:
                return True
        return False


def compare_tags(tag1: str, tag2: str) -> int:
    list1 = tag1.split(".")
    list2 = tag2.split(".")
    if (res := (len(list1) - len(list2))) != 0:
        return res
    for elem1, elem2 in zip(list1, list2, strict=True):
        if (res := (int(elem1) - int(elem2))) != 0:
            return res
    return 0


def contains_persistent_id(id_: int, entities: Iterable[PersistableEntity]) -> bool:
    return any(id_ == entity.persistent_id for entity in entities)


def create_command(
    class_name: str,
    command: MutableSequence[str],
    environment: MutableMapping[str, str] | None = None,
    workdir: str | None = None,
    stdin: int | str | None = None,
    stdout: int | str = asyncio.subprocess.STDOUT,
    stderr: int | str = asyncio.subprocess.STDOUT,
) -> str:
    # Format stdin
    stdin = (
        f" < {shlex.quote(str(stdin))}"
        if stdin is not None and stdin != asyncio.subprocess.DEVNULL
        else ""
    )
    # Format stderr
    if stderr == asyncio.subprocess.DEVNULL:
        stderr = "/dev/null"
    if stderr == stdout:
        stderr = " 2>&1"
    elif stderr != asyncio.subprocess.STDOUT:
        stderr = f" 2>{shlex.quote(str(stderr))}"
    else:
        stderr = ""
    # Format stdout
    if stdout == asyncio.subprocess.PIPE:
        raise WorkflowExecutionException(
            f"The `{class_name}` does not support `stdout` pipe redirection."
        )
    elif stdout == asyncio.subprocess.DEVNULL:
        stdout = "/dev/null"
    elif stdout != asyncio.subprocess.STDOUT:
        stdout = f" > {shlex.quote(str(stdout))}"
    else:
        stdout = ""
    if stderr == asyncio.subprocess.PIPE:
        raise WorkflowExecutionException(
            f"The `{class_name}` does not support `stderr` pipe redirection."
        )
    # Build command
    return "".join(
        "{workdir}" "{environment}" "{command}" "{stdin}" "{stdout}" "{stderr}"
    ).format(
        workdir=f"cd {workdir} && " if workdir is not None else "",
        environment=(
            "".join(
                [f'export {key}="{value}" && ' for (key, value) in environment.items()]
            )
            if environment is not None
            else ""
        ),
        command=" ".join(command),
        stdin=stdin,
        stdout=stdout,
        stderr=stderr,
    )


def get_job_step_name(job_name: str) -> str:
    return PurePosixPath(job_name).parent.as_posix()


def get_job_tag(job_name: str) -> str:
    return PurePosixPath(job_name).name


def dict_product(**kwargs) -> MutableMapping[Any, Any]:
    keys = kwargs.keys()
    vals = kwargs.values()
    for instance in itertools.product(*vals):
        yield dict(zip(keys, list(instance), strict=True))


def encode_command(command: str, shell: str = "sh") -> str:
    return f"echo {base64.b64encode(command.encode('utf-8')).decode('utf-8')} | base64 -d | {shell}"


async def eval_processors(unfinished: Iterable[asyncio.Task], name: str) -> Token:
    error_msg = ""
    while unfinished:
        finished, unfinished = await asyncio.wait(
            unfinished, return_when=asyncio.FIRST_COMPLETED
        )
        selected_task = None
        for task in finished:
            if isinstance(task.exception(), ProcessorTypeError):
                error_msg += (
                    f"\nTried {task.get_name()}, but received error {task.exception()}"
                )
            else:
                for remaining in unfinished:
                    remaining.cancel()
                selected_task = task
        if selected_task is not None:
            if selected_task.exception() is not None:
                raise selected_task.exception()
            else:
                return selected_task.result()
    raise ProcessorTypeError(f"No suitable token processors in {name}:{error_msg}")


def flatten_list(hierarchical_list):
    if not hierarchical_list:
        return hierarchical_list
    flat_list = []
    for el in hierarchical_list:
        if isinstance(el, MutableSequence):
            flat_list.extend(flatten_list(el))
        else:
            flat_list.append(el)
    return flat_list


def format_seconds_to_hhmmss(seconds: int) -> str:
    hours = seconds // (60 * 60)
    seconds %= 60 * 60
    minutes = seconds // 60
    seconds %= 60
    return "%02i:%02i:%02i" % (hours, minutes, seconds)


def get_class_fullname(cls: type):
    return cls.__module__ + "." + cls.__qualname__


def get_class_from_name(name: str) -> type:
    module_name, class_name = name.rsplit(".", 1)
    return getattr(importlib.import_module(module_name), class_name)


def get_date_from_ns(timestamp: int) -> str:
    base = datetime.datetime(1970, 1, 1)
    delta = datetime.timedelta(microseconds=round(timestamp / 1000))
    return (base + delta).replace(tzinfo=datetime.timezone.utc).isoformat()


def get_entity_ids(
    persistable_entities: Iterable[PersistableEntity] | None,
) -> MutableSequence[int]:
    return [pe.persistent_id for pe in (persistable_entities or []) if pe.persistent_id]


async def get_local_to_remote_destination(
    dst_connector: Connector, dst_location: ExecutionLocation, src: str, dst: str
):
    is_dst_dir, status = await dst_connector.run(
        location=dst_location,
        command=[f'test -d "{dst}"'],
        capture_output=True,
    )
    if status > 1:
        raise WorkflowExecutionException(is_dst_dir)
    # If destination path exists and is a directory
    elif status == 0:
        # Append src basename to dst
        return posixpath.join(dst, os.path.basename(src))
    # Otherwise
    else:
        # Keep current dst
        return dst


def get_option(
    name: str,
    value: Any,
) -> str:
    if len(name) > 1:
        name = f"-{name} "
    if isinstance(value, bool):
        return f"-{name} " if value else ""
    elif isinstance(value, str) or isinstance(value, int):
        return f'-{name} "{value}" '
    elif isinstance(value, MutableSequence):
        return "".join([f'-{name} "{item}" ' for item in value])
    elif value is None:
        return ""
    else:
        raise TypeError("Unsupported value type")


async def get_remote_to_remote_write_command(
    src_connector: Connector,
    src_location: ExecutionLocation,
    src: str,
    dst_connector: Connector,
    dst_locations: MutableSequence[ExecutionLocation],
    dst: str,
) -> MutableSequence[str]:
    is_dst_dir, status = await dst_connector.run(
        location=dst_locations[0],
        command=[f'test -d "{dst}"'],
        capture_output=True,
    )
    if status > 1:
        raise WorkflowExecutionException(is_dst_dir)
    # If destination path exists and is a directory
    elif status == 0:
        return ["tar", "xf", "-", "-C", dst]
    # Otherwise, if destination path does not exist
    else:
        # If basename must be renamed during transfer
        if posixpath.basename(src) != posixpath.basename(dst):
            is_src_dir, status = await src_connector.run(
                location=src_location,
                command=[f'test -d "{src}"'],
                capture_output=True,
            )
            if status > 1:
                raise WorkflowExecutionException(is_src_dir)
            # If source path is a directory
            elif status == 0:
                await asyncio.gather(
                    *(
                        asyncio.create_task(
                            dst_connector.run(
                                location=dst_location, command=["mkdir", "-p", dst]
                            )
                        )
                        for dst_location in dst_locations
                    )
                )
                return ["tar", "xf", "-", "-C", dst, "--strip-components", "1"]
            # Otherwise, if source path is a file
            else:
                return ["tar", "xf", "-", "-O", "|", "tee", dst, ">", "/dev/null"]
        # Otherwise, if basename must be preserved
        else:
            return ["tar", "xf", "-", "-C", posixpath.dirname(dst)]


def get_tag(tokens: Iterable[Token]) -> str:
    output_tag = "0"
    for tag in [t.tag for t in tokens]:
        if len(tag) > len(output_tag):
            output_tag = tag
    return output_tag


def make_future(obj: T) -> asyncio.Future[T]:
    future = asyncio.Future()
    future.set_result(obj)
    return future


def random_name() -> str:
    return str(uuid.uuid4())


async def run_in_subprocess(
    location: ExecutionLocation,
    command: MutableSequence[str],
    capture_output: bool,
    timeout: int | None,
) -> tuple[str, int] | None:
    proc = await asyncio.create_subprocess_exec(
        *shlex.split(" ".join(command)),
        env=os.environ | location.environment,
        stdin=None,
        stdout=(
            asyncio.subprocess.PIPE if capture_output else asyncio.subprocess.DEVNULL
        ),
        stderr=(
            asyncio.subprocess.PIPE if capture_output else asyncio.subprocess.DEVNULL
        ),
    )
    if capture_output:
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return stdout.decode().strip(), proc.returncode
    else:
        await asyncio.wait_for(proc.wait(), timeout=timeout)
        return None


def wrap_command(command: str):
    return ["/bin/sh", "-c", f"{command}"]
