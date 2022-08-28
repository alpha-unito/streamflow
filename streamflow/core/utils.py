from __future__ import annotations

import asyncio
import base64
import importlib
import itertools
import os
import posixpath
import uuid
from pathlib import Path
from typing import Any, MutableMapping, MutableSequence, Optional, Set, TYPE_CHECKING, Type, Union

from importlib_metadata import entry_points
from jsonref import loads

from streamflow.core.data import LOCAL_LOCATION
from streamflow.core.exception import InvalidPluginException, WorkflowExecutionException
from streamflow.core.workflow import Token
from streamflow.data import aiotarstream
from streamflow.ext import StreamFlowPlugin
from streamflow.log_handler import logger
from streamflow.workflow.token import IterationTerminationToken, ListToken, ObjectToken, TerminationToken

if TYPE_CHECKING:
    from streamflow.core.config import SchemaEntity
    from streamflow.core.deployment import Connector
    from typing import Iterable


class NamesStack(object):

    def __init__(self):
        self.stack: MutableSequence[Set] = [set()]

    def add_scope(self):
        self.stack.append(set())

    def add_name(self, name: str):
        self.stack[-1].add(name)

    def delete_scope(self):
        self.stack.pop()

    def delete_name(self, name: str):
        self.stack[-1].remove(name)

    def global_names(self) -> Set[str]:
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


def check_iteration_termination(inputs: Union[Token, Iterable[Token]]) -> bool:
    return check_token_class(inputs, IterationTerminationToken)


def check_termination(inputs: Union[Token, Iterable[Token]]) -> bool:
    return check_token_class(inputs, TerminationToken)


def check_token_class(inputs: Union[Token, Iterable[Token]], cls: Type[Token]):
    if isinstance(inputs, Token):
        return isinstance(inputs, cls)
    else:
        for token in inputs:
            if isinstance(token, MutableSequence):
                if check_token_class(token, cls):
                    return True
            elif isinstance(token, cls):
                return True
        return False


def create_command(command: MutableSequence[str],
                   environment: MutableMapping[str, str] = None,
                   workdir: Optional[str] = None,
                   stdin: Optional[Union[int, str]] = None,
                   stdout: Union[int, str] = asyncio.subprocess.STDOUT,
                   stderr: Union[int, str] = asyncio.subprocess.STDOUT) -> str:
    command = "".join(
        "{workdir}"
        "{environment}"
        "{command}"
        "{stdin}"
        "{stdout}"
        "{stderr}"
    ).format(
        workdir="cd {workdir} && ".format(workdir=workdir) if workdir is not None else "",
        environment="".join(["export %s=\"%s\" && " % (key, value) for (key, value) in
                             environment.items()]) if environment is not None else "",
        command=" ".join(command),
        stdin=" < {stdin}".format(stdin=stdin) if stdin is not None else "",
        stdout=" > {stdout}".format(stdout=stdout) if stdout != asyncio.subprocess.STDOUT else "",
        stderr=(" 2>&1" if stderr == stdout else
                " 2>{stderr}".format(stderr=stderr) if stderr != asyncio.subprocess.STDOUT else
                ""))
    return command


def dict_product(**kwargs) -> MutableMapping[Any, Any]:
    keys = kwargs.keys()
    vals = kwargs.values()
    for instance in itertools.product(*vals):
        yield dict(zip(keys, list(instance)))


async def extract_tar_stream(tar: aiotarstream.AioTarStream,
                             src: str,
                             dst: str,
                             transferBufferSize: Optional[int] = None) -> None:
    async for member in tar:
        if os.path.isdir(dst):
            if posixpath.join('/', member.path) == src:
                member.path = posixpath.basename(member.path)
                await tar.extract(member, dst)
                if member.isdir():
                    dst = os.path.join(dst, member.path)
            else:
                member.path = posixpath.relpath(posixpath.join('/', member.path), src)
                await tar.extract(member, dst)
        elif member.isfile():
            async with await tar.extractfile(member) as inputfile:
                with open(dst, 'wb') as outputfile:
                    while content := await inputfile.read(transferBufferSize):
                        outputfile.write(content)
        else:
            parent_dir = str(Path(dst).parent)
            member.path = posixpath.basename(member.path)
            await tar.extract(member, parent_dir)


def encode_command(command: str):
    return "echo {command} | base64 -d | sh".format(
        command=base64.b64encode(command.encode('utf-8')).decode('utf-8'))


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


def get_class_fullname(cls: Type):
    return cls.__module__ + '.' + cls.__qualname__


def get_class_from_name(name: str) -> Type:
    module_name, class_name = name.rsplit('.', 1)
    return getattr(importlib.import_module(module_name), class_name)


def get_path_processor(connector: Connector):
    return posixpath if connector is not None and connector.deployment_name != LOCAL_LOCATION else os.path


async def get_remote_to_remote_write_command(src_connector: Connector,
                                             src_location: str,
                                             src: str,
                                             dst_connector: Connector,
                                             dst_locations: MutableSequence[str],
                                             dst: str) -> MutableSequence[str]:
    if posixpath.basename(src) != posixpath.basename(dst):
        result, status = await src_connector.run(
            location=src_location,
            command=["test -d \"{path}\"".format(path=src)],
            capture_output=True)
        if status > 1:
            raise WorkflowExecutionException(result)
        # If is a directory
        elif status == 0:
            await asyncio.gather(*(asyncio.create_task(
                dst_connector.run(location=dst_location, command=["mkdir", "-p", dst])
            ) for dst_location in dst_locations))
            return ["tar", "xf", "-", "-C", dst, "--strip-components", "1"]
        # If is a file
        else:
            return ["tar", "xf", "-", "-O", ">", dst]
    else:
        return ["tar", "xf", "-", "-C", posixpath.dirname(dst)]


def get_size(path):
    if os.path.isfile(path):
        return os.path.getsize(path)
    else:
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(path, followlinks=True):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)
        return total_size


def get_tag(tokens: Iterable[Token]) -> str:
    output_tag = '0'
    for tag in [t.tag for t in tokens]:
        if len(tag) > len(output_tag):
            output_tag = tag
    return output_tag


def get_token_value(token: Token) -> Any:
    if isinstance(token, ListToken):
        return [get_token_value(t) for t in token.value]
    elif isinstance(token, ObjectToken):
        return {k: get_token_value(v) for k, v in token.value.items()}
    elif isinstance(token.value, Token):
        return get_token_value(token.value)
    else:
        return token.value


def inject_schema(schema: MutableMapping[str, Any],
                  classes: MutableMapping[str, Type[SchemaEntity]],
                  definition_name: str):
    for name, entity in classes.items():
        if entity_schema := entity.get_schema():
            with open(entity_schema, "r") as f:
                entity_schema = loads(
                    f.read(),
                    base_uri='file://{}/'.format(os.path.dirname(entity_schema)),
                    jsonschema=True)
            schema['definitions'][definition_name]['properties']['type'].setdefault('enum', []).append(name)
            schema['definitions'][definition_name]['definitions'][name] = entity_schema
            schema['definitions'][definition_name].setdefault('allOf', []).append({
                'if': {'properties': {'type': {'const': name}}},
                'then': {'properties': {'config': entity_schema}}})


def load_extensions():
    plugins = entry_points(group='unito.streamflow.plugin')
    for plugin in plugins:
        plugin = (plugin.load())()
        if isinstance(plugin, StreamFlowPlugin):
            plugin.register()
            logger.info("Succesfully registered plugin {}".format(
                get_class_fullname(type(plugin))))
        else:
            raise InvalidPluginException("StreamFlow plugins must extend the streamflow.ext.StreamFlowPlugin class")


def random_name() -> str:
    return str(uuid.uuid4())


def wrap_command(command: str):
    return ["/bin/sh", "-c", "{command}".format(command=command)]
