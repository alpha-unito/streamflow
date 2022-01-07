from __future__ import annotations

import asyncio
import base64
import itertools
import os
import posixpath
import tarfile
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, MutableSequence, MutableMapping, Optional, Union, Any, Set

from streamflow.core.data import LOCAL_LOCATION
from streamflow.core.workflow import Token
from streamflow.workflow.token import ListToken, TerminationToken, ObjectToken

if TYPE_CHECKING:
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


def check_termination(inputs: Union[Token, Iterable[Token]]) -> bool:
    if isinstance(inputs, Token):
        return isinstance(inputs, TerminationToken)
    else:
        for token in inputs:
            if isinstance(token, MutableSequence):
                if check_termination(token):
                    return True
            elif isinstance(token, TerminationToken):
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


def extract_tar_stream(tar: tarfile.TarFile,
                       src: str,
                       dst: str) -> None:
    for member in tar:
        if os.path.isdir(dst):
            if posixpath.join('/', member.path) == src:
                member.path = posixpath.basename(member.path)
                tar.extract(member, dst)
                if member.isdir():
                    dst = os.path.join(dst, member.path)
            else:
                member.path = posixpath.relpath(posixpath.join('/', member.path), src)
                tar.extract(member, dst)
        elif member.isfile():
            with tar.extractfile(member) as inputfile:
                with open(dst, 'wb') as outputfile:
                    outputfile.write(inputfile.read())
        else:
            parent_dir = str(Path(dst).parent)
            member.path = posixpath.basename(member.path)
            tar.extract(member, parent_dir)


def encode_command(command: str):
    return "echo {command} | base64 -d | sh".format(
        command=base64.b64encode(command.encode('utf-8')).decode('utf-8'))


def get_path_processor(connector: Connector):
    return posixpath if connector is not None and connector.deployment_name != LOCAL_LOCATION else os.path


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


def random_name() -> str:
    return str(uuid.uuid4())
