from __future__ import annotations

import asyncio
import base64
import os
import posixpath
import random
import string
import tarfile
from pathlib import Path
from typing import TYPE_CHECKING, MutableSequence, MutableMapping, Optional, Union, Any

from streamflow.core.workflow import Token, TerminationToken, Step

if TYPE_CHECKING:
    from typing import Iterable
    from typing_extensions import Text


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


def create_command(command: MutableSequence[Text],
                   environment: MutableMapping[Text, Text] = None,
                   workdir: Optional[Text] = None,
                   stdin: Optional[Union[int, Text]] = None,
                   stdout: Union[int, Text] = asyncio.subprocess.STDOUT,
                   stderr: Union[int, Text] = asyncio.subprocess.STDOUT) -> Text:
    command = "".join(
        "{workdir}"
        "{environment}"
        "{command}"
        "{stdin}"
        "{stdout}"
        "{stderr}"
    ).format(
        workdir="cd {workdir} && ".format(workdir=workdir) if workdir is not None else "",
        environment="".join(["export %s=%s && " % (key, value) for (key, value) in
                             environment.items()]) if environment is not None else "",
        command=" ".join(command),
        stdin=" < {stdin}".format(stdin=stdin) if stdin is not None else "",
        stdout=" > {stdout}".format(stdout=stdout) if stdout != asyncio.subprocess.STDOUT else "",
        stderr=(" 2>&1" if stderr == stdout else
                " 2>{stderr}".format(stderr=stderr) if stderr != asyncio.subprocess.STDOUT else
                ""))
    return command


def extract_tar_stream(tar: tarfile.TarFile,
                       src: Text,
                       dst: Text) -> None:
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


def encode_command(command: Text):
    return "echo {command} | base64 -d | sh".format(
        command=base64.b64encode(command.encode('utf-8')).decode('utf-8'))


def get_path_processor(step: Step):
    if step is not None and step.target is not None:
        return posixpath
    else:
        return os.path


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


def get_tag(tokens: MutableSequence[Token]) -> str:
    output_tag = '/'
    for tag in [t.tag for t in tokens]:
        if len(tag) > len(output_tag):
            output_tag = tag
    return output_tag


def get_token_value(token: Token) -> Any:
    if isinstance(token.job, MutableSequence):
        return [get_token_value(t) for t in token.value]
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


def random_name() -> Text:
    return ''.join([random.choice(string.ascii_letters) for _ in range(6)])
