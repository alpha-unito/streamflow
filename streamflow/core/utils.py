from __future__ import annotations

import io
import os
import posixpath
import random
import string
import tarfile
from pathlib import Path
from typing import TYPE_CHECKING, MutableSequence

from streamflow.core.workflow import TerminationToken, Step

if TYPE_CHECKING:
    from streamflow.core.workflow import Token
    from typing import Iterable
    from typing_extensions import Text


def check_termination(inputs: Iterable[Token]) -> bool:
    for token in inputs:
        if isinstance(token, MutableSequence):
            if check_termination(token):
                return True
        elif isinstance(token, TerminationToken):
            return True
    return False


def create_tar_from_byte_stream(byte_buffer: io.BytesIO,
                                src: Text,
                                dst: Text) -> None:
    byte_buffer.flush()
    byte_buffer.seek(0)
    with tarfile.open(fileobj=byte_buffer, mode='r:') as tar:
        for member in tar.getmembers():
            if os.path.isdir(dst):
                if member.path == src:
                    member.path = posixpath.basename(member.path)
                    tar.extract(member, dst)
                    if member.isdir():
                        dst = os.path.join(dst, member.path)
                else:
                    member.path = posixpath.relpath(member.path, src)
                    tar.extract(member, dst)
            elif member.isfile():
                with tar.extractfile(member) as inputfile:
                    with open(dst, 'wb') as outputfile:
                        outputfile.write(inputfile.read())
            else:
                parent_dir = str(Path(dst).parent)
                member.path = posixpath.basename(member.path)
                tar.extract(member, parent_dir)


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


def flatten_list(hierarchical_list):
    if not hierarchical_list:
        return hierarchical_list
    if isinstance(hierarchical_list[0], MutableSequence):
        return flatten_list(hierarchical_list[0]) + flatten_list(hierarchical_list[1:])
    return hierarchical_list[:1] + flatten_list(hierarchical_list[1:])


def random_name() -> Text:
    return ''.join([random.choice(string.ascii_letters) for _ in range(6)])
