from __future__ import annotations

import os
import posixpath
import random
import string
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
