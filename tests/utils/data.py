import posixpath
from pathlib import Path

import tests
from streamflow.core.utils import random_name


def get_data_path(*args: str) -> Path:
    path = Path(tests.__file__).parent.joinpath("data")
    for arg in args:
        path = path.joinpath(arg)
    return path


def random_abs_path(depth=2):
    return posixpath.join(posixpath.sep, *(random_name() for _ in range(depth)))
