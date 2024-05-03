from pathlib import Path

import tests


def get_data_path(*args: str) -> Path:
    path = Path(tests.__file__).parent.joinpath("data")
    for arg in args:
        path = path.joinpath(arg)
    return path
