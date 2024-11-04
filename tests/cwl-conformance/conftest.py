from __future__ import annotations

import io
import json
import os
from contextlib import redirect_stdout
from typing import Any

from cwltest import utils


def pytest_cwl_execute_test(
    config: utils.CWLTestConfig, processfile: str, jobfile: str | None
) -> tuple[int, dict[str, Any] | None]:
    from streamflow.core.exception import WorkflowException
    from streamflow.cwl.runner import main

    this_directory = os.path.abspath(os.path.dirname(__file__))
    args = [
        "--streamflow-file",
        os.path.join(this_directory, "streamflow.yml"),
        "--outdir",
        config.outdir,
        processfile,
    ]
    if jobfile is not None:
        args.append(jobfile)

    try:
        f = io.StringIO()
        with redirect_stdout(f):
            result = main(args)
            out = f.getvalue()
            return result, json.loads(out) if out else {}
    except WorkflowException:
        return 1, {}
