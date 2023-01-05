import io
import json
from contextlib import redirect_stdout
from typing import Any, Dict, Optional, Tuple

from cwltest import utils


def pytest_cwl_execute_test(
    config: utils.CWLTestConfig, processfile: str, jobfile: Optional[str]
) -> Tuple[int, Optional[Dict[str, Any]]]:
    from streamflow.core.exception import WorkflowException
    from streamflow.cwl.runner import main

    args = ["--outdir", config.outdir, processfile]
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
