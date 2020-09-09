from __future__ import annotations

import tempfile
from collections import MutableMapping
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from streamflow.core.workflow import Job
    from typing import Any
    from typing_extensions import Text


def build_context(job: Job) -> MutableMapping[Text, Any]:
    context = {
        'inputs': {},
        'self': None,
        'runtime': {}
    }
    for token in job.inputs:
        context['inputs'][token.name] = token.value
    context['runtime']['outdir'] = job.output_directory
    context['runtime']['tmpdir'] = '/tmp' if job.task.target is not None else tempfile.gettempdir()

    return context
