from __future__ import annotations

import os
import posixpath
import tempfile
from collections import MutableMapping
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from streamflow.core.workflow import Job, Task
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


def get_path_processor(task: Task):
    if task.target is not None:
        return posixpath
    else:
        return os.path
