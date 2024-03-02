import argparse
import json
import tempfile
from typing import MutableMapping

import pytest

from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import WorkflowExecutionException
from streamflow.cwl import runner

from cwltool.tests.util import get_data


def _get_args(
    cwltool_definition_relpath: str,
    cwltool_config_relpath: str | None = None,
    inline_config: MutableMapping | None = None,
) -> argparse.Namespace:
    temp_dir = tempfile.TemporaryDirectory()

    cwl_definition_abspath = get_data(cwltool_definition_relpath)
    if (cwltool_config_relpath and inline_config) or (
        not cwltool_config_relpath and not inline_config
    ):
        raise Exception(
            f"cwltool_config_relpath and inline_config parameters are mutually exclusive. Define just one of them. Current values: {cwltool_config_relpath, inline_config}"
        )
    elif cwltool_config_relpath:
        cwl_config_abspath = get_data(cwltool_config_relpath)
    elif inline_config:
        temp_config = tempfile.NamedTemporaryFile(delete=False)
        cwl_config_abspath = temp_config.name
        with open(cwl_config_abspath, "w") as fd:
            fd.write(json.dumps(inline_config))

    return runner.parser.parse_args(
        [
            cwl_definition_abspath,
            cwl_config_abspath,
            "--debug",
            "--outdir",
            temp_dir.name,
        ]
    )


@pytest.mark.asyncio
async def test_dot_product_transformer_raise_error(context: StreamFlowContext) -> None:
    """Test DotProductSizeTransformer which must raise an exception because the size tokens have different values"""
    args = _get_args(
        "tests/wf/scatter-wf4.cwl",
        inline_config={"inp1": ["one", "two", "extra"], "inp2": ["three", "four"]},
    )
    with pytest.raises(WorkflowExecutionException):
        await runner._async_main(args)
