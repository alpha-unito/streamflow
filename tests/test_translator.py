from __future__ import annotations

import json
import tempfile
from typing import MutableMapping, Any


from streamflow.core.context import StreamFlowContext
from streamflow.cwl.runner import main

from cwltool.tests.util import get_data


def _create_file(content: MutableMapping[Any, Any]) -> str:
    temp_config = tempfile.NamedTemporaryFile(delete=False)
    with open(temp_config.name, "w") as fd:
        fd.write(json.dumps(content))
    return temp_config.name


def test_dot_product_transformer_raises_error(context: StreamFlowContext) -> None:
    """Test DotProductSizeTransformer which must raise an exception because the size tokens have different values"""
    params = [
        get_data("tests/wf/scatter-wf4.cwl"),
        _create_file({"inp1": ["one", "two", "extra"], "inp2": ["three", "four"]}),
    ]
    assert main(params) == 1
