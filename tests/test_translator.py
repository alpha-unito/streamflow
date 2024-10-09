import json
import os
import tempfile
from collections.abc import MutableMapping
from typing import Any

import cwltool.context
import pytest

from streamflow.config.config import WorkflowConfig
from streamflow.config.validator import SfValidator
from streamflow.cwl.token import CWLFileToken
from streamflow.cwl.workflow import CWLWorkflow

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.cwl.runner import main

from cwltool.tests.util import get_data

from streamflow.cwl.translator import CWLTranslator
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.token import TerminationToken
from tests.utils.deployment import get_docker_deployment_config
from tests.utils.workflow import CWL_VERSION


def _create_file(content: MutableMapping[Any, Any]) -> str:
    temp_config = tempfile.NamedTemporaryFile(delete=False)
    with open(temp_config.name, "w") as fd:
        fd.write(json.dumps(content))
    return temp_config.name


@pytest.mark.asyncio
async def test_inject_remote_input(context: StreamFlowContext) -> None:
    cwl_workflow_path = "/path/to/local/cwl/wf_description"
    remote_workdir = "/remote/workdir"
    relative_data_path = "relative/path/to/data"
    input_data = {
        "class": "Directory",
        "location": f"file://{cwl_workflow_path}/{relative_data_path}",
    }
    streamflow_config = {
        "version": "v1.0",
        "workflows": {
            "test": {
                "type": "cwl",
                "config": {
                    "file": "cwl/main.cwl",
                    "settings": "cwl/config.yaml",
                },
                "bindings": [
                    {
                        "port": "/model",
                        "target": {
                            "deployment": "awesome",
                            "workdir": remote_workdir,
                        },
                    }
                ],
            }
        },
        "deployments": {
            "awesome": {
                "type": "docker",
                "config": {"image": get_docker_deployment_config().config["image"]},
            }
        },
    }

    # Check StreamFlow file schema
    SfValidator().validate(streamflow_config)

    # Build workflow
    translator = CWLTranslator(
        context=context,
        name=utils.random_name(),
        output_directory=tempfile.gettempdir(),
        cwl_definition=None,  # cwltool.process.Process,
        cwl_inputs={"model": input_data},
        workflow_config=WorkflowConfig(
            list(streamflow_config["workflows"].keys())[0], streamflow_config
        ),
        loading_context=cwltool.context.LoadingContext(),
    )
    workflow = CWLWorkflow(
        context=context,
        config={},
        name=translator.name,
        cwl_version=CWL_VERSION,
    )
    translator._inject_input(
        workflow=workflow,
        port_name="model",
        global_name="/model",
        port=workflow.create_port(),
        output_directory=cwl_workflow_path,
        value=translator.cwl_inputs["model"],
    )

    # Check input tokens
    input_tokens = workflow.steps["/model-injector"].get_input_port("model").token_list
    assert input_tokens[0].value == input_data
    assert isinstance(input_tokens[1], TerminationToken)

    # Execute injector steps
    executor = StreamFlowExecutor(workflow)
    await executor.run()

    # Check output tokens
    output_tokens = (
        workflow.steps["/model-injector"].get_output_port("model").token_list
    )
    assert isinstance(output_tokens[0], CWLFileToken)
    assert isinstance(output_tokens[1], TerminationToken)
    job = (
        workflow.steps["/model-injector/__schedule__"]
        .get_output_port("__job__")
        .token_list[0]
        .value
    )
    assert (
        len({job.input_directory, job.output_directory, job.tmp_directory}) == 1
        and job.input_directory == remote_workdir
    )
    assert output_tokens[0].value["class"] == "Directory"
    assert output_tokens[0].value["path"] == os.path.join(
        remote_workdir, relative_data_path
    )


def test_dot_product_transformer_raises_error(context: StreamFlowContext) -> None:
    """Test DotProductSizeTransformer which must raise an exception because the size tokens have different values"""
    params = [
        get_data("tests/wf/scatter-wf4.cwl"),
        _create_file({"inp1": ["one", "two", "extra"], "inp2": ["three", "four"]}),
    ]
    assert main(params) == 1
