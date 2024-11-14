import json
import os
import tempfile
from collections.abc import MutableMapping
from pathlib import PurePosixPath
from typing import Any

import cwltool.context
import pytest

from streamflow.config.config import WorkflowConfig
from streamflow.config.validator import SfValidator
from streamflow.core.deployment import _init_workdir
from streamflow.cwl.token import CWLFileToken
from streamflow.cwl.workflow import CWLWorkflow

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.cwl.runner import main

from cwltool.tests.util import get_data

from streamflow.cwl.translator import CWLTranslator
from streamflow.deployment.utils import get_binding_config
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.token import TerminationToken
from tests.utils.workflow import CWL_VERSION


def _create_file(content: MutableMapping[Any, Any]) -> str:
    temp_config = tempfile.NamedTemporaryFile(delete=False)
    with open(temp_config.name, "w") as fd:
        fd.write(json.dumps(content))
    return temp_config.name


def _get_workflow_config():
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
                        "step": "/compute_1",
                        "target": [
                            {"deployment": "wrapper_1"},
                            {
                                "deployment": "wrapper_2",
                                "workdir": "/other/remote/workdir_2",
                            },
                            {"deployment": "wrapper_3"},
                            {"deployment": "wrapper_4"},
                        ],
                    },
                    {
                        "port": "/model",
                        "target": {
                            "deployment": "awesome",
                            "workdir": "/remote/workdir/models",
                        },
                    },
                ],
            }
        },
        "deployments": {
            "awesome": {
                "type": "docker",
                "config": {"image": "busybox"},
            },
            "handsome": {
                "type": "docker",
                "config": {"image": "busybox"},
                "workdir": "/remote/workdir",
            },
            "wrapper_1": {
                "type": "docker",
                "config": {"image": "busybox"},
                "wraps": {"deployment": "handsome", "service": "boost"},
            },
            "wrapper_2": {
                "type": "docker",
                "config": {"image": "busybox"},
                "wraps": {"deployment": "handsome", "service": "boost"},
                "workdir": "/myremote/workdir",
            },
            "wrapper_3": {
                "type": "docker",
                "config": {"image": "busybox"},
                "wraps": "wrapper_1",
            },
            "wrapper_4": {
                "type": "docker",
                "config": {"image": "busybox"},
                "wraps": "awesome",
            },
        },
    }
    SfValidator().validate(streamflow_config)
    return WorkflowConfig(
        list(streamflow_config["workflows"].keys())[0], streamflow_config
    )


@pytest.mark.asyncio
async def test_inject_remote_input(context: StreamFlowContext) -> None:
    """Test injection of remote input data through the port targets in the StreamFlow file"""
    cwl_workflow_path = "/path/to/local/cwl/wf_description"
    relative_data_path = "relative/path/to/data"
    input_data = {
        "class": "Directory",
        "location": f"file://{cwl_workflow_path}/{relative_data_path}",
    }

    workflow_config = _get_workflow_config()
    remote_workdir = next(
        iter(workflow_config.get(PurePosixPath("/model"), "port")["targets"])
    )["workdir"]
    translator = CWLTranslator(
        context=context,
        name=utils.random_name(),
        output_directory=tempfile.gettempdir(),
        cwl_definition=None,  # cwltool.process.Process,
        cwl_inputs={"model": input_data},
        workflow_config=workflow_config,
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


def test_workdir_inheritance() -> None:
    """Test the workdir inheritance of deployments, wrapped deployments and targets"""
    workflow_config = _get_workflow_config()
    workdir_deployment_1 = workflow_config.deployments["handsome"]["workdir"]
    binding_config = get_binding_config("/compute_1", "step", workflow_config)

    # The `wrapper_1` deployment does NOT have a `workdir` and wraps the `handsome` deployment
    # Inherit `workdir` of the wrapped deployment
    assert binding_config.targets[0].deployment.name == "wrapper_1"
    assert binding_config.targets[0].deployment.workdir == workdir_deployment_1
    assert binding_config.targets[0].workdir == workdir_deployment_1

    # The `wrapper_2` deployment has a `workdir` and wraps the `handsome` deployment
    # Get `workdir` of the `wrapper_2` deployment
    assert binding_config.targets[1].deployment.name == "wrapper_2"
    assert (
        binding_config.targets[1].deployment.workdir
        == workflow_config.deployments["wrapper_2"]["workdir"]
    )
    # The step target can define a different workdir
    compute_1_target = workflow_config.get(PurePosixPath("/compute_1"), "step")
    assert (
        binding_config.targets[1].workdir == compute_1_target["targets"][1]["workdir"]
    )

    # The `wrapper_3` deployment does NOT have a `workdir` and wraps the `wrapper_1` deployment
    # Get `workdir` of the `handsome` deployment
    assert binding_config.targets[2].deployment.name == "wrapper_3"
    assert binding_config.targets[2].deployment.workdir == workdir_deployment_1
    assert binding_config.targets[2].workdir == workdir_deployment_1

    # The `wrapper_4` deployment does NOT has a `workdir` and wraps the `awesome` deployment
    # Get default `workdir` because `handsome` deployment does NOT has a `workdir` either
    assert binding_config.targets[3].deployment.name == "wrapper_4"
    assert binding_config.targets[3].deployment.workdir is None
    assert binding_config.targets[3].workdir == _init_workdir(
        binding_config.targets[3].deployment.name
    )


def test_dot_product_transformer_raises_error() -> None:
    """Test DotProductSizeTransformer which must raise an exception because the size tokens have different values"""
    params = [
        get_data("tests/wf/scatter-wf4.cwl"),
        _create_file({"inp1": ["one", "two", "extra"], "inp2": ["three", "four"]}),
    ]
    assert main(params) == 1
