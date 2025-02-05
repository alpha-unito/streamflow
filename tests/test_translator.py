import json
import os
import posixpath
import tempfile
from collections.abc import MutableMapping
from pathlib import PurePosixPath
from typing import Any, cast

import cwl_utils.parser
import cwl_utils.parser.utils
import pytest
from cwltool.tests.util import get_data

from streamflow.config.config import WorkflowConfig
from streamflow.config.validator import SfValidator
from streamflow.core import utils
from streamflow.core.config import BindingConfig
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import Target
from streamflow.cwl.runner import main
from streamflow.cwl.step import CWLTransferStep
from streamflow.cwl.token import CWLFileToken
from streamflow.cwl.translator import CWLTranslator
from streamflow.cwl.workflow import CWLWorkflow
from streamflow.data.remotepath import StreamFlowPath
from streamflow.deployment.utils import get_binding_config
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.port import JobPort
from streamflow.workflow.step import DeployStep, ScheduleStep
from streamflow.workflow.token import TerminationToken
from tests.utils.deployment import (
    get_docker_deployment_config,
    get_location,
    get_service,
)
from tests.utils.workflow import CWL_VERSION


def _create_file(content: MutableMapping[Any, Any]) -> str:
    temp_config = tempfile.NamedTemporaryFile(delete=False)
    with open(temp_config.name, "w") as fd:
        fd.write(json.dumps(content))
    return temp_config.name


def _get_streamflow_config() -> MutableMapping[str, Any]:
    return {
        "version": "v1.0",
        "workflows": {
            "test": {
                "type": "cwl",
                "config": {
                    "file": "cwl/main.cwl",
                    "settings": "cwl/config.yaml",
                },
            }
        },
    }


def _get_workflow_config(streamflow_config) -> WorkflowConfig:
    SfValidator().validate(streamflow_config)
    return WorkflowConfig(
        list(streamflow_config["workflows"].keys())[0], streamflow_config
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("config", ["File", "Directory:literal", "Directory:concrete"])
async def test_inject_remote_input(context: StreamFlowContext, config: str) -> None:
    """Test injection of remote input data through the port targets in the StreamFlow file"""

    # Create remote file
    docker_config = get_docker_deployment_config()
    location = await get_location(context, docker_config.type)
    remote_workdir = await StreamFlowPath(
        "home", context=context, location=location
    ).resolve()
    remote_path = remote_workdir / "data"
    await remote_path.mkdir(exist_ok=True)
    assert await remote_path.exists()

    file_type, *other = config.split(":")
    if file_type == "Directory":  # Input Directory
        dir_type = other[0]
        remote_path = remote_path / f"dir_{dir_type}"
        await remote_path.mkdir()
        assert await remote_path.exists()
        relative_path = os.path.relpath(remote_path, remote_workdir)
        if dir_type == "concrete":
            await (remote_path / "file0.txt").write_text("CWL")
            assert await (remote_path / "file0.txt").exists()
        input_dict = {
            "class": "Directory",
            "path": relative_path,
            "listing": [
                {"class": "File", "path": os.path.join(relative_path, "file0.txt")}
            ],
        }
    else:  # Input File
        remote_path = remote_path / "file1.txt"
        await remote_path.write_text("StreamFlow")
        assert await remote_path.exists()
        relative_path = os.path.relpath(remote_path, remote_workdir)
        await (remote_path.parent / "file2.txt").write_text("Workflow Manager")
        assert await (remote_path.parent / "file2.txt").exists()
        input_dict = {
            "class": "File",
            "path": relative_path,
            "secondaryFiles": [
                {
                    "class": "File",
                    "path": os.path.join(os.path.dirname(relative_path), "file2.txt"),
                }
            ],
        }

    # Create input data and call the `CWLTranslator` inject method
    cwl_workflow_path = os.path.dirname(__file__)
    port_name = "model"
    cwl_inputs = cwl_utils.parser.utils.load_inputfile_by_yaml(
        version=CWL_VERSION,
        yaml={port_name: input_dict},
        uri=__file__,
    )
    streamflow_config = _get_streamflow_config()
    streamflow_config["workflows"]["test"].setdefault("bindings", []).append(
        {
            "port": f"/{port_name}",
            "target": {
                "deployment": docker_config.name,
                "workdir": str(remote_workdir),
            },
        }
    )
    streamflow_config.setdefault("deployments", {})[docker_config.name] = {
        "type": docker_config.type,
        "config": docker_config.config,
    }

    workflow_config = _get_workflow_config(streamflow_config)
    translator = CWLTranslator(
        context=context,
        name=utils.random_name(),
        output_directory=tempfile.gettempdir(),
        cwl_definition=None,  # CWL object
        cwl_inputs=cwl_inputs,
        cwl_inputs_path=None,
        workflow_config=workflow_config,
    )
    workflow = CWLWorkflow(
        context=context,
        config={},
        name=translator.name,
        cwl_version=CWL_VERSION,
    )
    translator._inject_input(
        workflow=workflow,
        port_name=port_name,
        global_name=f"/{port_name}",
        port=workflow.create_port(),
        output_directory=cwl_workflow_path,
        value=translator.cwl_inputs[port_name],
    )

    # Add a transfer step in the workflow
    injector_schedule_step = workflow.steps[
        posixpath.join(posixpath.sep, f"{port_name}-injector", "__schedule__")
    ]
    input_injector_step = workflow.steps[
        posixpath.join(posixpath.sep, f"{port_name}-injector")
    ]
    binding_config = BindingConfig(
        targets=[
            Target(
                deployment=docker_config,
                service=get_service(context, docker_config.type),
                workdir=docker_config.workdir,
            )
        ]
    )
    schedule_step = workflow.create_step(
        cls=ScheduleStep,
        name=posixpath.join(posixpath.sep, port_name, "__schedule__"),
        job_prefix=posixpath.join(posixpath.sep, port_name),
        connector_ports={
            docker_config.name: next(
                iter(s for s in workflow.steps.values() if isinstance(s, DeployStep))
            ).get_output_port()
        },
        binding_config=binding_config,
    )
    transfer_step = workflow.create_step(
        cls=CWLTransferStep,
        name=posixpath.join(posixpath.sep, port_name, "__transfer__", port_name),
        job_port=schedule_step.get_output_port(),
    )
    transfer_step.add_input_port(port_name, input_injector_step.get_output_port())
    transfer_step.add_output_port(port_name, workflow.create_port())

    # Check input tokens
    input_tokens = input_injector_step.get_input_port(port_name).token_list
    assert input_tokens[0].value["class"] == file_type
    assert input_tokens[0].value["path"] == str(remote_path)
    assert isinstance(input_tokens[1], TerminationToken)

    # Execute workflow
    #   Deploy step -> { ScheduleInjector, ScheduleTransfer }
    #   ScheduleInjector step -> { Injector }
    #   Injector step -> { Transfer }
    #   ScheduleTransfer step -> { Transfer }
    #   Transfer step -> {}
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    job = await cast(
        JobPort,
        injector_schedule_step.get_output_port("__job__"),
    ).get_job(port_name)

    # Check output tokens of input injector step
    output_tokens = input_injector_step.get_output_port(port_name).token_list
    assert isinstance(output_tokens[0], CWLFileToken)
    assert isinstance(output_tokens[1], TerminationToken)
    assert len(
        {job.input_directory, job.output_directory, job.tmp_directory}
    ) == 1 and job.input_directory == str(remote_workdir)
    assert output_tokens[0].value["class"] == file_type
    assert output_tokens[0].value["path"] == str(remote_workdir / relative_path)

    # Check output tokens of transfer step
    output_tokens = transfer_step.get_output_port(port_name).token_list
    assert isinstance(output_tokens[0], CWLFileToken)
    assert isinstance(output_tokens[1], TerminationToken)
    assert output_tokens[0].value["class"] == file_type

    if file_type == "Directory":
        remote_files = sorted(
            [p async for p in remote_path.glob("*")],
            key=lambda x: os.path.basename(x),
        )
        assert len(remote_files) == 1
        wf_files = sorted(
            output_tokens[0].value["listing"],
            key=lambda x: x["basename"],
        )
        assert len(wf_files) == 1
    else:
        remote_files = sorted(
            [p async for p in remote_path.parent.glob("*")],
            key=lambda x: os.path.basename(x),
        )
        assert len(remote_files) == 2
        wf_files = sorted(
            (output_tokens[0].value, *output_tokens[0].value["secondaryFiles"]),
            key=lambda x: x["basename"],
        )
        assert len(wf_files) == 2

    for remote_file, wf_file in zip(remote_files, wf_files):
        assert wf_file["basename"] == os.path.basename(remote_file)
        assert wf_file["checksum"] == f"sha1${await remote_file.checksum()}"


def test_workdir_inheritance() -> None:
    """Test the workdir inheritance of deployments, wrapped deployments and targets"""
    streamflow_config = _get_streamflow_config()
    streamflow_config["workflows"]["test"].setdefault("bindings", []).append(
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
    )
    streamflow_config.setdefault("deployments", {})
    streamflow_config["deployments"] |= {
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
    }

    workflow_config = _get_workflow_config(streamflow_config)
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
    assert binding_config.targets[3].workdir == (
        os.path.join(os.path.realpath(tempfile.gettempdir()), "streamflow")
        if binding_config.targets[3].deployment == "local"
        else posixpath.join("/tmp", "streamflow")
    )


def test_dot_product_transformer_raises_error() -> None:
    """Test DotProductSizeTransformer which must raise an exception because the size tokens have different values"""
    params = [
        get_data("tests/wf/scatter-wf4.cwl"),
        _create_file({"inp1": ["one", "two", "extra"], "inp2": ["three", "four"]}),
    ]
    assert main(params) == 1
