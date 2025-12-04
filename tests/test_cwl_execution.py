import asyncio
import itertools
import os
import posixpath
from collections.abc import MutableSequence
from typing import cast

import pytest

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataType
from streamflow.cwl import utils as cwl_utils
from streamflow.cwl.command import CWLCommand, CWLCommandTokenProcessor
from streamflow.cwl.hardware import CWLHardwareRequirement
from streamflow.cwl.step import CWLExecuteStep, CWLInputInjectorStep, CWLScheduleStep
from streamflow.cwl.token import CWLFileToken
from streamflow.cwl.translator import create_command_output_processor_base
from streamflow.cwl.workflow import CWLWorkflow
from streamflow.data.remotepath import StreamFlowPath
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.token import ListToken, ObjectToken
from streamflow.workflow.utils import get_job_token
from tests.utils.cwl import get_cwl_parser
from tests.utils.deployment import get_local_deployment_config, get_location
from tests.utils.utils import inject_tokens
from tests.utils.workflow import (
    CWL_VERSION,
    create_deploy_step,
    create_schedule_step,
    create_workflow,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "token_type,file_type",
    itertools.product(("file", "list", "object"), ("file", "dir", "listing")),
)
async def test_initial_workdir(
    context: StreamFlowContext, token_type: str, file_type: str
) -> None:
    """
    Test the initial working directory.
    Copy the input files to the job's working directory and compute the echo of the paths.
    Verify that the files exist in the directory and that the printed paths match the expected values.
    """
    in_port_name = "in_1"
    out_port_name = "out-1"
    step_name = posixpath.join(posixpath.sep, utils.random_name())
    # Get deployment
    deployment_config = get_local_deployment_config()
    execution_location = await get_location(
        context, deployment_t=deployment_config.type
    )
    # Create workflow
    workflow, (in_port, out_port) = await create_workflow(context, num_port=2)
    deploy_step = create_deploy_step(
        workflow=workflow, deployment_config=deployment_config
    )
    injector_schedule_step = create_schedule_step(
        workflow,
        cls=CWLScheduleStep,
        deploy_steps=[deploy_step],
        name_prefix=step_name + "-injector/__schedule__",
        hardware_requirement=CWLHardwareRequirement(cwl_version=CWL_VERSION),
    )
    injector = workflow.create_step(
        cls=CWLInputInjectorStep,
        name=step_name + "-injector",
        job_port=injector_schedule_step.get_output_port(),
    )
    injector.add_input_port(in_port_name, in_port)
    injector.add_output_port(in_port_name, workflow.create_port())
    schedule_step = create_schedule_step(
        workflow,
        cls=CWLScheduleStep,
        deploy_steps=[deploy_step],
        name_prefix=step_name,
        hardware_requirement=CWLHardwareRequirement(cwl_version=CWL_VERSION),
    )
    schedule_step.add_input_port(in_port_name, injector.get_output_port())
    execute_step = workflow.create_step(
        cls=CWLExecuteStep,
        name=step_name,
        job_port=schedule_step.get_output_port(),
        recoverable=True,
    )
    workdir_expression = f"$(inputs.{in_port_name}"
    if token_type == "object":
        workdir_expression += ".a"
    if file_type == "listing":
        if token_type == "list":
            workdir_expression += "[0]"
        workdir_expression += ".listing"
    workdir_expression += ")"
    execute_step.command = CWLCommand(
        step=execute_step,
        base_command=["echo"],
        processors=[
            CWLCommandTokenProcessor(name=in_port_name, expression=workdir_expression)
        ],
        full_js=True,
        initial_work_dir=workdir_expression,
    )
    execute_step.add_input_port(in_port_name, injector.get_output_port())
    execute_step.add_output_port(
        name=out_port_name,
        port=out_port,
        output_processor=create_command_output_processor_base(
            port_name=out_port.name,
            workflow=cast(CWLWorkflow, workflow),
            port_target=None,
            port_type="string",
            cwl_element=get_cwl_parser(CWL_VERSION).CommandOutputParameter(
                type_="string"
            ),
            context={"hints": {}, "requirements": {}, "version": CWL_VERSION},
        ),
    )
    # Create input data
    path = None
    try:
        if file_type == "file":
            path = StreamFlowPath(
                "/tmp", "test.txt", context=context, location=execution_location
            )
            await path.write_text("Hello world")
            path = await path.resolve()
            file_token = CWLFileToken(
                {
                    "class": "File",
                    "path": str(path),
                }
            )
            initial_paths = [str(path)]
        elif file_type in ("dir", "listing"):
            path = StreamFlowPath(
                "/tmp", "land", context=context, location=execution_location
            )
            await path.mkdir()
            path = await path.resolve()
            await (path / "lvl1").mkdir()
            await (path / "lvl2").mkdir()
            await (path / "lvl1" / "text.txt").write_text("Hello dir")
            file_token = CWLFileToken(
                {
                    "class": "Directory",
                    "path": str(path),
                    "listing": [
                        {
                            "class": "Directory",
                            "path": str(path / "lvl1"),
                            "listing": [
                                {
                                    "class": "File",
                                    "path": str(path / "lvl1" / "text.txt"),
                                }
                            ],
                        },
                        {"class": "Directory", "path": str(path / "lvl2")},
                    ],
                }
            )
            initial_paths = (
                [str(path)]
                if file_type == "dir"
                else [str(path / "lvl1"), str(path / "lvl2")]
            )
        else:
            raise NotImplementedError(f"Unsupported file type: {file_type}")
        # Inject input token
        if token_type == "file":
            token_value = file_token
        elif token_type == "list":
            token_value = ListToken([file_token])
        elif token_type == "object":
            token_value = ObjectToken({"a": file_token})
        else:
            raise ValueError(f"Invalid token_type: {token_type}")
        await inject_tokens([token_value], in_port, context)
        # Execute workflow
        await workflow.save(context)
        executor = StreamFlowExecutor(workflow)
        await executor.run()
        token = next(iter(execute_step.get_output_port(out_port_name).token_list))
        job = get_job_token(
            posixpath.join(execute_step.name, "0"),
            schedule_step.get_output_port().token_list,
        ).value
        outdir = StreamFlowPath(
            job.output_directory, context=context, location=execution_location
        )
        files_found = [str(f) async for f in outdir.glob("*")]
        for out_file, init_path in zip(
            token.value.split(" "), initial_paths, strict=True
        ):
            # Check whether the file has been copied to the job output directory
            files_found.remove(out_file)
            #  Check whether the command argument has the correct path
            assert out_file != str(init_path)
            assert out_file == os.path.join(
                job.output_directory, os.path.basename(init_path)
            )
        assert len(files_found) == 0
    finally:
        if path:
            await path.rmtree()


@pytest.mark.asyncio
@pytest.mark.parametrize("file_type", ("file", "directory"))
async def test_creating_file(
    chosen_deployment_types: MutableSequence[str],
    context: StreamFlowContext,
    file_type: str,
) -> None:
    """Test the creation of files and their registration in the `DataManager`."""
    deployments = ["local", "docker"]
    for deployment in deployments:
        if deployment not in chosen_deployment_types:
            pytest.skip(f"Deployment {deployment} was not activated")
    locations = await asyncio.gather(
        *(
            asyncio.create_task(get_location(context, deployment))
            for deployment in deployments
        )
    )
    basename = utils.random_name()
    path = os.path.join("/tmp", basename)
    match file_type:
        case "file":
            await cwl_utils.write_remote_file(
                context=context,
                locations=locations,
                content="Hello",
                path=path,
                relpath=basename,
            )
        case "directory":
            await cwl_utils.create_remote_directory(
                context=context, locations=locations, path=path, relpath=basename
            )
        case _:
            raise ValueError(f"Unsupported file type: {file_type}")
    real_paths = []
    for location in locations:
        src_location = await context.data_manager.get_source_location(
            path, location.deployment
        )
        assert src_location is not None
        real_path = await StreamFlowPath(
            path, context=context, location=location
        ).resolve()
        assert real_path is not None
        real_paths.append(str(real_path))
        assert src_location.path == real_paths[-1]
    assert len(
        context.data_manager.get_data_locations(path=path, data_type=DataType.PRIMARY)
    ) == len(deployments)
    assert len(
        context.data_manager.get_data_locations(
            path=path, data_type=DataType.SYMBOLIC_LINK
        )
    ) == len([p for p in real_paths if p != path])
