import itertools
import os
import posixpath
from typing import cast

import pytest

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataType
from streamflow.cwl.command import CWLCommand, CWLCommandTokenProcessor
from streamflow.cwl.hardware import CWLHardwareRequirement
from streamflow.cwl.step import CWLExecuteStep
from streamflow.cwl.token import CWLFileToken
from streamflow.cwl.translator import create_command_output_processor_base
from streamflow.cwl.workflow import CWLWorkflow
from streamflow.data.remotepath import StreamFlowPath
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.token import ListToken, ObjectToken
from streamflow.workflow.utils import get_job_token
from tests.utils.cwl import get_cwl_parser
from tests.utils.deployment import get_location
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
    itertools.product(("file", "list", "object"), ("file", "dir")),
)
async def test_initial_workdir(
    context: StreamFlowContext, token_type: str, file_type: str
) -> None:
    """
    Test the initial working directory.
    Copy the input files to the job's working directory and compute the echo of the paths.
    Verify that the files exist in the directory and that the printed paths match the expected values.
    """
    workflow, (in_port_schedule, in_port, out_port) = await create_workflow(
        context, num_port=3
    )
    step_name = posixpath.join(posixpath.sep, utils.random_name())
    deploy_step = create_deploy_step(workflow)
    schedule_step = create_schedule_step(
        workflow,
        [deploy_step],
        name_prefix=step_name,
        hardware_requirement=CWLHardwareRequirement(cwl_version=CWL_VERSION),
    )
    in_port_name = "in_1"
    out_port_name = "out-1"
    execute_step = workflow.create_step(
        cls=CWLExecuteStep,
        name=step_name,
        job_port=schedule_step.get_output_port(),
        recoverable=True,
    )
    arg = f"$(inputs.{in_port_name}" + (".a)" if token_type == "object" else ")")
    execute_step.command = CWLCommand(
        step=execute_step,
        base_command=["echo"],
        processors=[CWLCommandTokenProcessor(name=in_port_name, expression=arg)],
        full_js=True,
        initial_work_dir=arg,
    )
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
    execution_location = await get_location(context, "local")
    path = None
    try:
        if file_type == "file":
            path = StreamFlowPath(
                "/tmp", "test.txt", context=context, location=execution_location
            )
            await path.write_text("Hello world")
            path = await path.resolve()
        else:
            path = StreamFlowPath(
                "/tmp", "land", context=context, location=execution_location
            )
            await path.mkdir()
            path = await path.resolve()
            await (path / "lvl1").mkdir()
            await (path / "lvl2").mkdir(exist_ok=True)
            await (path / "lvl1" / "text.txt").write_text("Hello dir")
        file_token = CWLFileToken(
            {
                "class": "File" if file_type == "file" else "Directory",
                "path": str(path),
            }
        )
        context.data_manager.register_path(
            location=execution_location,
            path=str(path),
            relpath=str(path),
            data_type=DataType.PRIMARY,
        )
        if token_type == "file":
            token_value = file_token
        elif token_type == "list":
            token_value = ListToken([file_token])
        elif token_type == "object":
            token_value = ObjectToken({"a": file_token})
        else:
            raise ValueError(f"Invalid token_type: {token_type}")
        execute_step.add_input_port(in_port_name, in_port)
        await inject_tokens([token_value], in_port, context)

        schedule_step.add_input_port(in_port_name, in_port_schedule)
        await inject_tokens([token_value], in_port_schedule, context)

        await workflow.save(context)
        executor = StreamFlowExecutor(workflow)
        await executor.run()
        token = next(
            iter(next(iter(execute_step.get_output_ports().values())).token_list)
        )
        job = get_job_token(
            posixpath.join(execute_step.name, "0"),
            schedule_step.get_output_port().token_list,
        ).value
        # Check whether the file has been copied to the job output directory
        outdir = StreamFlowPath(
            job.output_directory, context=context, location=execution_location
        )
        async for f in outdir.glob("*"):
            assert f == token.value
        #  Check if the command argument has the correct path
        assert token.value != str(path)
        assert token.value == os.path.join(job.output_directory, path.parts[-1])
    finally:
        if path:
            await path.rmtree()
