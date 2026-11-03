from __future__ import annotations

import os
import posixpath
from collections.abc import MutableSequence
from typing import cast

import pytest

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.cwl.hardware import CWLHardwareRequirement
from streamflow.cwl.step import CWLExecuteStep, CWLScheduleStep
from streamflow.cwl.translator import create_command_output_processor_base
from streamflow.cwl.workflow import CWLWorkflow
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.utils import get_job_token
from tests.utils.cwl import get_cwl_parser
from tests.utils.deployment import get_deployment_config
from tests.utils.workflow import (
    CWL_VERSION,
    create_deploy_step,
    create_schedule_step,
    create_workflow,
)

DEPLOYMENTS = ("local", "docker", "docker-wrapper")


async def _run_env_tool(
    context: StreamFlowContext,
    deployment_t: str,
) -> tuple[tuple[str, str], str | None]:
    """Execute a minimal CWL CLT tool printing `env` and return (home, tmpdir), stdout."""
    from streamflow.cwl.command import CWLCommand

    deployment_config = await get_deployment_config(context, deployment_t)
    step_name = posixpath.join(posixpath.sep, utils.random_name())
    workflow, (out_port,) = await create_workflow(context, num_port=1, save=False)
    deploy_step = create_deploy_step(
        workflow=workflow, deployment_config=deployment_config
    )
    schedule_step = create_schedule_step(
        workflow,
        cls=CWLScheduleStep,
        deploy_steps=[deploy_step],
        name_prefix=step_name,
        hardware_requirement=CWLHardwareRequirement(cwl_version=CWL_VERSION),
    )
    execute_step = workflow.create_step(
        cls=CWLExecuteStep,
        name=step_name,
        job_port=schedule_step.get_output_port(),
        recoverable=True,
    )
    execute_step.command = CWLCommand(
        step=execute_step,
        base_command=["env"],
        environment={"FOO_A": "BAR"},
        full_js=True,
    )
    out_port_name = utils.random_name()
    execute_step.add_output_port(
        name=out_port_name,
        port=out_port,
        output_processor=create_command_output_processor_base(
            port_name=out_port.name,
            workflow=cast(CWLWorkflow, workflow),
            port_target=None,
            port_type="string",
            cwl_element=get_cwl_parser(CWL_VERSION).CommandOutputParameter(
                id=utils.random_name(), type_="string"
            ),
            context={"hints": {}, "requirements": {}, "version": CWL_VERSION},
        ),
    )
    await workflow.save(context.database)
    executor = StreamFlowExecutor(workflow)
    await executor.run()
    job = get_job_token(
        posixpath.join(execute_step.name, "0"),
        schedule_step.get_output_port().token_list,
    ).value
    stdout = next(iter(execute_step.get_output_port(out_port_name).token_list)).value
    return (
        (job.output_directory or "", job.tmp_directory or ""),
        stdout,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("deployment_t", DEPLOYMENTS)
async def test_tool_uses_clean_cwl_environment(
    chosen_deployment_types: MutableSequence[str],
    context: StreamFlowContext,
    deployment_t: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The tool runs in a fresh environment, per the CWL spec."""
    if deployment_t not in chosen_deployment_types:
        pytest.skip(f"Deployment {deployment_t} was not activated")
    host_path = os.environ.get("PATH") or ""
    sentinel_path = "/sf-HOST-PATH-sentinel"
    sentinel_host_path = f"{sentinel_path}:{host_path}" if host_path else sentinel_path
    monkeypatch.setenv("PATH", sentinel_host_path)
    (home, tmpdir), stdout = await _run_env_tool(context, deployment_t)
    assert stdout is not None
    env = dict(line.split("=", 1) for line in stdout.splitlines())
    assert env["HOME"] == home
    assert env["TMPDIR"] == tmpdir
    assert env["FOO_A"] == "BAR"
    if deployment_t == "local":
        # PATH may be inherited from the parent process on local executions
        assert env["PATH"] == sentinel_host_path
    else:
        # A container provides its own PATH, so the host sentinel must not appear
        assert sentinel_path not in env["PATH"]


@pytest.mark.asyncio
@pytest.mark.parametrize("deployment_t", DEPLOYMENTS)
async def test_tool_does_not_inherit_parent_environment(
    chosen_deployment_types: MutableSequence[str],
    context: StreamFlowContext,
    deployment_t: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Arbitrary host variables do not leak into the tool."""
    if deployment_t not in chosen_deployment_types:
        pytest.skip(f"Deployment {deployment_t} was not activated")
    monkeypatch.setenv("MY_SF_SENTINEL", "must-not-leak")
    _, stdout = await _run_env_tool(context, deployment_t)
    assert stdout is not None
    assert "MY_SF_SENTINEL" not in stdout
