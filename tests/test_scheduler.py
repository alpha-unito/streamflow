from __future__ import annotations

import asyncio
from typing import cast

import pytest
import pytest_asyncio

from streamflow.core.config import BindingConfig, Config
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import (
    DeploymentConfig,
    LocalTarget,
    Target,
)
from streamflow.core.scheduling import Hardware
from streamflow.core.workflow import Job, Status
from tests.utils.connector import ParameterizableHardwareConnector
from tests.utils.deployment import (
    get_docker_deployment_config,
    get_service,
    get_deployment_config,
    get_parameterizable_hardware_deployment_config,
)
from tests.utils.workflow import random_job_name


@pytest_asyncio.fixture(scope="module")
async def deployment_config(context, deployment) -> DeploymentConfig:
    return await get_deployment_config(context, deployment)


@pytest_asyncio.fixture(scope="module")
async def service(context, deployment) -> str | None:
    return get_service(context, deployment)


@pytest.mark.asyncio
async def test_scheduling(
    context: StreamFlowContext,
    deployment_config: DeploymentConfig,
    service: str | None,
):
    """Test scheduling a job on a remote environment."""

    job = Job(
        name=random_job_name(),
        workflow_id=0,
        inputs={},
        input_directory=deployment_config.workdir,
        output_directory=deployment_config.workdir,
        tmp_directory=deployment_config.workdir,
    )
    hardware_requirement = Hardware(cores=1)
    target = Target(
        deployment=deployment_config,
        service=service,
    )
    binding_config = BindingConfig(targets=[target])
    await context.scheduler.schedule(job, binding_config, hardware_requirement)
    assert context.scheduler.job_allocations[job.name].status == Status.FIREABLE
    await context.scheduler.notify_status(job.name, Status.COMPLETED)
    assert context.scheduler.job_allocations[job.name].status == Status.COMPLETED


@pytest.mark.asyncio
async def test_single_env_few_resources(context: StreamFlowContext):
    """Test scheduling two jobs on single environment but with resources for one job at a time."""

    # Inject custom hardware to manipulate available resources
    machine_hardware = Hardware(
        cores=1,
        memory=100,
        input_directory=100,
        output_directory=100,
        tmp_directory=100,
    )
    deployment_config = get_parameterizable_hardware_deployment_config()
    conn = cast(
        ParameterizableHardwareConnector,
        context.deployment_manager.get_connector("custom-hardware"),
    )
    conn.set_hardware(machine_hardware)

    # Create fake jobs and schedule them
    jobs = []
    for _ in range(2):
        jobs.append(
            Job(
                name=random_job_name(),
                workflow_id=0,
                inputs={},
                input_directory=None,
                output_directory=None,
                tmp_directory=None,
            )
        )
    hardware_requirement = Hardware(cores=1)
    target = Target(
        deployment=deployment_config,
        service=get_service(context, deployment_config.type),
        workdir=deployment_config.workdir,
    )
    binding_config = BindingConfig(targets=[target])
    task_pending = [
        asyncio.create_task(
            context.scheduler.schedule(job, binding_config, hardware_requirement)
        )
        for job in jobs
    ]
    assert len(task_pending) == 2

    # Available resources to schedule only one job (timeout parameter useful if a deadlock occurs)
    _, task_pending = await asyncio.wait(
        task_pending, return_when=asyncio.FIRST_COMPLETED, timeout=60
    )
    assert len(task_pending) == 1
    assert context.scheduler.job_allocations[jobs[0].name].status == Status.FIREABLE

    # First job changes status to RUNNING and continue to keep all resources
    # Testing that second job is not scheduled (timeout parameter necessary)
    await context.scheduler.notify_status(jobs[0].name, Status.RUNNING)
    _, task_pending = await asyncio.wait(task_pending, timeout=2)
    assert len(task_pending) == 1
    assert context.scheduler.job_allocations[jobs[0].name].status == Status.RUNNING

    # First job completes and the second job can be scheduled (timeout parameter useful if a deadlock occurs)
    await context.scheduler.notify_status(jobs[0].name, Status.COMPLETED)
    _, task_pending = await asyncio.wait(
        task_pending, return_when=asyncio.ALL_COMPLETED, timeout=60
    )
    assert len(task_pending) == 0
    assert context.scheduler.job_allocations[jobs[0].name].status == Status.COMPLETED
    assert context.scheduler.job_allocations[jobs[1].name].status == Status.FIREABLE

    # Second job completed
    await context.scheduler.notify_status(jobs[1].name, Status.RUNNING)
    assert context.scheduler.job_allocations[jobs[1].name].status == Status.RUNNING
    await context.scheduler.notify_status(jobs[1].name, Status.COMPLETED)
    assert context.scheduler.job_allocations[jobs[1].name].status == Status.COMPLETED


@pytest.mark.asyncio
async def test_single_env_enough_resources(context: StreamFlowContext):
    """Test scheduling two jobs on a single environment with resources for all jobs together."""

    # Inject custom hardware to manipulate available resources
    machine_hardware = Hardware(
        cores=2,
        memory=100,
        input_directory=100,
        output_directory=100,
        tmp_directory=100,
    )

    deployment_config = get_parameterizable_hardware_deployment_config()
    conn = cast(
        ParameterizableHardwareConnector,
        context.deployment_manager.get_connector("custom-hardware"),
    )
    conn.set_hardware(machine_hardware)

    # Create fake jobs and schedule them
    jobs = []
    for _ in range(2):
        jobs.append(
            Job(
                name=random_job_name(),
                workflow_id=0,
                inputs={},
                input_directory=None,
                output_directory=None,
                tmp_directory=None,
            )
        )
    hardware_requirement = Hardware(cores=1)
    target = Target(
        deployment=deployment_config,
        service=get_service(context, deployment_config.type),
        workdir=deployment_config.workdir,
    )
    binding_config = BindingConfig(targets=[target])
    task_pending = [
        asyncio.create_task(
            context.scheduler.schedule(job, binding_config, hardware_requirement)
        )
        for job in jobs
    ]
    assert len(task_pending) == 2

    # Available resources to schedule all the jobs (timeout parameter useful if a deadlock occurs)
    _, task_pending = await asyncio.wait(
        task_pending, return_when=asyncio.ALL_COMPLETED, timeout=60
    )
    assert len(task_pending) == 0
    for j in jobs:
        assert context.scheduler.job_allocations[j.name].status == Status.FIREABLE

    # Jobs change status to RUNNING
    for j in jobs:
        await context.scheduler.notify_status(j.name, Status.RUNNING)
        assert context.scheduler.job_allocations[j.name].status == Status.RUNNING

    # Jobs change status to COMPLETED
    for j in jobs:
        await context.scheduler.notify_status(j.name, Status.COMPLETED)
        assert context.scheduler.job_allocations[j.name].status == Status.COMPLETED


@pytest.mark.asyncio
async def test_multi_env(context: StreamFlowContext):
    """Test scheduling two jobs on two different environments."""

    # Inject custom hardware to manipulate available resources
    machine_hardware = Hardware(cores=1)
    param_config = get_parameterizable_hardware_deployment_config()
    conn = cast(
        ParameterizableHardwareConnector,
        context.deployment_manager.get_connector("custom-hardware"),
    )
    conn.set_hardware(machine_hardware)

    # Create fake jobs with two different env and schedule them
    jobs = []
    target = Target(
        deployment=param_config,
        service=get_service(context, param_config.type),
        workdir=param_config.workdir,
    )
    docker_config = get_docker_deployment_config()
    docker_target = Target(
        deployment=docker_config,
        service=get_service(context, docker_config.type),
        workdir=docker_config.workdir,
    )
    for i in range(2):
        jobs.append(
            (
                Job(
                    name=random_job_name(),
                    workflow_id=0,
                    inputs={},
                    input_directory=None,
                    output_directory=None,
                    tmp_directory=None,
                ),
                BindingConfig(targets=[target] if i == 0 else [docker_target]),
            )
        )
    hardware_requirement = Hardware(cores=1)
    task_pending = [
        asyncio.create_task(
            context.scheduler.schedule(job, binding_config, hardware_requirement)
        )
        for job, binding_config in jobs
    ]
    assert len(task_pending) == 2

    # Available resources to schedule all the jobs (timeout parameter useful if a deadlock occurs)
    _, task_pending = await asyncio.wait(
        task_pending, return_when=asyncio.ALL_COMPLETED, timeout=60
    )
    assert len(task_pending) == 0
    for j, _ in jobs:
        assert context.scheduler.job_allocations[j.name].status == Status.FIREABLE

    # Jobs change status to RUNNING
    for j, _ in jobs:
        await context.scheduler.notify_status(j.name, Status.RUNNING)
        assert context.scheduler.job_allocations[j.name].status == Status.RUNNING

    # Jobs change status to COMPLETED
    for j, _ in jobs:
        await context.scheduler.notify_status(j.name, Status.COMPLETED)
        assert context.scheduler.job_allocations[j.name].status == Status.COMPLETED


@pytest.mark.asyncio
async def test_multi_targets_one_job(context: StreamFlowContext):
    """Test scheduling one jobs with two targets: Local and Docker Image. The job will be scheduled in the first"""

    # Inject custom hardware to manipulate available resources
    machine_hardware = Hardware(cores=1)
    param_config = get_parameterizable_hardware_deployment_config()
    conn = cast(
        ParameterizableHardwareConnector,
        context.deployment_manager.get_connector("custom-hardware"),
    )
    conn.set_hardware(machine_hardware)

    # Create fake job with two targets and schedule it
    job = Job(
        name=random_job_name(),
        workflow_id=0,
        inputs={},
        input_directory=None,
        output_directory=None,
        tmp_directory=None,
    )

    target = Target(
        deployment=param_config,
        service=get_service(context, param_config.type),
        workdir=param_config.workdir,
    )
    docker_config = get_docker_deployment_config()
    docker_target = Target(
        deployment=docker_config,
        service=get_service(context, docker_config.type),
        workdir=docker_config.workdir,
    )
    binding_config = BindingConfig(targets=[target, docker_target])

    hardware_requirement = Hardware(cores=1)
    task_pending = [
        asyncio.create_task(
            context.scheduler.schedule(job, binding_config, hardware_requirement)
        )
    ]
    assert len(task_pending) == 1

    # Available resources to schedule the job on the first target (timeout parameter useful if a deadlock occurs)
    _, task_pending = await asyncio.wait(
        task_pending, return_when=asyncio.FIRST_COMPLETED, timeout=60
    )
    assert len(task_pending) == 0
    assert context.scheduler.job_allocations[job.name].status == Status.FIREABLE

    # Check if it has been scheduled into the first target
    assert (
        context.scheduler.job_allocations[job.name].target.deployment.name
        == param_config.name
    )

    # Job changes status to RUNNING
    await context.scheduler.notify_status(job.name, Status.RUNNING)
    assert context.scheduler.job_allocations[job.name].status == Status.RUNNING

    # Job changes status to COMPLETED
    await context.scheduler.notify_status(job.name, Status.COMPLETED)
    assert context.scheduler.job_allocations[job.name].status == Status.COMPLETED


@pytest.mark.asyncio
async def test_multi_targets_two_jobs(context: StreamFlowContext):
    """
    Test scheduling two jobs with two same targets: Local and Docker Image.
    The first job will be scheduled in the local target and the second job in the docker target because the local resources will be full.
    """

    # Inject custom connector to manipulate available resources
    machine_hardware = Hardware(cores=1)
    param_config = get_parameterizable_hardware_deployment_config()
    conn = cast(
        ParameterizableHardwareConnector,
        context.deployment_manager.get_connector("custom-hardware"),
    )
    conn.set_hardware(machine_hardware)

    # Create fake jobs with two same targets and schedule them
    jobs = []
    for _ in range(2):
        jobs.append(
            Job(
                name=random_job_name(),
                workflow_id=0,
                inputs={},
                input_directory=None,
                output_directory=None,
                tmp_directory=None,
            )
        )
    target = Target(
        deployment=param_config,
        service=get_service(context, param_config.type),
        workdir=param_config.workdir,
    )
    docker_config = get_docker_deployment_config()
    docker_target = Target(
        deployment=docker_config,
        service=get_service(context, docker_config.type),
        workdir=docker_config.workdir,
    )
    binding_config = BindingConfig(targets=[target, docker_target])

    hardware_requirement = Hardware(cores=1)
    task_pending = [
        asyncio.create_task(
            context.scheduler.schedule(job, binding_config, hardware_requirement)
        )
        for job in jobs
    ]
    assert len(task_pending) == 2

    # Available resources to schedule the jobs on the two targets (timeout parameter useful if a deadlock occurs)
    _, task_pending = await asyncio.wait(
        task_pending, return_when=asyncio.ALL_COMPLETED, timeout=60
    )
    assert len(task_pending) == 0
    for j in jobs:
        assert context.scheduler.job_allocations[j.name].status == Status.FIREABLE

    # Check if they have been scheduled into the right targets
    assert (
        context.scheduler.job_allocations[jobs[0].name].target.deployment.name
        == param_config.name
    )
    assert (
        context.scheduler.job_allocations[jobs[1].name].target.deployment.name
        == docker_config.name
    )

    # Jobs change status to RUNNING
    for j in jobs:
        await context.scheduler.notify_status(j.name, Status.RUNNING)
        assert context.scheduler.job_allocations[j.name].status == Status.RUNNING

    # Jobs change status to COMPLETED
    for j in jobs:
        await context.scheduler.notify_status(j.name, Status.COMPLETED)
        assert context.scheduler.job_allocations[j.name].status == Status.COMPLETED


@pytest.mark.asyncio
async def test_binding_filter(context: StreamFlowContext):
    """Test Binding Filter using a job with two targets both free. With the CustomBindingFilter the scheduling will choose the second target"""
    job = Job(
        name=random_job_name(),
        workflow_id=0,
        inputs={},
        input_directory=None,
        output_directory=None,
        tmp_directory=None,
    )
    local_target = LocalTarget()
    docker_config = get_docker_deployment_config()
    docker_target = Target(
        deployment=docker_config,
        service=get_service(context, docker_config.type),
        workdir=docker_config.workdir,
    )

    filter_config = Config(name="reverse-filter", type="reverse", config={})
    binding_config = BindingConfig(
        targets=[local_target, docker_target], filters=[filter_config]
    )

    # Schedule the job
    task_pending = [
        asyncio.create_task(context.scheduler.schedule(job, binding_config, None))
    ]
    assert len(task_pending) == 1

    # Both targets are available for scheduling (timeout parameter useful if a deadlock occurs)
    _, task_pending = await asyncio.wait(
        task_pending, return_when=asyncio.FIRST_COMPLETED, timeout=60
    )
    assert len(task_pending) == 0
    assert context.scheduler.job_allocations[job.name].status == Status.FIREABLE

    # Check if the job has been scheduled into the second target
    assert (
        context.scheduler.job_allocations[job.name].target.deployment.name
        == docker_config.name
    )

    # Job changes status to RUNNING
    await context.scheduler.notify_status(job.name, Status.RUNNING)
    assert context.scheduler.job_allocations[job.name].status == Status.RUNNING

    # Job changes status to COMPLETED
    await context.scheduler.notify_status(job.name, Status.COMPLETED)
    assert context.scheduler.job_allocations[job.name].status == Status.COMPLETED
