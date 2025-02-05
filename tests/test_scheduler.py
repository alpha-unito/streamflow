from __future__ import annotations

import asyncio
import os
from collections.abc import MutableSequence
from typing import cast

import pytest
import pytest_asyncio

from streamflow.core.config import BindingConfig
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import (
    DeploymentConfig,
    FilterConfig,
    LocalTarget,
    Target,
)
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.scheduling import Hardware, Storage
from streamflow.core.workflow import Job, Status
from streamflow.cwl.hardware import CWLHardwareRequirement
from streamflow.data import utils
from tests.utils.connector import ParameterizableHardwareConnector
from tests.utils.deployment import (
    get_deployment_config,
    get_docker_deployment_config,
    get_local_deployment_config,
    get_parameterizable_hardware_deployment_config,
    get_service,
)
from tests.utils.workflow import CWL_VERSION, random_job_name


async def _notify_status_and_test(
    context: StreamFlowContext, jobs: Job | MutableSequence[Job], status: Status
) -> None:
    if not isinstance(jobs, MutableSequence):
        jobs = [jobs]
    for j in jobs:
        await context.scheduler.notify_status(j.name, status)
        assert context.scheduler.get_allocation(j.name).status == status


def _prepare_connector(context: StreamFlowContext, num_jobs: int = 1):
    # Inject custom hardware to manipulate available resources
    hardware_requirement = CWLHardwareRequirement(cwl_version=CWL_VERSION)
    conn = cast(
        type[ParameterizableHardwareConnector],
        context.deployment_manager.get_connector("custom-hardware"),
    )
    conn.set_hardware(
        Hardware(
            cores=hardware_requirement.cores * num_jobs,
            memory=hardware_requirement.memory * num_jobs,
            storage={
                os.sep: Storage(
                    os.sep,
                    hardware_requirement.tmpdir * num_jobs
                    + hardware_requirement.outdir * num_jobs,
                )
            },
        )
    )
    param_config = get_parameterizable_hardware_deployment_config()
    return hardware_requirement, Target(
        deployment=param_config,
        service=get_service(context, param_config.type),
        workdir=param_config.workdir,
    )


@pytest_asyncio.fixture(scope="module")
async def deployment_config(context, deployment) -> DeploymentConfig:
    return await get_deployment_config(context, deployment)


@pytest.fixture(scope="module")
def service(context, deployment) -> str | None:
    return get_service(context, deployment)


@pytest.mark.asyncio
async def test_bind_volumes(context: StreamFlowContext):
    """Test the binding of volumes in stacked locations"""
    local_deployment = get_local_deployment_config()
    local_connector = context.deployment_manager.get_connector(local_deployment.name)
    local_location = next(
        iter(
            (
                await local_connector.get_available_locations(
                    get_service(context, local_deployment.type)
                )
            ).values()
        )
    )
    docker_deployment = get_docker_deployment_config()
    docker_connector = context.deployment_manager.get_connector(docker_deployment.name)
    docker_location = next(
        iter(
            (
                await docker_connector.get_available_locations(
                    get_service(context, docker_deployment.type)
                )
            ).values()
        )
    )
    assert not (
        {"/tmp/streamflow", "/home/output"} - docker_location.hardware.storage.keys()
    )
    path = docker_deployment.workdir
    mount_point = await utils.get_mount_point(context, docker_location, path)
    container_hardware = await utils.bind_mount_point(
        context,
        local_connector,
        local_location,
        Hardware(
            cores=float(1),
            memory=float(100),
            storage={
                key: Storage(
                    mount_point=mount_point,
                    size=float(100),
                    paths={path},
                    bind=docker_location.hardware.get_storage(mount_point).bind,
                )
                for key in ["tmpdir", "workdir"]
            },
        ),
    )
    # "/tmp/streamflow", "/home/output" are both mounted to the local workdir. So they collapse to the same mount point
    assert len(container_hardware.storage) == 1 and next(
        iter(container_hardware.storage.values())
    ).mount_point == await utils.get_mount_point(
        context, local_location, local_deployment.workdir
    )
    assert container_hardware <= local_location.hardware


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

    filter_config = FilterConfig(name="reverse-filter", type="reverse", config={})
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
    assert context.scheduler.get_allocation(job.name).status == Status.FIREABLE

    # Check if the job has been scheduled into the second target
    assert (
        context.scheduler.get_allocation(job.name).target.deployment.name
        == docker_config.name
    )

    # Job changes status to RUNNING
    await _notify_status_and_test(context, job, Status.RUNNING)
    # Job changes status to COMPLETED
    await _notify_status_and_test(context, job, Status.COMPLETED)


def test_hardware():
    """Test Hardware arithmetic and comparison operations"""
    main_hardware = Hardware(
        cores=float(2**4),
        memory=float(2**10),
        storage={
            "placeholder_1": Storage(
                mount_point=os.sep,
                size=float(2**20),
            ),
            "placeholder_2": Storage(
                mount_point=os.sep,
                size=float(2**12),
            ),
            "placeholder_3": Storage(
                mount_point=os.path.join(os.sep, "tmp"),
                size=float(2**15),
            ),
        },
    )
    additional_storage_size = float(2**10)
    secondary_hardware = main_hardware + Hardware(
        storage={
            "placeholder_4": Storage(
                mount_point=os.path.join(os.sep, "tmp"), size=additional_storage_size
            )
        }
    )
    # Secondary hardware after the sum operation has the storage keys in the normalized form
    assert secondary_hardware.storage.keys() == {os.sep, os.path.join(os.sep, "tmp")}
    assert (
        secondary_hardware.get_storage(os.path.join(os.sep, "tmp")).size
        == main_hardware.storage["placeholder_3"].size + additional_storage_size
    )

    # Changing storage keys and the number of storages to evaluate whether operations are independent of them.
    # The `placeholder_1` and `placeholder_2` storages collapse into the `other_name_1` storage during the
    # previous addition operation because they share the same mount point.
    secondary_hardware.storage["other_name_1.1"] = secondary_hardware.storage.pop(
        main_hardware.storage["placeholder_1"].mount_point
    )
    secondary_hardware.storage["other_name_2.1"] = secondary_hardware.storage.pop(
        main_hardware.storage["placeholder_3"].mount_point
    )
    assert main_hardware <= secondary_hardware
    assert secondary_hardware >= main_hardware

    # Hardware must have the same `mount_point` to execute comparison operations.
    # The "bigger" hardware must have at least all the mount_point values of the "smaller" one;
    # otherwise, an error must be raised.
    # If this check is not performed, for example, if the two hardware have different
    # `mount_point` values at scheduling time, the scheduling process may enter an infinite loop.
    main_hardware.storage.pop("placeholder_3")
    with pytest.raises(WorkflowExecutionException) as err:
        _ = secondary_hardware <= main_hardware
    assert (
        str(err.value) == f"Invalid `Hardware` comparison: {main_hardware} should "
        f"contain all the storage included in {secondary_hardware}."
    )
    with pytest.raises(WorkflowExecutionException) as err:
        _ = main_hardware >= secondary_hardware
    assert (
        str(err.value) == f"Invalid `Hardware` comparison: {main_hardware} should "
        f"contain all the storage included in {secondary_hardware}."
    )


@pytest.mark.asyncio
async def test_multi_env(context: StreamFlowContext):
    """Test scheduling two jobs on two different environments."""
    hardware_requirement, target = _prepare_connector(context)
    docker_config = get_docker_deployment_config()
    docker_target = Target(
        deployment=docker_config,
        service=get_service(context, docker_config.type),
        workdir=docker_config.workdir,
    )
    jobs = [
        (
            Job(
                name=random_job_name(f"a{i}"),
                workflow_id=0,
                inputs={},
                input_directory=None,
                output_directory=None,
                tmp_directory=None,
            ),
            BindingConfig(targets=[target] if i == 0 else [docker_target]),
        )
        for i in range(2)
    ]
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
        assert context.scheduler.get_allocation(j.name).status == Status.FIREABLE

    # Jobs change status to RUNNING
    await _notify_status_and_test(context, [j for j, _ in jobs], Status.RUNNING)
    # Jobs change status to COMPLETED
    await _notify_status_and_test(context, [j for j, _ in jobs], Status.COMPLETED)


@pytest.mark.asyncio
async def test_multi_targets_one_job(context: StreamFlowContext):
    """Test scheduling one jobs with two targets: Local and Docker Image. The job will be scheduled in the first"""
    hardware_requirement, target = _prepare_connector(context)
    # Create fake job with two targets and schedule it
    job = Job(
        name=random_job_name(),
        workflow_id=0,
        inputs={},
        input_directory=None,
        output_directory=None,
        tmp_directory=None,
    )

    docker_config = get_docker_deployment_config()
    docker_target = Target(
        deployment=docker_config,
        service=get_service(context, docker_config.type),
        workdir=docker_config.workdir,
    )
    binding_config = BindingConfig(targets=[target, docker_target])

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
    assert context.scheduler.get_allocation(job.name).status == Status.FIREABLE

    # Check if it has been scheduled into the first target
    assert (
        context.scheduler.get_allocation(job.name).target.deployment.name
        == target.deployment.name
    )

    # Job changes status to RUNNING
    await _notify_status_and_test(context, job, Status.RUNNING)
    # Job changes status to COMPLETED
    await _notify_status_and_test(context, job, Status.COMPLETED)


@pytest.mark.asyncio
async def test_multi_targets_two_jobs(context: StreamFlowContext):
    """
    Test scheduling two jobs with two same targets: Local and Docker Image.
    The first job will be scheduled in the local target and the second job in the docker target because the local resources will be full.
    """
    hardware_requirement, target = _prepare_connector(context)
    # Create fake jobs with two same targets and schedule them
    jobs = [
        Job(
            name=random_job_name(),
            workflow_id=0,
            inputs={},
            input_directory=None,
            output_directory=None,
            tmp_directory=None,
        )
        for _ in range(2)
    ]
    docker_config = get_docker_deployment_config()
    docker_target = Target(
        deployment=docker_config,
        service=get_service(context, docker_config.type),
        workdir=docker_config.workdir,
    )
    binding_config = BindingConfig(targets=[target, docker_target])

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
        assert context.scheduler.get_allocation(j.name).status == Status.FIREABLE

    # Check if they have been scheduled into the right targets
    assert (
        context.scheduler.get_allocation(jobs[0].name).target.deployment.name
        == target.deployment.name
    )
    assert (
        context.scheduler.get_allocation(jobs[1].name).target.deployment.name
        == docker_config.name
    )

    # Job changes status to RUNNING
    await _notify_status_and_test(context, jobs, Status.RUNNING)
    # Job changes status to COMPLETED
    await _notify_status_and_test(context, jobs, Status.COMPLETED)


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
    hardware_requirement = CWLHardwareRequirement(cwl_version=CWL_VERSION)
    target = Target(deployment=deployment_config, service=service)
    binding_config = BindingConfig(targets=[target])
    await context.scheduler.schedule(job, binding_config, hardware_requirement)
    assert context.scheduler.get_allocation(job.name).status == Status.FIREABLE
    await _notify_status_and_test(context, job, Status.COMPLETED)


@pytest.mark.asyncio
async def test_single_env_enough_resources(context: StreamFlowContext):
    """Test scheduling two jobs on a single environment with resources for all jobs together."""
    num_jobs = 2
    hardware_requirement, target = _prepare_connector(context, num_jobs=num_jobs)
    # Create fake jobs and schedule them
    jobs = [
        Job(
            name=random_job_name(),
            workflow_id=0,
            inputs={},
            input_directory=None,
            output_directory=None,
            tmp_directory=None,
        )
        for _ in range(num_jobs)
    ]

    binding_config = BindingConfig(targets=[target])
    task_pending = [
        asyncio.create_task(
            context.scheduler.schedule(job, binding_config, hardware_requirement)
        )
        for job in jobs
    ]
    assert len(task_pending) == num_jobs

    # Available resources to schedule all the jobs (timeout parameter useful if a deadlock occurs)
    _, task_pending = await asyncio.wait(
        task_pending, return_when=asyncio.ALL_COMPLETED, timeout=60
    )
    assert len(task_pending) == 0
    for j in jobs:
        assert context.scheduler.get_allocation(j.name).status == Status.FIREABLE

    # Jobs change status to RUNNING
    await _notify_status_and_test(context, jobs, Status.RUNNING)
    # Jobs change status to COMPLETED
    await _notify_status_and_test(context, jobs, Status.COMPLETED)


@pytest.mark.asyncio
async def test_single_env_few_resources(context: StreamFlowContext):
    """Test scheduling two jobs on single environment but with resources for one job at a time."""
    hardware_requirement, target = _prepare_connector(context)
    # Create fake jobs and schedule them
    jobs = [
        Job(
            name=random_job_name(),
            workflow_id=0,
            inputs={},
            input_directory=None,
            output_directory=None,
            tmp_directory=None,
        )
        for _ in range(2)
    ]

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
    assert context.scheduler.get_allocation(jobs[0].name).status == Status.FIREABLE
    assert context.scheduler.get_allocation(jobs[1].name) is None

    # First job changes status to RUNNING and continue to keep all resources
    # Testing that second job is not scheduled (timeout parameter necessary)
    await context.scheduler.notify_status(jobs[0].name, Status.RUNNING)
    _, task_pending = await asyncio.wait(task_pending, timeout=2)

    assert len(task_pending) == 1
    assert context.scheduler.get_allocation(jobs[0].name).status == Status.RUNNING
    assert context.scheduler.get_allocation(jobs[1].name) is None

    # First job completes and the second job can be scheduled (timeout parameter useful if a deadlock occurs)
    await context.scheduler.notify_status(jobs[0].name, Status.COMPLETED)
    _, task_pending = await asyncio.wait(
        task_pending, return_when=asyncio.ALL_COMPLETED, timeout=60
    )
    assert len(task_pending) == 0
    assert context.scheduler.get_allocation(jobs[0].name).status == Status.COMPLETED
    assert context.scheduler.get_allocation(jobs[1].name).status == Status.FIREABLE

    # Second job completed
    await _notify_status_and_test(context, jobs[1], Status.RUNNING)
    await _notify_status_and_test(context, jobs[1], Status.COMPLETED)
