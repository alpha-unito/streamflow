import asyncio
from typing import MutableMapping, MutableSequence, Optional

import pytest

from streamflow.core import utils
from streamflow.core.config import BindingConfig, Config
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import (
    BindingFilter,
    LOCAL_LOCATION,
    LocalTarget,
    Target,
)
from streamflow.core.scheduling import AvailableLocation, Hardware
from streamflow.core.workflow import Job, Status
from streamflow.deployment.connector import LocalConnector
from tests.conftest import get_docker_deployment_config


class CustomConnector(LocalConnector):
    def __init__(
        self,
        deployment_name: str,
        config_dir: str,
        hardware: Hardware,
        transferBufferSize: int = 2**16,
    ):
        super().__init__(deployment_name, config_dir, transferBufferSize)
        self.hardware = hardware

    async def get_available_locations(
        self,
        service: Optional[str] = None,
        input_directory: Optional[str] = None,
        output_directory: Optional[str] = None,
        tmp_directory: Optional[str] = None,
    ) -> MutableMapping[str, AvailableLocation]:
        return {
            LOCAL_LOCATION: AvailableLocation(
                name=LOCAL_LOCATION,
                deployment=self.deployment_name,
                service=service,
                hostname="localhost",
                slots=1,
                hardware=self.hardware,
            )
        }


class CustomBindingFilter(BindingFilter):
    async def get_targets(
        self, job: Job, targets: MutableSequence[Target]
    ) -> MutableSequence[Target]:
        return targets[::-1]

    @classmethod
    def get_schema(cls) -> str:
        return ""


@pytest.mark.asyncio
async def test_single_env_few_resources(context: StreamFlowContext):
    """Test scheduling two jobs on single environment but with resources for one job at a time."""

    machine_hardware = Hardware(
        cores=1,
        memory=100,
        input_directory=100,
        output_directory=100,
        tmp_directory=100,
    )

    # inject custom connector to manipulate available resources
    conn = context.deployment_manager.get_connector(LOCAL_LOCATION)
    context.deployment_manager.deployments_map[LOCAL_LOCATION] = CustomConnector(
        deployment_name=conn.deployment_name,
        config_dir=conn.config_dir,
        transferBufferSize=conn.transferBufferSize,
        hardware=machine_hardware,
    )

    # Create fake jobs and schedule them
    jobs = []
    for _ in range(2):
        jobs.append(
            Job(
                name=utils.random_name(),
                workflow_id=0,
                inputs={},
                input_directory=utils.random_name(),
                output_directory=utils.random_name(),
                tmp_directory=utils.random_name(),
            )
        )
    hardware_requirement = Hardware(cores=1)
    local_target = LocalTarget()
    binding_config = BindingConfig(targets=[local_target])
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

    machine_hardware = Hardware(
        cores=2,
        memory=100,
        input_directory=100,
        output_directory=100,
        tmp_directory=100,
    )

    # Inject custom connector to manipulate available resources
    conn = context.deployment_manager.get_connector(LOCAL_LOCATION)
    context.deployment_manager.deployments_map[LOCAL_LOCATION] = CustomConnector(
        deployment_name=conn.deployment_name,
        config_dir=conn.config_dir,
        transferBufferSize=conn.transferBufferSize,
        hardware=machine_hardware,
    )

    # Create fake jobs and schedule them
    jobs = []
    for _ in range(2):
        jobs.append(
            Job(
                name=utils.random_name(),
                workflow_id=0,
                inputs={},
                input_directory=utils.random_name(),
                output_directory=utils.random_name(),
                tmp_directory=utils.random_name(),
            )
        )
    hardware_requirement = Hardware(cores=1)
    local_target = LocalTarget()
    binding_config = BindingConfig(targets=[local_target])
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

    # Inject custom connector to manipulate available resources
    machine_hardware = Hardware(cores=1)
    conn = context.deployment_manager.get_connector(LOCAL_LOCATION)
    context.deployment_manager.deployments_map[LOCAL_LOCATION] = CustomConnector(
        deployment_name=conn.deployment_name,
        config_dir=conn.config_dir,
        transferBufferSize=conn.transferBufferSize,
        hardware=machine_hardware,
    )

    # Create fake jobs with two different env and schedule them
    jobs = []
    local_target = LocalTarget()
    docker_target = Target(
        deployment=get_docker_deployment_config(),
        service="test-multi-env",
        workdir=utils.random_name(),
    )
    for i in range(2):
        jobs.append(
            (
                Job(
                    name=utils.random_name(),
                    workflow_id=0,
                    inputs={},
                    input_directory=utils.random_name(),
                    output_directory=utils.random_name(),
                    tmp_directory=utils.random_name(),
                ),
                BindingConfig(targets=[local_target] if i == 0 else [docker_target]),
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

    # Inject custom connector to manipulate available resources
    machine_hardware = Hardware(cores=1)
    conn = context.deployment_manager.get_connector(LOCAL_LOCATION)
    context.deployment_manager.deployments_map[LOCAL_LOCATION] = CustomConnector(
        deployment_name=conn.deployment_name,
        config_dir=conn.config_dir,
        transferBufferSize=conn.transferBufferSize,
        hardware=machine_hardware,
    )

    # Create fake job with two targets and schedule it
    job = Job(
        name=utils.random_name(),
        workflow_id=0,
        inputs={},
        input_directory=utils.random_name(),
        output_directory=utils.random_name(),
        tmp_directory=utils.random_name(),
    )
    local_target = LocalTarget()
    docker_target = Target(
        deployment=get_docker_deployment_config(),
        service="test-multi-targ-1",
        workdir=utils.random_name(),
    )
    binding_config = BindingConfig(targets=[local_target, docker_target])

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
        == LOCAL_LOCATION
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
    conn = context.deployment_manager.get_connector(LOCAL_LOCATION)
    context.deployment_manager.deployments_map[LOCAL_LOCATION] = CustomConnector(
        deployment_name=conn.deployment_name,
        config_dir=conn.config_dir,
        transferBufferSize=conn.transferBufferSize,
        hardware=machine_hardware,
    )

    # Create fake jobs with two same targets and schedule them
    jobs = []
    for _ in range(2):
        jobs.append(
            Job(
                name=utils.random_name(),
                workflow_id=0,
                inputs={},
                input_directory=utils.random_name(),
                output_directory=utils.random_name(),
                tmp_directory=utils.random_name(),
            )
        )
    local_target = LocalTarget()
    docker_target = Target(
        deployment=get_docker_deployment_config(),
        service="test-multi-targ-2",
        workdir=utils.random_name(),
    )
    binding_config = BindingConfig(targets=[local_target, docker_target])

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
        == LOCAL_LOCATION
    )
    assert (
        context.scheduler.job_allocations[jobs[1].name].target.deployment.name
        == get_docker_deployment_config().name
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
        name=utils.random_name(),
        workflow_id=0,
        inputs={},
        input_directory=utils.random_name(),
        output_directory=utils.random_name(),
        tmp_directory=utils.random_name(),
    )
    local_target = LocalTarget()
    docker_target = Target(
        deployment=get_docker_deployment_config(),
        service="test-binding-target",
        workdir=utils.random_name(),
    )

    # Inject custom filter that returns the targets list backwards
    filter_config_name = "custom"
    filter_config = Config(name=filter_config_name, type="shuffle", config={})
    binding_config = BindingConfig(
        targets=[local_target, docker_target], filters=[filter_config]
    )
    context.scheduler.binding_filter_map[filter_config_name] = CustomBindingFilter()

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
        == get_docker_deployment_config().name
    )

    # Job changes status to RUNNING
    await context.scheduler.notify_status(job.name, Status.RUNNING)
    assert context.scheduler.job_allocations[job.name].status == Status.RUNNING

    # Job changes status to COMPLETED
    await context.scheduler.notify_status(job.name, Status.COMPLETED)
    assert context.scheduler.job_allocations[job.name].status == Status.COMPLETED
