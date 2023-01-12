import pytest
import asyncio

from typing import MutableMapping, Optional
from tests.conftest import get_docker_deploy_config

from streamflow.core import utils
from streamflow.core.workflow import Job, Status
from streamflow.core.config import BindingConfig
from streamflow.core.scheduling import AvailableLocation, Hardware
from streamflow.core.deployment import (
    LOCAL_LOCATION,
    LocalTarget,
    Target,
)

from streamflow.deployment.connector import LocalConnector

from streamflow.core.context import StreamFlowContext


class InjectedConnector(LocalConnector):
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

    # inject custom connector to manipolate available resources
    conn = context.deployment_manager.get_connector(LOCAL_LOCATION)
    context.deployment_manager.deployments_map[LOCAL_LOCATION] = InjectedConnector(
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
                inputs={},
                input_directory=utils.random_name(),
                output_directory=utils.random_name(),
                tmp_directory=utils.random_name(),
            )
        )
    hardware_requirement = Hardware(cores=1)
    step_target = LocalTarget()
    binding_config = BindingConfig(targets=[step_target])
    task_pending = [
        asyncio.create_task(
            context.scheduler.schedule(job, binding_config, hardware_requirement)
        )
        for job in jobs
    ]
    assert len(task_pending) == 2

    # Available resources to schedule only one job
    _, task_pending = await asyncio.wait(
        task_pending, return_when=asyncio.FIRST_COMPLETED
    )
    assert len(task_pending) == 1
    assert context.scheduler.job_allocations[jobs[0].name].status == Status.FIREABLE

    # First job change status in RUNNING and continue to keep all resources
    # Testing that second job is not scheduled
    await context.scheduler.notify_status(jobs[0].name, Status.RUNNING)
    _, task_pending = await asyncio.wait(task_pending, timeout=2)
    assert len(task_pending) == 1
    assert context.scheduler.job_allocations[jobs[0].name].status == Status.RUNNING

    # First job complete and the second job can be schedulated
    await context.scheduler.notify_status(jobs[0].name, Status.COMPLETED)
    _, task_pending = await asyncio.wait(
        task_pending, return_when=asyncio.ALL_COMPLETED
    )
    assert len(task_pending) == 0
    assert context.scheduler.job_allocations[jobs[0].name].status == Status.COMPLETED
    assert context.scheduler.job_allocations[jobs[1].name].status == Status.FIREABLE

    # Second job complete
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

    # Inject custom connector to manipolate available resources
    conn = context.deployment_manager.get_connector(LOCAL_LOCATION)
    context.deployment_manager.deployments_map[LOCAL_LOCATION] = InjectedConnector(
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
                inputs={},
                input_directory=utils.random_name(),
                output_directory=utils.random_name(),
                tmp_directory=utils.random_name(),
            )
        )
    hardware_requirement = Hardware(cores=1)
    step_target = LocalTarget()
    binding_config = BindingConfig(targets=[step_target])
    task_pending = [
        asyncio.create_task(
            context.scheduler.schedule(job, binding_config, hardware_requirement)
        )
        for job in jobs
    ]
    assert len(task_pending) == 2

    # Available resources to schedule all the jobs
    _, task_pending = await asyncio.wait(
        task_pending, return_when=asyncio.ALL_COMPLETED
    )
    assert len(task_pending) == 0
    for j in jobs:
        assert context.scheduler.job_allocations[j.name].status == Status.FIREABLE

    # Jobs change their status in RUNNING
    for j in jobs:
        await context.scheduler.notify_status(j.name, Status.RUNNING)
        assert context.scheduler.job_allocations[j.name].status == Status.RUNNING

    # Jobs change their status in COMPLETED
    for j in jobs:
        await context.scheduler.notify_status(j.name, Status.COMPLETED)
        assert context.scheduler.job_allocations[j.name].status == Status.COMPLETED


@pytest.mark.asyncio
async def test_multi_env(context: StreamFlowContext):
    """Test scheduling two jobs on two different environments."""

    # Inject custom connector to manipolate available resources
    machine_hardware = Hardware(cores=1)
    conn = context.deployment_manager.get_connector(LOCAL_LOCATION)
    context.deployment_manager.deployments_map[LOCAL_LOCATION] = InjectedConnector(
        deployment_name=conn.deployment_name,
        config_dir=conn.config_dir,
        transferBufferSize=conn.transferBufferSize,
        hardware=machine_hardware,
    )

    # Create fake jobs with two different env and schedule them
    jobs = []
    local_target = LocalTarget()
    docker_target = Target(
        deployment=get_docker_deploy_config(),
        service="test-multi-env",
        workdir=utils.random_name(),
    )
    for i in range(2):
        jobs.append(
            (
                Job(
                    name=utils.random_name(),
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

    # Available resources to schedule all the jobs
    _, task_pending = await asyncio.wait(
        task_pending, return_when=asyncio.ALL_COMPLETED
    )
    assert len(task_pending) == 0
    for j, _ in jobs:
        assert context.scheduler.job_allocations[j.name].status == Status.FIREABLE

    # Jobs change their status in RUNNING
    for j, _ in jobs:
        await context.scheduler.notify_status(j.name, Status.RUNNING)
        assert context.scheduler.job_allocations[j.name].status == Status.RUNNING

    # Jobs change their status in COMPLETED
    for j, _ in jobs:
        await context.scheduler.notify_status(j.name, Status.COMPLETED)
        assert context.scheduler.job_allocations[j.name].status == Status.COMPLETED
