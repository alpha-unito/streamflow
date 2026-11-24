from __future__ import annotations

import asyncio
import os
from collections.abc import Callable, MutableSequence
from typing import NamedTuple, cast

import pytest
import pytest_asyncio

from streamflow.core.config import BindingConfig
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import (
    DeploymentConfig,
    ExecutionLocation,
    FilterConfig,
    LocalTarget,
    Target,
    WrapsConfig,
)
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.scheduling import Hardware, Storage
from streamflow.core.utils import get_job_step_name, random_name
from streamflow.core.workflow import Job, Status
from streamflow.cwl.hardware import CWLHardwareRequirement
from streamflow.data import utils
from streamflow.data.remotepath import StreamFlowPath
from streamflow.deployment.connector import DockerConnector, SingularityConnector
from tests.utils.connector import ParameterizableHardwareConnector
from tests.utils.deployment import (
    get_deployment_config,
    get_docker_deployment_config,
    get_local_deployment_config,
    get_parameterizable_hardware_deployment_config,
    get_service,
)
from tests.utils.utils import InjectPlugin
from tests.utils.workflow import CWL_VERSION, random_job_name


class _HardwareTestContext(NamedTuple):
    binding_config: BindingConfig
    deployment_name: str
    exec_location: ExecutionLocation
    jobs: MutableSequence[Job]
    requirement: CWLHardwareRequirement


async def _cleanup_hardware_test(
    context: StreamFlowContext,
    deployment_name: str,
    jobs: MutableSequence[Job],
    paths: MutableSequence[StreamFlowPath | None] | None = None,
) -> None:
    for j in jobs:
        try:
            if context.scheduler.get_allocation(j.name).status != Status.COMPLETED:
                await context.scheduler.notify_status(j.name, Status.COMPLETED)
        except WorkflowExecutionException:
            pass
    await context.scheduler.context.deployment_manager.undeploy(deployment_name)
    if paths is not None:
        await asyncio.gather(
            *(asyncio.create_task(path.rmtree()) for path in paths if path is not None)
        )


def _get_scheduled_job(
    context: StreamFlowContext, jobs: MutableSequence[Job]
) -> Job | None:
    for job in jobs:
        try:
            context.scheduler.get_allocation(job.name)
            return job
        except WorkflowExecutionException:
            continue
    return None


async def _notify_status_and_test(
    context: StreamFlowContext, jobs: Job | MutableSequence[Job], status: Status
) -> None:
    if not isinstance(jobs, MutableSequence):
        jobs = [jobs]
    for j in jobs:
        await context.scheduler.notify_status(j.name, status)
        assert context.scheduler.get_allocation(j.name).status == status


def _prepare_connector(
    context: StreamFlowContext,
    deployment_name: str,
    location_memory: Callable[[float], float] | None = None,
    num_jobs: int = 1,
) -> tuple[CWLHardwareRequirement, Target]:
    # Inject custom hardware to manipulate available resources
    hardware_requirement = CWLHardwareRequirement(cwl_version=CWL_VERSION)
    conn = cast(
        ParameterizableHardwareConnector,
        context.deployment_manager.get_connector(deployment_name),
    )
    conn.set_hardware(
        hardware=Hardware(
            cores=float(hardware_requirement.cores * num_jobs),
            memory=float(
                location_memory(hardware_requirement.memory)
                if location_memory
                else hardware_requirement.memory * num_jobs
            ),
            storage={
                os.sep: Storage(
                    os.sep,
                    float(hardware_requirement.tmpdir * num_jobs)
                    + float(hardware_requirement.outdir * num_jobs),
                )
            },
        )
    )
    param_config = get_parameterizable_hardware_deployment_config(name=deployment_name)
    return hardware_requirement, Target(
        deployment=param_config,
        service=get_service(context, param_config.type),
        workdir=param_config.workdir,
    )


async def _setup_hardware_test(
    context: StreamFlowContext,
    num_jobs: int,
    requirement: CWLHardwareRequirement,
    memory: float,
    storage: float,
    cores: float | None = None,
    job_dirs: MutableSequence[tuple[str | None, str | None, str | None]] | None = None,
    inmemory: bool = False,
) -> _HardwareTestContext:
    with InjectPlugin(plugin_name="parameterizable-hardware"):
        config = get_parameterizable_hardware_deployment_config(name=random_name())
        await context.deployment_manager.deploy(config)
        conn = cast(
            ParameterizableHardwareConnector,
            context.deployment_manager.get_connector(config.name),
        )
        # The allocated in-memory storage must be smaller than the total available memory
        assert not inmemory or storage < memory
        disk = Storage(mount_point=os.sep, size=storage, in_memory=inmemory)
        conn.set_hardware(
            hardware=Hardware(
                cores=cores if cores is not None else float(requirement.cores),
                memory=memory,
                storage={os.sep: disk},
            )
        )
    param_config = get_parameterizable_hardware_deployment_config(name=config.name)
    target = Target(
        deployment=param_config,
        service=get_service(context, param_config.type),
        workdir=param_config.workdir,
    )
    exec_location = next(
        iter(
            (
                await conn.get_available_locations(
                    service=get_service(context, param_config.type)
                )
            ).values()
        )
    ).location
    jobs = []
    for i in range(num_jobs):
        job_name = random_job_name()
        if job_dirs is not None and i < len(job_dirs):
            in_dir, out_dir, tmp_dir = job_dirs[i]
        else:
            for d in ("input", "output", "tmp"):
                await StreamFlowPath(
                    param_config.workdir,
                    f"{get_job_step_name(job_name).lstrip(os.sep)}-{d}-{i}",
                    context=context,
                    location=exec_location,
                ).mkdir(parents=True, exist_ok=True)
            in_dir = os.path.join(
                param_config.workdir,
                f"{get_job_step_name(job_name).lstrip(os.sep)}-input-{i}",
            )
            out_dir = os.path.join(
                param_config.workdir,
                f"{get_job_step_name(job_name).lstrip(os.sep)}-output-{i}",
            )
            tmp_dir = os.path.join(
                param_config.workdir,
                f"{get_job_step_name(job_name).lstrip(os.sep)}-tmp-{i}",
            )
        jobs.append(
            Job(
                name=job_name,
                workflow_id=0,
                inputs={},
                input_directory=in_dir,
                output_directory=out_dir,
                tmp_directory=tmp_dir,
            )
        )
    return _HardwareTestContext(
        binding_config=BindingConfig(targets=[target]),
        deployment_name=config.name,
        exec_location=exec_location,
        jobs=jobs,
        requirement=requirement,
    )


def _get_stacked_test_config(
    name: str,
    inner_ctx: _HardwareTestContext,
    locations: int,
    outer_hardware: Hardware | None = None,
) -> DeploymentConfig:
    inner_target = inner_ctx.binding_config.targets[0]
    config: dict[str, int | float | bool | str | list[str] | None] = {
        "locations": locations
    }
    if outer_hardware is not None:
        storage = outer_hardware.storage[os.sep]
        config.update(
            {
                "outer_cores": outer_hardware.cores,
                "outer_memory": outer_hardware.memory,
                "outer_storage_size": storage.size,
                "outer_storage_paths": sorted(storage.paths),
                "outer_storage_in_memory": storage.in_memory,
            }
        )
        if storage.bind is not None:
            config["outer_bind"] = storage.bind
    return DeploymentConfig(
        name=name,
        type="stacked-test",
        config=config,
        external=False,
        lazy=False,
        workdir=inner_target.workdir,
        wraps=WrapsConfig(deployment=inner_ctx.deployment_name),
    )


async def _prepare_stacked_jobs(
    context: StreamFlowContext,
    jobs: MutableSequence[Job],
    location: ExecutionLocation,
    workdir: str,
) -> None:
    for index, job in enumerate(jobs):
        input_directory = os.path.join(workdir, f"input-{index}")
        output_directory = os.path.join(workdir, f"output-{index}")
        tmp_directory = os.path.join(workdir, f"tmp-{index}")
        job.input_directory = input_directory
        job.output_directory = output_directory
        job.tmp_directory = tmp_directory
        for path in (input_directory, output_directory, tmp_directory):
            await StreamFlowPath(path, context=context, location=location).mkdir(
                parents=True, exist_ok=True
            )


async def _remove_stacked_job_dirs(
    context: StreamFlowContext,
    jobs: MutableSequence[Job],
    location: ExecutionLocation,
) -> None:
    for job in jobs:
        for path in (job.input_directory, job.output_directory, job.tmp_directory):
            if path is not None:
                if await (
                    streamflow_path := StreamFlowPath(
                        path, context=context, location=location
                    )
                ).exists():
                    await streamflow_path.rmtree()


def _expand_inner_hardware(
    context: StreamFlowContext,
    inner_ctx: _HardwareTestContext,
    in_memory: bool = False,
) -> None:
    connector = cast(
        ParameterizableHardwareConnector,
        context.deployment_manager.get_connector(inner_ctx.deployment_name),
    )
    connector.set_hardware(
        Hardware(
            cores=1000.0,
            memory=1_000_000.0,
            storage={
                os.sep: Storage(
                    mount_point=os.sep, size=1_000_000.0, in_memory=in_memory
                )
            },
        )
    )


async def _wake_inner_scheduler(
    context: StreamFlowContext,
    inner_ctx: _HardwareTestContext,
    in_memory: bool = False,
) -> None:
    """Expand inner capacity and release a public scheduler wait via a wake job."""
    _expand_inner_hardware(context=context, inner_ctx=inner_ctx, in_memory=in_memory)
    wake_job = Job(
        name=random_job_name(),
        workflow_id=0,
        inputs={},
        input_directory=inner_ctx.jobs[0].input_directory,
        output_directory=inner_ctx.jobs[0].output_directory,
        tmp_directory=inner_ctx.jobs[0].tmp_directory,
    )
    await context.scheduler.schedule(
        wake_job, inner_ctx.binding_config, hardware_requirement=None
    )
    await context.scheduler.notify_status(wake_job.name, Status.COMPLETED)


async def _complete_jobs(
    context: StreamFlowContext, jobs: MutableSequence[Job]
) -> None:
    """Mark allocated jobs as completed so their resources are released."""
    for job in jobs:
        try:
            if context.scheduler.get_allocation(job.name).status != Status.COMPLETED:
                await context.scheduler.notify_status(job.name, Status.COMPLETED)
        except WorkflowExecutionException:
            pass


@pytest_asyncio.fixture(scope="session")
async def deployment_config(
    context: StreamFlowContext, deployment: str
) -> DeploymentConfig:
    return await get_deployment_config(context, deployment)


@pytest.fixture(scope="session")
def service(context: StreamFlowContext, deployment: str) -> str | None:
    return get_service(context, deployment)


@pytest.mark.asyncio
@pytest.mark.parametrize("container_deployment_name", ["docker", "singularity"])
async def test_bind_volumes(
    chosen_deployment_types: MutableSequence[str],
    container_deployment_name: str,
    context: StreamFlowContext,
) -> None:
    """Test the binding of volumes in stacked locations"""
    for deployment in [container_deployment_name, "local"]:
        if deployment not in chosen_deployment_types:
            pytest.skip(f"Deployment {deployment} was not activated")
    # Get local deployment
    local_deployment = get_local_deployment_config()
    service = get_service(context, local_deployment.type)
    local_connector = context.deployment_manager.get_connector(local_deployment.name)
    assert local_connector is not None and local_deployment.workdir is not None
    local_location = next(
        iter((await local_connector.get_available_locations(service=service)).values())
    )
    assert local_location.hardware is not None

    # Get container deployment
    container_paths = {
        "/tmp/streamflow",
        "/home/output",
        "/home/mydata",
        "/home/workdir",
        "/home/workdir1",
        "/home/workdir2",
    }
    container_deployment = await get_deployment_config(
        context, container_deployment_name
    )
    service = get_service(context, container_deployment.type)
    assert (
        container_connector := context.deployment_manager.get_connector(
            container_deployment.name
        )
    ) is not None
    container_location = next(
        iter(
            (
                await container_connector.get_available_locations(service=service)
            ).values()
        )
    )
    # Add the bind with only src (dst will be the same path)
    if container_deployment_name == "docker":
        container_paths.add(
            next(
                b
                for b in cast(DockerConnector, container_connector).volume
                if ":" not in b
            )
        )
    else:
        container_paths.add(
            next(
                b
                for b in cast(SingularityConnector, container_connector).bind
                if ":" not in b
            )
        )

    assert (
        container_location.hardware is not None
        and not container_paths - container_location.hardware.storage.keys()
    )
    assert (container_path := container_deployment.workdir) is not None
    mount_point = await utils.get_mount_point(
        context, container_location, container_path
    )
    fake_hardware = Hardware(
        cores=float(1),
        memory=float(100),
        storage={
            key: Storage(
                mount_point=mount_point,
                size=float(100),
                paths={container_path},
                bind=container_location.hardware.get_storage(mount_point).bind,
            )
            for key in ["tmpdir", "workdir"]
        },
    )
    assert not fake_hardware.is_normalized()
    container_hardware = await utils.bind_mount_point(
        context, local_location, fake_hardware
    )
    assert (
        container_hardware.is_normalized()
        and len(container_hardware.storage) == 1
        and len(fake_hardware.storage) == 2
        and next(iter(container_hardware.storage.values())).size
        == sum(s.size for s in fake_hardware.storage.values())
    )
    # The target paths are all mounted to the same host path
    assert next(
        iter(container_hardware.storage.values())
    ).mount_point == await utils.get_mount_point(
        context, local_location, local_deployment.workdir
    )
    assert local_location.hardware.satisfies(container_hardware)


@pytest.mark.asyncio
async def test_binding_filter(
    chosen_deployment_types: MutableSequence[str], context: StreamFlowContext
) -> None:
    """Test Binding Filter using a job with two targets both free. With the CustomBindingFilter the scheduling will choose the second target"""
    for deployment in ["docker", "local"]:
        if deployment not in chosen_deployment_types:
            pytest.skip(f"Deployment {deployment} was not activated")
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
    with InjectPlugin("reverse"):
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


def test_hardware() -> None:
    """Test Hardware arithmetic operations"""
    testing_mount_point = os.path.join(os.sep, "tmp")
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
                mount_point=testing_mount_point,
                size=float(2**15),
            ),
        },
    )
    assert not main_hardware.is_normalized()
    extra_hardware = Hardware(
        cores=float(5),
        storage={
            "placeholder_4": Storage(mount_point=testing_mount_point, size=float(2**10))
        },
    )
    secondary_hardware = main_hardware + extra_hardware
    assert secondary_hardware.is_normalized()
    assert secondary_hardware.cores == main_hardware.cores + extra_hardware.cores
    assert secondary_hardware.memory == main_hardware.memory
    # Secondary hardware after the sum operation has the storage keys in the normalized form
    assert secondary_hardware.storage.keys() == {os.sep, testing_mount_point}
    assert (
        secondary_hardware.get_storage(testing_mount_point).size
        == main_hardware.get_storage(testing_mount_point).size
        + extra_hardware.get_storage(testing_mount_point).size
    )

    # Changing storage keys and the number of storages to evaluate whether operations are independent of them
    secondary_hardware.storage["other_name_1.1"] = secondary_hardware.storage.pop(
        main_hardware.storage["placeholder_1"].mount_point
    )
    secondary_hardware.storage["other_name_2.1"] = secondary_hardware.storage.pop(
        main_hardware.storage["placeholder_3"].mount_point
    )
    # The `secondary_hardware` has more cores and disk space in the `/tmp` storage than `main_hardware`
    assert secondary_hardware.satisfies(main_hardware)
    assert not main_hardware.satisfies(secondary_hardware)

    # Testing difference operation
    secondary_hardware -= extra_hardware
    assert secondary_hardware.cores == main_hardware.cores
    assert secondary_hardware.memory == main_hardware.memory
    assert secondary_hardware.storage.keys() == {os.sep, testing_mount_point}
    assert (
        secondary_hardware.get_storage(testing_mount_point).size
        == main_hardware.get_storage(testing_mount_point).size
    )
    assert main_hardware.satisfies(secondary_hardware)
    # Testing the validity of the storage comparison
    main_hardware.storage.pop("placeholder_3")
    with pytest.raises(WorkflowExecutionException) as err:
        _ = main_hardware.satisfies(secondary_hardware)
    assert (
        str(err.value) == f"Invalid `Hardware` comparison: {main_hardware} should "
        f"contain all the storage included in {secondary_hardware}."
    )


@pytest.mark.asyncio
async def test_multi_env(
    chosen_deployment_types: MutableSequence[str], context: StreamFlowContext
) -> None:
    """Test scheduling two jobs on two different environments."""
    for deployment in ["docker", "local"]:
        if deployment not in chosen_deployment_types:
            pytest.skip(f"Deployment {deployment} was not activated")
    with InjectPlugin(plugin_name="parameterizable-hardware"):
        config = get_parameterizable_hardware_deployment_config(name=random_name())
        await context.deployment_manager.deploy(config)
        hardware_requirement, target = _prepare_connector(
            context=context, deployment_name=config.name
        )
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
    await context.deployment_manager.undeploy(config.name)


@pytest.mark.asyncio
async def test_multi_targets_one_job(
    chosen_deployment_types: MutableSequence[str], context: StreamFlowContext
) -> None:
    """Test scheduling one jobs with two targets: Local and Docker Image. The job will be scheduled in the first"""
    for deployment in ["docker", "local"]:
        if deployment not in chosen_deployment_types:
            pytest.skip(f"Deployment {deployment} was not activated")
    with InjectPlugin(plugin_name="parameterizable-hardware"):
        config = get_parameterizable_hardware_deployment_config(name=random_name())
        await context.deployment_manager.deploy(config)
        hardware_requirement, target = _prepare_connector(
            context=context, deployment_name=config.name
        )
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
    await context.deployment_manager.undeploy(config.name)


@pytest.mark.asyncio
async def test_multi_targets_two_jobs(
    chosen_deployment_types: MutableSequence[str], context: StreamFlowContext
) -> None:
    """
    Test scheduling two jobs with two same targets: Local and Docker Image.
    The first job will be scheduled in the local target and the second job in the docker target because the local resources will be full.
    """
    for deployment in ["docker", "local"]:
        if deployment not in chosen_deployment_types:
            pytest.skip(f"Deployment {deployment} was not activated")
    with InjectPlugin(plugin_name="parameterizable-hardware"):
        config = get_parameterizable_hardware_deployment_config(name=random_name())
        await context.deployment_manager.deploy(config)
        hardware_requirement, target = _prepare_connector(
            context=context, deployment_name=config.name
        )
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
    await context.deployment_manager.undeploy(config.name)


@pytest.mark.asyncio
async def test_scheduling(
    context: StreamFlowContext,
    deployment_config: DeploymentConfig,
    service: str | None,
) -> None:
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
async def test_single_env_enough_resources(context: StreamFlowContext) -> None:
    """Test scheduling two jobs on a single environment with resources for all jobs together."""
    num_jobs = 2
    with InjectPlugin(plugin_name="parameterizable-hardware"):
        config = get_parameterizable_hardware_deployment_config(name=random_name())
        await context.deployment_manager.deploy(config)
        hardware_requirement, target = _prepare_connector(
            context=context, deployment_name=config.name, num_jobs=num_jobs
        )
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
    try:
        task_pending = [
            asyncio.create_task(
                context.scheduler.schedule(job, binding_config, hardware_requirement)
            )
            for job in jobs
        ]
        assert len(task_pending) == num_jobs

        # Available resources to schedule all the jobs (timeout parameter useful if a deadlock occurs)
        task_completed, task_pending = await asyncio.wait(
            task_pending, return_when=asyncio.ALL_COMPLETED, timeout=60
        )
        assert len(task_pending) == 0
        # Test errors were raised
        for t in task_completed:
            assert t.result() is None
        for j in jobs:
            assert context.scheduler.get_allocation(j.name).status == Status.FIREABLE

        # Jobs change status to RUNNING
        await _notify_status_and_test(context, jobs, Status.RUNNING)
        # Jobs change status to COMPLETED
        await _notify_status_and_test(context, jobs, Status.COMPLETED)
    finally:
        await asyncio.gather(
            *(
                asyncio.create_task(
                    context.scheduler.notify_status(j.name, Status.COMPLETED)
                )
                for j in jobs
            )
        )
        await context.deployment_manager.undeploy(config.name)


@pytest.mark.asyncio
async def test_single_env_few_resources(context: StreamFlowContext) -> None:
    """Test scheduling two jobs on single environment but with resources for one job at a time."""
    num_jobs = 2
    with InjectPlugin(plugin_name="parameterizable-hardware"):
        config = get_parameterizable_hardware_deployment_config(name=random_name())
        await context.deployment_manager.deploy(config)
        hardware_requirement, target = _prepare_connector(
            context=context,
            deployment_name=config.name,
            location_memory=lambda x: x * num_jobs * 3,
        )
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
    try:
        task_pending = [
            asyncio.create_task(
                context.scheduler.schedule(job, binding_config, hardware_requirement)
            )
            for job in jobs
        ]
        assert len(task_pending) == 2

        # Available resources to schedule only one job (timeout parameter useful if a deadlock occurs)
        task_completed, task_pending = await asyncio.wait(
            task_pending, return_when=asyncio.FIRST_COMPLETED, timeout=60
        )
        assert len(task_pending) == 1
        # Test errors were raised
        for t in task_completed:
            assert t.result() is None
        assert context.scheduler.get_allocation(jobs[0].name).status == Status.FIREABLE
        with pytest.raises(
            WorkflowExecutionException,
            match=f"Could not retrieve allocation for job {jobs[1].name}",
        ):
            context.scheduler.get_allocation(jobs[1].name)

        # First job changes status to RUNNING and continue to keep all resources
        # Testing that second job is not scheduled (timeout parameter necessary)
        await context.scheduler.notify_status(jobs[0].name, Status.RUNNING)
        _, task_pending = await asyncio.wait(task_pending, timeout=2)

        assert len(task_pending) == 1
        assert context.scheduler.get_allocation(jobs[0].name).status == Status.RUNNING
        with pytest.raises(
            WorkflowExecutionException,
            match=f"Could not retrieve allocation for job {jobs[1].name}",
        ):
            context.scheduler.get_allocation(jobs[1].name)

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
    finally:
        await asyncio.gather(
            *(
                asyncio.create_task(
                    context.scheduler.notify_status(j.name, Status.COMPLETED)
                )
                for j in jobs
            )
        )
        await context.deployment_manager.undeploy(config.name)


@pytest.mark.asyncio
async def test_shared_stacked_locations(context: StreamFlowContext) -> None:
    """
    Test scheduling two jobs on two different deployments but share the same stacked location.
    The underlying location has enough resources for only one job at a time.
    """
    num_jobs = 2
    with InjectPlugin(plugin_name="parameterizable-hardware"):
        param_config = get_parameterizable_hardware_deployment_config(
            name=random_name()
        )
        await context.deployment_manager.deploy(param_config)
        _prepare_connector(
            context=context, deployment_name=param_config.name, num_jobs=1
        )
    hardware_requirement = CWLHardwareRequirement(cwl_version=CWL_VERSION)

    targets = []
    for i in range(num_jobs):
        docker_config = await get_deployment_config(context, "docker")
        docker_config.name += f"-{i}"
        docker_config.wraps = WrapsConfig(
            deployment=param_config.name,
            service=get_service(context, param_config.type),
        )
        targets.append(
            Target(
                deployment=docker_config,
                service=get_service(context, docker_config.type),
                workdir=docker_config.workdir,
            )
        )
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
    try:
        await asyncio.gather(
            *(
                asyncio.create_task(
                    context.deployment_manager.deploy(target.deployment)
                )
                for target in targets
            )
        )
        task_pending = [
            asyncio.create_task(
                context.scheduler.schedule(
                    job, BindingConfig(targets=[target]), hardware_requirement
                )
            )
            for job, target in zip(jobs, targets, strict=True)
        ]
        task_completed, task_pending = await asyncio.wait(
            task_pending, return_when=asyncio.FIRST_COMPLETED, timeout=60
        )
        assert len(task_pending) == 1
        # Test errors were raised
        for t in task_completed:
            assert t.result() is None
        try:
            assert (
                context.scheduler.get_allocation(jobs[0].name).status == Status.FIREABLE
            )
            with pytest.raises(
                WorkflowExecutionException,
                match=f"Could not retrieve allocation for job {jobs[1].name}",
            ):
                context.scheduler.get_allocation(jobs[1].name)
            fst_job, snd_job = jobs[0], jobs[1]
        except WorkflowExecutionException:
            assert (
                context.scheduler.get_allocation(jobs[1].name).status == Status.FIREABLE
            )
            with pytest.raises(
                WorkflowExecutionException,
                match=f"Could not retrieve allocation for job {jobs[0].name}",
            ):
                context.scheduler.get_allocation(jobs[0].name)
            fst_job, snd_job = jobs[1], jobs[0]

        # First job changes status to RUNNING and continue to keep all resources
        # Testing that second job is not scheduled (timeout parameter necessary)
        await context.scheduler.notify_status(fst_job.name, Status.RUNNING)
        _, task_pending = await asyncio.wait(task_pending, timeout=2)

        assert len(task_pending) == 1
        assert context.scheduler.get_allocation(fst_job.name).status == Status.RUNNING
        with pytest.raises(
            WorkflowExecutionException,
            match=f"Could not retrieve allocation for job {snd_job.name}",
        ):
            context.scheduler.get_allocation(snd_job.name)

        # First job completes and the second job can be scheduled (timeout parameter useful if a deadlock occurs)
        await context.scheduler.notify_status(fst_job.name, Status.COMPLETED)
        _, task_pending = await asyncio.wait(
            task_pending, return_when=asyncio.ALL_COMPLETED, timeout=60
        )
        assert len(task_pending) == 0
        assert context.scheduler.get_allocation(fst_job.name).status == Status.COMPLETED
        assert context.scheduler.get_allocation(snd_job.name).status == Status.FIREABLE

        # Second job completed
        await _notify_status_and_test(context, snd_job, Status.RUNNING)
        await _notify_status_and_test(context, snd_job, Status.COMPLETED)
    finally:
        for j in jobs:
            try:
                if context.scheduler.get_allocation(j.name).status != Status.COMPLETED:
                    await context.scheduler.notify_status(j.name, Status.COMPLETED)
            except WorkflowExecutionException:
                pass
        await asyncio.gather(
            *(
                asyncio.create_task(
                    context.deployment_manager.undeploy(target.deployment.name)
                )
                for target in targets
            )
        )
        await context.deployment_manager.undeploy(param_config.name)


@pytest.mark.asyncio
async def test_stacked_shared_inner_requirement(context: StreamFlowContext) -> None:
    """Check that one job's inner requirement is not multiplied.
    Two outer locations share one inner with enough memory for one job."""
    requirement = CWLHardwareRequirement(
        cwl_version=CWL_VERSION, cores=1, memory=100, tmpdir=100, outdir=100
    )
    inner_ctx = await _setup_hardware_test(
        context=context,
        num_jobs=1,
        requirement=requirement,
        memory=100.0,
        storage=1000.0,
        cores=8.0,
    )
    stacked_config = _get_stacked_test_config(
        name=f"stacked-shared-requirement-{random_name()}",
        inner_ctx=inner_ctx,
        locations=2,
    )
    target = Target(
        deployment=stacked_config,
        service=None,
        workdir=stacked_config.workdir,
        locations=1,
    )
    binding_config = BindingConfig(targets=[target])
    await _prepare_stacked_jobs(
        context, inner_ctx.jobs, inner_ctx.exec_location, target.workdir
    )
    job = inner_ctx.jobs[0]
    task: asyncio.Task[None] | None = None
    deployed = False
    with InjectPlugin("stacked-test"):
        try:
            await context.deployment_manager.deploy(stacked_config)
            deployed = True
            task = asyncio.create_task(
                context.scheduler.schedule(job, binding_config, requirement)
            )
            done, _ = await asyncio.wait([task], timeout=10)
            assert len(done) == 1, "job was not scheduled on the shared inner location"
            assert task.result() is None
            assert context.scheduler.get_allocation(job.name).status == Status.FIREABLE
        finally:
            if task is not None:
                if not task.done():
                    await _wake_inner_scheduler(context=context, inner_ctx=inner_ctx)
                await task
            await _complete_jobs(context=context, jobs=inner_ctx.jobs)
            if deployed:
                await context.deployment_manager.undeploy(stacked_config.name)
            await StreamFlowPath(
                inner_ctx.binding_config.targets[0].workdir,
                context=context,
                location=inner_ctx.exec_location,
            ).rmtree()
            await context.deployment_manager.undeploy(inner_ctx.deployment_name)


@pytest.mark.asyncio
async def test_stacked_shared_inner_free(context: StreamFlowContext) -> None:
    """Check that completion frees shared inner resources.
    Two outer locations share one inner, and a second job follows completion."""
    requirement = CWLHardwareRequirement(
        cwl_version=CWL_VERSION, cores=1, memory=100, tmpdir=100, outdir=100
    )
    inner_ctx = await _setup_hardware_test(
        context=context,
        num_jobs=2,
        requirement=requirement,
        memory=300.0,
        storage=1000.0,
        cores=8.0,
    )
    stacked_config = _get_stacked_test_config(
        name=f"stacked-shared-free-{random_name()}",
        inner_ctx=inner_ctx,
        locations=2,
    )
    target = Target(
        deployment=stacked_config,
        service=None,
        workdir=stacked_config.workdir,
        locations=2,
    )
    binding_config = BindingConfig(targets=[target])
    await _prepare_stacked_jobs(
        context, inner_ctx.jobs, inner_ctx.exec_location, target.workdir
    )
    first_job, second_job = inner_ctx.jobs
    follow_up: asyncio.Task[None] | None = None
    deployed = False
    with InjectPlugin("stacked-test"):
        try:
            await context.deployment_manager.deploy(stacked_config)
            deployed = True
            await context.scheduler.schedule(first_job, binding_config, requirement)
            assert (
                context.scheduler.get_allocation(first_job.name).status
                == Status.FIREABLE
            )
            await context.scheduler.notify_status(first_job.name, Status.RUNNING)
            await context.scheduler.notify_status(first_job.name, Status.COMPLETED)
            follow_up = asyncio.create_task(
                context.scheduler.schedule(second_job, binding_config, requirement)
            )
            done, _ = await asyncio.wait([follow_up], timeout=10)
            assert (
                len(done) == 1
            ), "second job was not scheduled after the first job completed"
            assert follow_up.result() is None
            assert (
                context.scheduler.get_allocation(second_job.name).status
                == Status.FIREABLE
            )
        finally:
            if follow_up is not None:
                if not follow_up.done():
                    await _wake_inner_scheduler(context=context, inner_ctx=inner_ctx)
                await follow_up
            await _complete_jobs(context=context, jobs=inner_ctx.jobs)
            if deployed:
                await context.deployment_manager.undeploy(stacked_config.name)
            await StreamFlowPath(
                inner_ctx.binding_config.targets[0].workdir,
                context=context,
                location=inner_ctx.exec_location,
            ).rmtree()
            await context.deployment_manager.undeploy(inner_ctx.deployment_name)


@pytest.mark.asyncio
async def test_stacked_in_memory_free(context: StreamFlowContext) -> None:
    """Check that completion releases the inner in-memory reservation.
    A disk-backed outer wraps inner in-memory storage; a second job follows."""
    requirement = CWLHardwareRequirement(
        cwl_version=CWL_VERSION, cores=1, memory=100, tmpdir=100, outdir=100
    )
    inner_ctx = await _setup_hardware_test(
        context=context,
        num_jobs=2,
        requirement=requirement,
        memory=300.0,
        storage=200.0,
        inmemory=True,
        cores=8.0,
    )
    workdir = inner_ctx.binding_config.targets[0].workdir
    await _prepare_stacked_jobs(
        context, inner_ctx.jobs, inner_ctx.exec_location, workdir
    )
    first_job, second_job = inner_ctx.jobs
    outer_storage_paths = {
        path
        for job in inner_ctx.jobs
        for path in (job.tmp_directory, job.output_directory)
        if path is not None
    }
    outer_hardware = Hardware(
        cores=4.0,
        memory=1000.0,
        storage={
            os.sep: Storage(
                mount_point=os.sep,
                size=1000.0,
                paths=outer_storage_paths,
                bind=os.sep,
                in_memory=False,
            )
        },
    )
    stacked_config = _get_stacked_test_config(
        name=f"stacked-in-memory-{random_name()}",
        inner_ctx=inner_ctx,
        locations=1,
        outer_hardware=outer_hardware,
    )
    target = Target(
        deployment=stacked_config,
        service=None,
        workdir=workdir,
        locations=1,
    )
    binding_config = BindingConfig(targets=[target])
    follow_up: asyncio.Task[None] | None = None
    outer_location = inner_ctx.exec_location
    deployed = False
    with InjectPlugin(plugin_name="stacked-test"):
        try:
            await context.deployment_manager.deploy(stacked_config)
            deployed = True
            stacked_connector = context.deployment_manager.get_connector(
                stacked_config.name
            )
            assert stacked_connector is not None
            available_locations = await stacked_connector.get_available_locations(
                service=target.service
            )
            assert len(available_locations) == 1
            available_location = next(iter(available_locations.values()))
            assert available_location.local
            outer_location = available_location.location
            await _prepare_stacked_jobs(
                context, inner_ctx.jobs, outer_location, workdir
            )
            await context.scheduler.schedule(first_job, binding_config, requirement)
            assert (
                context.scheduler.get_allocation(first_job.name).status
                == Status.FIREABLE
            )
            await context.scheduler.notify_status(first_job.name, Status.RUNNING)
            await context.scheduler.notify_status(first_job.name, Status.COMPLETED)
            follow_up = asyncio.create_task(
                context.scheduler.schedule(second_job, binding_config, requirement)
            )
            done, _ = await asyncio.wait([follow_up], timeout=10)
            assert len(done) == 1, (
                "second job was not scheduled: the completed job left the 200 MB "
                "in-memory fold reserved on the inner location, so its memory never "
                "returned to 0 and the second job cannot fit"
            )
            assert follow_up.result() is None
            assert (
                context.scheduler.get_allocation(second_job.name).status
                == Status.FIREABLE
            )
        finally:
            if follow_up is not None:
                if not follow_up.done():
                    await _wake_inner_scheduler(
                        context=context, inner_ctx=inner_ctx, in_memory=True
                    )
                await follow_up
            await _complete_jobs(context=context, jobs=inner_ctx.jobs)
            await _remove_stacked_job_dirs(context, inner_ctx.jobs, outer_location)
            await StreamFlowPath(
                workdir, context=context, location=outer_location
            ).rmtree()
            if deployed:
                await context.deployment_manager.undeploy(stacked_config.name)
            await context.deployment_manager.undeploy(inner_ctx.deployment_name)


@pytest.mark.asyncio
async def test_residual_in_memory_stacked(context: StreamFlowContext) -> None:
    """Check that a residual output stays charged to inner memory.
    An unbound outer tmpfs keeps a 50 MiB output before a 330 MB follow-up."""
    first_requirement = CWLHardwareRequirement(
        cwl_version=CWL_VERSION, cores=1, memory=100, tmpdir=50, outdir=50
    )
    inner_ctx = await _setup_hardware_test(
        context=context,
        num_jobs=2,
        requirement=first_requirement,
        memory=350.0,
        storage=1000.0,
        cores=4.0,
    )
    workdir = inner_ctx.binding_config.targets[0].workdir
    await _prepare_stacked_jobs(
        context, inner_ctx.jobs, inner_ctx.exec_location, workdir
    )
    first_job, second_job = inner_ctx.jobs
    outer_storage_paths = {
        path
        for job in inner_ctx.jobs
        for path in (job.tmp_directory, job.output_directory)
        if path is not None
    }
    # The outer location is synthetic; inner accounting still uses the shared inner location.
    outer_hardware = Hardware(
        cores=4.0,
        memory=1000.0,
        storage={
            os.sep: Storage(
                mount_point=os.sep,
                size=200.0,
                paths=outer_storage_paths,
                in_memory=True,
            )
        },
    )
    stacked_config = _get_stacked_test_config(
        name=f"residual-in-memory-stacked-{random_name()}",
        inner_ctx=inner_ctx,
        locations=1,
        outer_hardware=outer_hardware,
    )
    target = Target(
        deployment=stacked_config,
        service=None,
        workdir=stacked_config.workdir,
        locations=1,
    )
    binding_config = BindingConfig(targets=[target])
    second_requirement = CWLHardwareRequirement(
        cwl_version=CWL_VERSION, cores=1, memory=330, tmpdir=0, outdir=0
    )
    second_task: asyncio.Task[None] | None = None
    outer_location = inner_ctx.exec_location
    deployed = False
    with InjectPlugin("stacked-test"):
        try:
            await context.deployment_manager.deploy(stacked_config)
            deployed = True
            stacked_connector = context.deployment_manager.get_connector(
                stacked_config.name
            )
            assert stacked_connector is not None
            available_locations = await stacked_connector.get_available_locations(
                service=target.service
            )
            assert len(available_locations) == 1
            available_location = next(iter(available_locations.values()))
            assert available_location.local
            outer_location = available_location.location
            await _prepare_stacked_jobs(
                context, inner_ctx.jobs, outer_location, target.workdir
            )
            await context.scheduler.schedule(
                first_job, binding_config, first_requirement
            )
            assert (
                context.scheduler.get_allocation(first_job.name).status
                == Status.FIREABLE
            )
            await context.scheduler.notify_status(first_job.name, Status.RUNNING)
            assert first_job.output_directory is not None
            # Writing 50 MiB leaves an in-memory residual beyond the bound 50 MiB storage.
            await StreamFlowPath(
                first_job.output_directory,
                "residual.bin",
                context=context,
                location=outer_location,
            ).write_text("\0" * (50 * 1024 * 1024))
            await context.scheduler.notify_status(first_job.name, Status.COMPLETED)
            second_task = asyncio.create_task(
                context.scheduler.schedule(
                    second_job, binding_config, second_requirement
                )
            )
            done, _ = await asyncio.wait([second_task], timeout=5)
            assert len(done) == 0, (
                "second job was scheduled because the 50 MiB unbound in-memory "
                "residual was dropped while rebinding to the inner location"
            )
            with pytest.raises(WorkflowExecutionException):
                context.scheduler.get_allocation(second_job.name)
        finally:
            if second_task is not None:
                if not second_task.done():
                    await _wake_inner_scheduler(context=context, inner_ctx=inner_ctx)
                await second_task
            await _complete_jobs(context=context, jobs=inner_ctx.jobs)
            await _remove_stacked_job_dirs(context, inner_ctx.jobs, outer_location)
            await StreamFlowPath(
                workdir, context=context, location=outer_location
            ).rmtree()
            if deployed:
                await context.deployment_manager.undeploy(stacked_config.name)
            await context.deployment_manager.undeploy(inner_ctx.deployment_name)


@pytest.mark.asyncio
async def test_parallel_in_memory(context: StreamFlowContext) -> None:
    """
    Verify that in-memory storage prevents parallel job scheduling.

    Two jobs each need 300 MB effective (100 memory + 200 in_memory storage).
    With 550 MB memory and 450 MB storage, only one fits at a time. The first
    job runs, completes, and the second can be scheduled.
    """
    num_jobs = 2
    requirement = CWLHardwareRequirement(
        cwl_version=CWL_VERSION,
        cores=1,
        memory=100,
        tmpdir=100,
        outdir=100,
    )
    inmemory_ctx = await _setup_hardware_test(
        context,
        num_jobs,
        requirement,
        memory=550.0,
        storage=450.0,
        inmemory=True,
    )
    path = None
    try:
        tasks = [
            asyncio.create_task(
                context.scheduler.schedule(
                    job, inmemory_ctx.binding_config, inmemory_ctx.requirement
                )
            )
            for job in inmemory_ctx.jobs
        ]
        done, pending = await asyncio.wait(
            tasks, return_when=asyncio.FIRST_COMPLETED, timeout=60
        )
        assert len(done) == 1
        for t in done:
            assert t.result() is None
        first_job = _get_scheduled_job(context=context, jobs=inmemory_ctx.jobs)
        assert first_job is not None
        second_job = next(j for j in inmemory_ctx.jobs if j is not first_job)

        await context.scheduler.notify_status(first_job.name, Status.RUNNING)
        path = StreamFlowPath(
            first_job.output_directory,
            "persistent.dat",
            context=context,
            location=inmemory_ctx.exec_location,
        )
        await path.write_text("\0" * (10 * 1024 * 1024))
        await context.scheduler.notify_status(first_job.name, Status.COMPLETED)

        done, _ = await asyncio.wait(pending, timeout=60)
        assert len(done) == 1
        assert (
            context.scheduler.get_allocation(second_job.name).status == Status.FIREABLE
        )
    finally:
        await _cleanup_hardware_test(
            context=context,
            deployment_name=inmemory_ctx.deployment_name,
            jobs=inmemory_ctx.jobs,
            paths=[path],
        )


@pytest.mark.asyncio
async def test_residual_in_memory(context: StreamFlowContext) -> None:
    """
    Verify that residual in-memory files leak memory.

    A job with in_memory storage writes a 50 MB file and completes.
    The file persists on disk, consuming memory. A subsequent job needing
    330 MB of memory should be blocked (only 300 MB free).
    """
    requirement = CWLHardwareRequirement(
        cwl_version=CWL_VERSION,
        cores=1,
        memory=100,
        tmpdir=50,
        outdir=50,
    )
    inmemory_ctx = await _setup_hardware_test(
        context,
        2,
        requirement,
        memory=350.0,
        storage=200.0,
        inmemory=True,
    )
    path = None
    try:
        await context.scheduler.schedule(
            inmemory_ctx.jobs[0], inmemory_ctx.binding_config, inmemory_ctx.requirement
        )
        first_job = _get_scheduled_job(context=context, jobs=inmemory_ctx.jobs[:1])
        assert first_job is not None

        await context.scheduler.notify_status(first_job.name, Status.RUNNING)
        path = StreamFlowPath(
            first_job.output_directory,
            "persistent.dat",
            context=context,
            location=inmemory_ctx.exec_location,
        )
        await path.write_text("\0" * (50 * 1024 * 1024))
        await context.scheduler.notify_status(first_job.name, Status.COMPLETED)

        snd_hardware = CWLHardwareRequirement(
            cwl_version=CWL_VERSION,
            cores=1,
            memory=330,
            tmpdir=0,
            outdir=0,
        )
        snd_job = inmemory_ctx.jobs[1]
        snd_task = asyncio.create_task(
            context.scheduler.schedule(
                snd_job, inmemory_ctx.binding_config, snd_hardware
            )
        )
        done, _ = await asyncio.wait([snd_task], timeout=5)
        assert len(done) == 0
        snd_task.cancel()
        with pytest.raises(WorkflowExecutionException):
            context.scheduler.get_allocation(snd_job.name)
    finally:
        await _cleanup_hardware_test(
            context=context,
            deployment_name=inmemory_ctx.deployment_name,
            jobs=inmemory_ctx.jobs,
            paths=[path],
        )


@pytest.mark.asyncio
async def test_disk_usage(context: StreamFlowContext) -> None:
    """
    Verify that the scheduler correctly tracks disk usage across multiple jobs.
    Jobs fill available storage until the last one is left pending because no space remains.
    """
    requirement = CWLHardwareRequirement(
        cwl_version=CWL_VERSION,
        cores=1,
        memory=100,
        tmpdir=25,
        outdir=25,
    )
    hw_ctx = await _setup_hardware_test(
        context,
        num_jobs=3,
        requirement=requirement,
        memory=1000.0,
        storage=100.0,
        job_dirs=[(None, None, None)],
    )
    out_file, out_file2 = None, None
    try:
        await context.scheduler.schedule(
            hw_ctx.jobs[0], hw_ctx.binding_config, hw_ctx.requirement
        )
        first_job = _get_scheduled_job(context=context, jobs=hw_ctx.jobs[:1])
        assert first_job is not None
        await context.scheduler.notify_status(first_job.name, Status.RUNNING)
        out_file = StreamFlowPath(
            hw_ctx.binding_config.targets[0].workdir,
            "out.dat",
            context=context,
            location=hw_ctx.exec_location,
        )
        await out_file.write_text("\0" * (30 * 1024 * 1024))
        await context.scheduler.notify_status(first_job.name, Status.COMPLETED)

        task = asyncio.create_task(
            context.scheduler.schedule(
                hw_ctx.jobs[1], hw_ctx.binding_config, hw_ctx.requirement
            )
        )
        done, _ = await asyncio.wait([task], timeout=5)
        assert len(done) == 1
        task.cancel()
        second_job = _get_scheduled_job(context=context, jobs=hw_ctx.jobs[1:2])
        assert second_job is not None
        await context.scheduler.notify_status(second_job.name, Status.RUNNING)
        out_file2 = StreamFlowPath(
            second_job.output_directory,
            "out.dat",
            context=context,
            location=hw_ctx.exec_location,
        )
        await out_file2.write_text("\0" * (70 * 1024 * 1024))
        await context.scheduler.notify_status(second_job.name, Status.COMPLETED)

        task = asyncio.create_task(
            context.scheduler.schedule(
                hw_ctx.jobs[2], hw_ctx.binding_config, hw_ctx.requirement
            )
        )
        done, _ = await asyncio.wait([task], timeout=5)
        assert len(done) == 0
        task.cancel()
        with pytest.raises(WorkflowExecutionException):
            context.scheduler.get_allocation(hw_ctx.jobs[2].name)
    finally:
        await _cleanup_hardware_test(
            context=context,
            deployment_name=hw_ctx.deployment_name,
            jobs=hw_ctx.jobs,
            paths=[out_file, out_file2],
        )
