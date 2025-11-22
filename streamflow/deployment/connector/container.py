from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import posixpath
from abc import ABC, abstractmethod
from collections.abc import MutableMapping, MutableSequence
from importlib.resources import files
from shutil import which
from typing import Any, AsyncContextManager, cast

import psutil

from streamflow.core import utils
from streamflow.core.data import StreamWrapper
from streamflow.core.deployment import Connector, ExecutionLocation
from streamflow.core.exception import (
    WorkflowDefinitionException,
    WorkflowExecutionException,
)
from streamflow.core.scheduling import AvailableLocation, Hardware, Storage
from streamflow.core.utils import (
    get_local_to_remote_destination,
    get_option,
    random_name,
)
from streamflow.deployment.connector.base import (
    FS_TYPES_TO_SKIP,
    BatchConnector,
    copy_local_to_remote,
    copy_remote_to_local,
    copy_remote_to_remote,
    copy_same_connector,
)
from streamflow.deployment.wrapper import ConnectorWrapper, get_inner_location
from streamflow.log_handler import logger


async def _get_storage_from_binds(
    connector: Connector,
    location: ExecutionLocation,
    name: str,
    entity: str,
    binds: MutableMapping[str, str],
) -> MutableMapping[str, Storage]:
    storage = {}
    stdout, returncode = await connector.run(
        location=location,
        command=[
            "df",
            "-aT",
            "|",
            "tail",
            "-n",
            "+2",
            "|",
            "awk",
            "'NF == 1 {device = $1; getline; $0 = device $0} {print $7, $2, $5}'",
        ],
        capture_output=True,
    )
    if returncode == 0:
        for line in stdout.splitlines():
            try:
                mount_point, fs_type, size = line.split(" ")
                if fs_type not in FS_TYPES_TO_SKIP:
                    storage[mount_point] = Storage(
                        mount_point=mount_point,
                        size=float(size) / 2**10,
                    )
                    if mount_point in binds:
                        storage[mount_point].bind = binds[mount_point]
            except ValueError as e:
                logger.warning(
                    f"Skipping line {line} for deployment {location.deployment}: {e}"
                )
            except Exception as e:
                raise WorkflowExecutionException(
                    f"Error parsing {line} for deployment {location.deployment}: {e}"
                )
        return storage
    else:
        raise WorkflowExecutionException(
            f"FAILED retrieving bind mounts using `df -aT` for {entity} '{name}' "
            f"in deployment {connector.deployment_name}: [{returncode}]: {stdout}"
        )


def _parse_mount(mount: str) -> tuple[str, str]:
    source = next(
        part[4:]
        for part in mount.split(",")
        if part.startswith("src=") or part.startswith("source=")
    )
    destination = next(
        part[4:]
        for part in mount.split(",")
        if part.startswith("dst=")
        or part.startswith("dest=")
        or part.startswith("destination=")
        or part.startswith("target=")
    )
    return source, destination


class ContainerInstance:
    __slots__ = ("address", "cores", "current_user", "memory", "volumes")

    def __init__(
        self,
        address: str,
        cores: float,
        current_user: bool,
        memory: float,
        volumes: MutableMapping[str, Storage],
    ):
        self.address: str = address
        self.cores: float = cores
        self.current_user: bool = current_user
        self.memory: float = memory
        self.volumes: MutableMapping[str, Storage] = volumes


class ContainerConnector(ConnectorWrapper, ABC):
    def __init__(
        self,
        deployment_name: str,
        config_dir: str,
        connector: Connector,
        service: str | None,
        transferBufferSize: int,
    ):
        if isinstance(connector, BatchConnector):
            raise WorkflowDefinitionException(
                f"Deployment {self.deployment_name} of type {self.__class__.__name__} "
                f"cannot wrap deployment {connector.deployment_name} "
                f"of type {connector.__class__.__name__}, which extends the BatchConnector type."
            )
        super().__init__(
            deployment_name=deployment_name,
            config_dir=config_dir,
            connector=connector,
            service=service,
            transferBufferSize=transferBufferSize,
        )
        self._inner_location: AvailableLocation | None = None
        self._instances: MutableMapping[str, ContainerInstance] = {}

    async def _check_effective_location(
        self,
        common_paths: MutableMapping[str, Any],
        effective_locations: MutableSequence[ExecutionLocation],
        location: ExecutionLocation,
        path: str,
        source_location: ExecutionLocation | None = None,
    ) -> tuple[MutableMapping[str, Any], MutableSequence[ExecutionLocation]]:
        # Get all container mounts
        for volume in (await self._get_instance(location.name)).volumes.values():
            # If volume is a bind mount
            if volume.bind is not None:
                # If path is in a persistent volume
                if path.startswith(volume.mount_point):
                    if path not in common_paths:
                        common_paths[path] = []
                    # Check if path is shared with another location that has been already processed
                    for i, common_path in enumerate(common_paths[path]):
                        if common_path["source"] == volume.bind:
                            # If path has already been processed, but the current location is the source, substitute it
                            if (
                                source_location
                                and location.name == source_location.name
                            ):
                                effective_locations.remove(common_path["location"])
                                common_path["location"] = location
                                common_paths[path][i] = common_path
                                effective_locations.append(location)
                                return common_paths, effective_locations
                            # Otherwise simply skip current location
                            else:
                                return common_paths, effective_locations
                    # If this is the first location encountered with the persistent path, add it to the list
                    effective_locations.append(location)
                    common_paths[path].append(
                        {"source": volume.bind, "location": location}
                    )
                    return common_paths, effective_locations
        # If path is not in a persistent volume, add the current location to the list
        effective_locations.append(location)
        return common_paths, effective_locations

    def _get_inner_location(self) -> ExecutionLocation:
        if self._inner_location is None:
            raise ValueError(
                f"The _inner_location parameter of {self.__class__.__name__} "
                "should not be null at this time"
            )
        return self._inner_location.location

    async def _local_copy(
        self, src: str, dst: str, location: ExecutionLocation, read_only: bool
    ) -> None:
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"COPYING from {src} to {dst} on location {location}")
        await self.run(
            location=location,
            command=(["ln", "-snf"] if read_only else ["/bin/cp", "-rf"]) + [src, dst],
        )
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"COMPLETED copy from {src} to {dst} on location {location}")

    async def copy_local_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[ExecutionLocation],
        read_only: bool = False,
    ) -> None:
        bind_locations = {}
        copy_tasks = []
        dst = await get_local_to_remote_destination(self, locations[0], src, dst)
        for location in await self._get_effective_locations(locations, dst):
            instance = await self._get_instance(location.name)
            # Check if the container user is the current host user
            # and if the destination path is on a mounted volume
            if (
                instance.current_user
                and (adjusted_dst := self._get_host_path(instance, dst)) is not None
            ):
                # If data is read_only, check if the source path is bound to a mounted volume, too
                if (
                    read_only
                    and (adjusted_src := self._get_container_path(instance, src))
                    is not None
                ):
                    # If yes, then create a symbolic link
                    copy_tasks.append(
                        asyncio.create_task(
                            self._local_copy(
                                src=adjusted_src,
                                dst=dst,
                                location=location,
                                read_only=read_only,
                            )
                        )
                    )
                # Otherwise, delegate transfer to the inner connector
                else:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(
                            f"Delegating the transfer of {src} "
                            f"to {adjusted_dst} from local file-system "
                            f"to location {location.name} to the inner connector."
                        )
                    bind_locations.setdefault(adjusted_dst, []).append(location)
            # Otherwise, check if the source path is bound to a mounted volume
            elif (adjusted_src := self._get_container_path(instance, src)) is not None:
                # If yes, perform a copy command through the container connector
                copy_tasks.append(
                    asyncio.create_task(
                        self._local_copy(
                            src=adjusted_src,
                            dst=dst,
                            location=location,
                            read_only=read_only,
                        )
                    )
                )
            else:
                # If not, perform a transfer through the container connector
                copy_tasks.append(
                    asyncio.create_task(
                        copy_local_to_remote(
                            connector=self,
                            location=location,
                            src=src,
                            dst=dst,
                            writer_command=["tar", "xf", "-", "-C", "/"],
                        )
                    )
                )
        # Delegate bind transfers to the inner connector, preventing symbolic links
        for dst, locations in bind_locations.items():
            copy_tasks.append(
                asyncio.create_task(
                    self.connector.copy_local_to_remote(
                        src=src, dst=dst, locations=locations, read_only=False
                    )
                )
            )
        await asyncio.gather(*copy_tasks)

    async def copy_remote_to_local(
        self,
        src: str,
        dst: str,
        location: ExecutionLocation,
        read_only: bool = False,
    ) -> None:
        instance = await self._get_instance(location.name)
        # Check if the container user is the current host user
        # and the source path is on a mounted volume in the source location
        if (
            instance.current_user
            and (adjusted_src := self._get_host_path(instance, src)) is not None
        ):
            # If data is read_only, check if the destination path is bound to a mounted volume, too
            if (
                read_only
                and self._wraps_local()
                and not os.path.exists(dst)
                and self._get_container_path(instance, dst) is not None
            ):
                # If yes, then create a symbolic link
                if logger.isEnabledFor(logging.INFO):
                    logger.info(
                        f"COPYING from {adjusted_src} to {dst} on local file-system"
                    )
                os.symlink(adjusted_src, dst)
                if logger.isEnabledFor(logging.INFO):
                    logger.info(
                        f"COMPLETED copy from {adjusted_src} to {dst} on local file-system"
                    )
            # Otherwise, delegate transfer to the inner connector
            else:
                logger.debug(
                    f"Delegating the transfer of {adjusted_src} "
                    f"to {dst} from location {location.name} "
                    f"to the local file-system {location.name} to the inner "
                    f"{self.connector.__class__.__name__} connector."
                )
                await self.connector.copy_remote_to_local(
                    src=adjusted_src, dst=dst, location=location, read_only=read_only
                )
        # Otherwise, check if the destination path is bound to a mounted volume
        elif (adjusted_dst := self._get_container_path(instance, dst)) is not None:
            # If yes, perform a copy command through the container connector
            await self._local_copy(
                src=src, dst=adjusted_dst, location=location, read_only=read_only
            )
        else:
            # If not, perform a transfer through the container connector
            await copy_remote_to_local(
                connector=self,
                location=location,
                src=src,
                dst=dst,
                reader_command=["tar", "chf", "-", "-C", *posixpath.split(src)],
            )

    async def copy_remote_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[ExecutionLocation],
        source_location: ExecutionLocation,
        source_connector: Connector | None = None,
        read_only: bool = False,
    ) -> None:
        source_connector = source_connector or self
        # Check if the source path is on a mounted volume on the source location
        if (
            source_connector == self
            and (
                host_src := self._get_host_path(
                    await self._get_instance(source_location.name), src
                )
            )
            is not None
        ):
            # If performing a transfer between two containers that wrap a LocalConnector,
            # then perform a local-to-remote copy
            if self._wraps_local():
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"Performing a local-to-remote copy of {host_src} "
                        f"to {dst} from location {source_location.name} to "
                        f"to locations {', '.join(loc.name for loc in locations)} "
                        f"through the {self.__class__.__name__} copy strategy."
                    )
                await self.copy_local_to_remote(
                    src=host_src, dst=dst, locations=locations, read_only=read_only
                )
            # Otherwise, check for optimizations
            else:
                effective_locations = await self._get_effective_locations(
                    locations, dst
                )
                bind_locations = {}
                unbound_locations = []
                copy_tasks = []
                for location in effective_locations:
                    instance = await self._get_instance(location.name)
                    # Check if the source path is on a mounted volume
                    if (
                        adjusted_src := self._get_container_path(instance, host_src)
                    ) is not None:
                        # If yes, perform a copy command through the container connector
                        copy_tasks.append(
                            asyncio.create_task(
                                self._local_copy(
                                    src=adjusted_src,
                                    dst=dst,
                                    location=location,
                                    read_only=read_only,
                                )
                            )
                        )
                    # Otherwise, check if the container user is the current host user
                    # and the destination path is on a mounted volume
                    elif (
                        instance.current_user
                        and (adjusted_dst := self._get_host_path(instance, dst))
                        is not None
                    ):
                        # If yes, then delegate transfer to the inner connector
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(
                                f"Delegating the transfer of {src} "
                                f"to {adjusted_dst} from location {source_location.name} "
                                f"to location {location.name} to the inner "
                                f"{self.connector.__class__.__name__} connector."
                            )
                        bind_locations.setdefault(adjusted_dst, []).append(location)
                    # Otherwise, mask the location as not bound
                    else:
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(
                                f"Copying from {src} to {dst} from location {source_location.name} to "
                                f"location {location.name} through the "
                                f"{self.__class__.__name__} copy strategy."
                            )
                        unbound_locations.append(location)
                # Delegate bind transfers to the inner connector, preventing symbolic links
                for host_dst, locs in bind_locations.items():
                    copy_tasks.append(
                        asyncio.create_task(
                            self.connector.copy_local_to_remote(
                                src=host_src,
                                dst=host_dst,
                                locations=locs,
                                read_only=False,
                            )
                        )
                    )
                # Perform a standard remote-to-remote copy for unbound locations
                copy_tasks.append(
                    asyncio.create_task(
                        copy_remote_to_remote(
                            connector=self,
                            locations=unbound_locations,
                            src=src,
                            dst=dst,
                            source_connector=source_connector,
                            source_location=source_location,
                        )
                    )
                )
                # Wait for all copy tasks to finish
                await asyncio.gather(*copy_tasks)
        # Otherwise, perform a standard remote-to-remote copy
        else:
            if locations := await copy_same_connector(
                connector=self,
                locations=await self._get_effective_locations(
                    locations, dst, source_location
                ),
                source_location=source_location,
                src=src,
                dst=dst,
                read_only=read_only,
            ):
                await copy_remote_to_remote(
                    connector=self,
                    locations=locations,
                    src=src,
                    dst=dst,
                    source_connector=source_connector,
                    source_location=source_location,
                )

    async def _get_effective_locations(
        self,
        locations: MutableSequence[ExecutionLocation],
        dst_path: str,
        source_location: ExecutionLocation | None = None,
    ) -> MutableSequence[ExecutionLocation]:
        common_paths = {}
        effective_locations = []
        for location in locations:
            common_paths, effective_locations = await self._check_effective_location(
                common_paths, effective_locations, location, dst_path, source_location
            )
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                f"Extracted effective locations {','.join(loc.name for loc in effective_locations)} "
                f"from initial locations {','.join(loc.name for loc in locations)}"
            )
        return effective_locations

    def _get_container_path(self, instance: ContainerInstance, path: str) -> str | None:
        for volume in instance.volumes.values():
            if volume.bind is not None and path.startswith(volume.bind):
                path_processor = os.path if self._wraps_local() else posixpath
                return posixpath.join(
                    volume.mount_point,
                    *path_processor.split(path_processor.relpath(path, volume.bind)),
                )
        return None

    def _get_host_path(self, instance: ContainerInstance, path: str) -> str | None:
        for volume in instance.volumes.values():
            if volume.bind is not None and path.startswith(volume.mount_point):
                path_processor = os.path if self._wraps_local() else posixpath
                return path_processor.join(
                    volume.bind,
                    *posixpath.split(posixpath.relpath(path, volume.mount_point)),
                )
        return None

    @abstractmethod
    async def _get_instance(self, location: str) -> ContainerInstance: ...

    @abstractmethod
    def _get_run_command(
        self, command: str, location: ExecutionLocation, interactive: bool = False
    ) -> MutableSequence[str]: ...

    async def _prepare_volumes(
        self, binds: MutableSequence[str] | None, mounts: MutableSequence[str] | None
    ):
        sources = [b.split(":", 2)[0] for b in binds] if binds is not None else []
        for m in mounts if mounts is not None else []:
            mount_type = next(
                part[5:] for part in m.split(",") if part.startswith("type=")
            )
            if mount_type == "bind":
                sources.append(
                    next(
                        part[4:]
                        for part in m.split(",")
                        if part.startswith("src=") or part.startswith("source=")
                    )
                )
        if self._wraps_local():
            for src in sources:
                os.makedirs(src, exist_ok=True)
        else:
            await self.connector.run(
                location=self._get_inner_location(),
                command=["mkdir", "-p"] + sources,
            )

    def _wraps_local(self) -> bool:
        if self._inner_location is None:
            raise ValueError(
                f"The _inner_location parameter of {self.__class__.__name__} "
                "should not be null at this time"
            )
        return self._inner_location.local

    async def deploy(self, external: bool) -> None:
        # Retrieve the underlying location
        locations = await self.connector.get_available_locations(service=self.service)
        if len(locations) != 1:
            raise WorkflowDefinitionException(
                f"{self.__class__.__name__} connectors support only nested connectors with a single location. "
                f"{self.connector.deployment_name} returned {len(locations)} available locations."
            )
        else:
            self._inner_location = next(iter(locations.values()))

    async def get_stream_reader(
        self, command: MutableSequence[str], location: ExecutionLocation
    ) -> AsyncContextManager[StreamWrapper]:
        return await self.connector.get_stream_reader(
            command=self._get_run_command(
                command=utils.encode_command(" ".join(command), "sh"),
                location=location,
                interactive=False,
            ),
            location=get_inner_location(location),
        )

    async def get_stream_writer(
        self, command: MutableSequence[str], location: ExecutionLocation
    ) -> AsyncContextManager[StreamWrapper]:
        encoded_command = base64.b64encode(" ".join(command).encode("utf-8")).decode(
            "utf-8"
        )
        return await self.connector.get_stream_writer(
            command=self._get_run_command(
                command=f"eval $(echo {encoded_command} | base64 -d)",
                location=location,
                interactive=True,
            ),
            location=get_inner_location(location),
        )

    async def run(
        self,
        location: ExecutionLocation,
        command: MutableSequence[str],
        environment: MutableMapping[str, str] | None = None,
        workdir: str | None = None,
        stdin: int | str | None = None,
        stdout: int | str = asyncio.subprocess.STDOUT,
        stderr: int | str = asyncio.subprocess.STDOUT,
        capture_output: bool = False,
        timeout: int | None = None,
        job_name: str | None = None,
    ) -> tuple[str, int] | None:
        command = utils.create_command(
            self.__class__.__name__,
            command,
            environment,
            workdir,
            stdin,
            stdout,
            stderr,
        )
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "EXECUTING command {command} on {location} {job}".format(
                    command=command,
                    location=location,
                    job=f"for job {job_name}" if job_name else "",
                )
            )
        return await self.connector.run(
            location=get_inner_location(location),
            command=self._get_run_command(
                command=utils.encode_command(command, "sh"),
                location=location,
            ),
            capture_output=capture_output,
            timeout=timeout,
            job_name=job_name,
        )


class DockerBaseConnector(ContainerConnector, ABC):
    def __init__(
        self,
        deployment_name: str,
        config_dir: str,
        connector: Connector,
        service: str | None,
        transferBufferSize: int,
    ):
        super().__init__(
            deployment_name=deployment_name,
            config_dir=config_dir,
            connector=connector,
            service=service,
            transferBufferSize=transferBufferSize,
        )

    async def _check_docker_installed(self) -> None:
        if self._wraps_local():
            returncode = 0 if which("docker") is not None else 1
        else:
            _, returncode = await self.connector.run(
                location=self._get_inner_location(),
                command=["which", "docker"],
                capture_output=True,
            )
        if returncode != 0:
            raise WorkflowExecutionException(
                "Docker must be installed on the system to use the "
                f"{self.__class__.__name__} connector."
            )

    async def _get_docker_version(self) -> str:
        stdout, _ = await self.connector.run(
            location=self._get_inner_location(),
            command=["docker", "version", "--format", "'{{.Client.Version}}'"],
            capture_output=True,
        )
        return stdout

    def _get_run_command(
        self, command: str, location: ExecutionLocation, interactive: bool = False
    ) -> MutableSequence[str]:
        return [
            "docker",
            "exec",
            "-i" if interactive else "",
            location.name,
            "sh",
            "-c",
            f"'{command}'",
        ]

    async def _populate_instance(self, name: str):
        # Build execution location
        location = ExecutionLocation(
            name=name,
            deployment=self.deployment_name,
            stacked=True,
            wraps=self._get_inner_location(),
        )
        # Inspect Docker container
        stdout, returncode = await self.connector.run(
            location=self._get_inner_location(),
            command=[
                "docker",
                "inspect",
                "--format",
                "'{{json .}}'",
                name,
            ],
            capture_output=True,
        )
        if returncode == 0:
            try:
                container = json.loads(stdout) if stdout else {}
            except json.decoder.JSONDecodeError:
                raise WorkflowExecutionException(
                    f"Error inspecting Docker container {name}: {stdout}"
                )
        else:
            raise WorkflowExecutionException(
                f"Error inspecting Docker container {name}: [{returncode}] {stdout}"
            )
        # Check if the container user is the current host user
        if self._wraps_local():
            host_user = os.getuid()
        else:
            stdout, returncode = await self.connector.run(
                location=self._get_inner_location(),
                command=["id", "-u"],
                capture_output=True,
            )
            if returncode == 0:
                try:
                    host_user = int(stdout)
                except ValueError:
                    raise WorkflowExecutionException(
                        f"Error retrieving volumes for Docker container {name}: {stdout}"
                    )
            else:
                raise WorkflowExecutionException(
                    f"Error retrieving volumes for Docker container {name}: [{returncode}] {stdout}"
                )
        stdout, returncode = await self.run(
            location=location,
            command=["id", "-u"],
            capture_output=True,
        )
        if returncode == 0:
            try:
                container_user = int(stdout)
            except ValueError:
                raise WorkflowExecutionException(
                    f"Error retrieving volumes for Docker container {name}: {stdout}"
                )
        else:
            raise WorkflowExecutionException(
                f"Error retrieving volumes for Docker container {name}: [{returncode}] {stdout}"
            )
        # Retrieve cores and memory
        stdout, returncode = await self.run(
            location=location,
            command=["test", "-e", "/sys/fs/cgroup/cpuset"],
            capture_output=True,
        )
        if returncode > 1:
            raise WorkflowExecutionException(
                f"Error retrieving cores for Docker container {name}: [{returncode}] {stdout}"
            )
        elif returncode == 0:
            # Handle Cgroups V1 filesystem
            stdout, returncode = await self.run(
                location=location,
                command=[
                    "cat",
                    "/sys/fs/cgroup/cpu/cpu.cfs_quota_us",
                    "/sys/fs/cgroup/cpu/cpu.cfs_period_us",
                    "/sys/fs/cgroup/cpuset/cpuset.effective_cpus",
                    "/sys/fs/cgroup/memory/memory.limit_in_bytes",
                ],
                capture_output=True,
            )
            quota, period, cpuset, memory = stdout.splitlines()
            if int(quota) == -1:
                quota = "max"
            if int(memory) == 9223372036854771712:
                memory = "max"
        else:
            # Handle Cgroups V2 filesystem
            stdout, returncode = await self.run(
                location=location,
                command=[
                    "cat",
                    "/sys/fs/cgroup/cpu.max",
                    "/sys/fs/cgroup/cpuset.cpus.effective",
                    "/sys/fs/cgroup/memory.max",
                ],
                capture_output=True,
            )
            cfs, cpuset, memory = stdout.splitlines()
            quota, period = cfs.split()
        if returncode == 0:
            if quota != "max":
                cores = (int(quota) / 10**5) * (int(period) / 10**5)
            else:
                cores = 0.0
                for s in cpuset.split(","):
                    if "-" in s:
                        start, end = s.split("-")
                        cores += int(end) - int(start) + 1
                    else:
                        cores += 1
            if memory != "max":
                memory = float(memory) / 2**20
            else:
                stdout, returncode = await self.run(
                    location=location,
                    command=[
                        "cat",
                        "/proc/meminfo",
                        "|",
                        "grep",
                        "MemTotal",
                        "|",
                        "awk",
                        "'{print $2}'",
                    ],
                    capture_output=True,
                )
                if returncode == 0:
                    memory = float(stdout) / 2**10
                else:
                    raise WorkflowExecutionException(
                        f"Error retrieving memory from `/proc/meminfo` for Docker container {name}: "
                        f"[{returncode}] {stdout}"
                    )
        else:
            raise WorkflowExecutionException(
                f"Error retrieving hardware resources for Docker container {name}: [{returncode}] {stdout}"
            )
        # Get storage
        volumes = await _get_storage_from_binds(
            connector=self,
            location=location,
            name=name,
            entity="Docker container",
            binds={
                v["Destination"]: v["Source"]
                for v in container["Mounts"]
                if v["Type"] == "bind"
            },
        )
        # Create instance
        addresses = [
            v["IPAddress"]
            for v in container["NetworkSettings"]["Networks"].values()
            if v["IPAddress"]
        ]
        self._instances[name] = ContainerInstance(
            address=addresses[0] if addresses else "",
            cores=cores,
            memory=memory,
            current_user=host_user == container_user,
            volumes=volumes,
        )


class DockerConnector(DockerBaseConnector):
    def __init__(
        self,
        deployment_name: str,
        config_dir: str,
        connector: Connector,
        service: str | None,
        image: str,
        addHost: MutableSequence[str] | None = None,
        blkioWeight: int | None = None,
        blkioWeightDevice: MutableSequence[int] | None = None,
        capAdd: MutableSequence[str] | None = None,
        capDrop: MutableSequence[str] | None = None,
        cgroupParent: str | None = None,
        cgroupns: str | None = None,
        cidfile: str | None = None,
        command: MutableSequence[str] | None = None,
        containerId: str | None = None,
        cpuPeriod: int | None = None,
        cpuQuota: int | None = None,
        cpuRTPeriod: int | None = None,
        cpuRTRuntime: int | None = None,
        cpuShares: int | None = None,
        cpus: float | None = None,
        cpusetCpus: str | None = None,
        cpusetMems: str | None = None,
        detachKeys: str | None = None,
        device: MutableSequence[str] | None = None,
        deviceCgroupRule: MutableSequence[str] | None = None,
        deviceReadBps: MutableSequence[str] | None = None,
        deviceReadIops: MutableSequence[str] | None = None,
        deviceWriteBps: MutableSequence[str] | None = None,
        deviceWriteIops: MutableSequence[str] | None = None,
        dns: MutableSequence[str] | None = None,
        dnsOptions: MutableSequence[str] | None = None,
        dnsSearch: MutableSequence[str] | None = None,
        domainname: str | None = None,
        entrypoint: str | None = None,
        env: MutableSequence[str] | None = None,
        envFile: MutableSequence[str] | None = None,
        expose: MutableSequence[str] | None = None,
        gpus: MutableSequence[str] | None = None,
        groupAdd: MutableSequence[str] | None = None,
        healthCmd: str | None = None,
        healthInterval: str | None = None,
        healthRetries: int | None = None,
        healthStartPeriod: str | None = None,
        healthTimeout: str | None = None,
        hostname: str | None = None,
        init: bool = True,
        ip: str | None = None,
        ip6: str | None = None,
        ipc: str | None = None,
        isolation: str | None = None,
        kernelMemory: int | None = None,
        label: MutableSequence[str] | None = None,
        labelFile: MutableSequence[str] | None = None,
        link: MutableSequence[str] | None = None,
        linkLocalIP: MutableSequence[str] | None = None,
        logDriver: str | None = None,
        logOpts: MutableSequence[str] | None = None,
        macAddress: str | None = None,
        memory: int | None = None,
        memoryReservation: int | None = None,
        memorySwap: int | None = None,
        memorySwappiness: int | None = None,
        mount: MutableSequence[str] | None = None,
        network: MutableSequence[str] | None = None,
        networkAlias: MutableSequence[str] | None = None,
        noHealthcheck: bool = False,
        oomKillDisable: bool = False,
        oomScoreAdj: int | None = None,
        pid: str | None = None,
        pidsLimit: int | None = None,
        privileged: bool = False,
        publish: MutableSequence[str] | None = None,
        publishAll: bool = False,
        readOnly: bool = False,
        restart: str | None = None,
        rm: bool = True,
        runtime: str | None = None,
        securityOpts: MutableSequence[str] | None = None,
        shmSize: int | None = None,
        sigProxy: bool = True,
        stopSignal: str | None = None,
        stopTimeout: int | None = None,
        storageOpts: MutableSequence[str] | None = None,
        sysctl: MutableSequence[str] | None = None,
        tmpfs: MutableSequence[str] | None = None,
        transferBufferSize: int = 2**16,
        ulimit: MutableSequence[str] | None = None,
        user: str | None = None,
        userns: str | None = None,
        uts: str | None = None,
        volume: MutableSequence[str] | None = None,
        volumeDriver: str | None = None,
        volumesFrom: MutableSequence[str] | None = None,
        workdir: str | None = None,
    ):
        super().__init__(
            deployment_name=deployment_name,
            config_dir=config_dir,
            connector=connector,
            service=service,
            transferBufferSize=transferBufferSize,
        )
        self.image: str = image
        self.addHost: MutableSequence[str] | None = addHost
        self.blkioWeight: int | None = blkioWeight
        self.blkioWeightDevice: MutableSequence[int] | None = blkioWeightDevice
        self.capAdd: MutableSequence[str] | None = capAdd
        self.capDrop: MutableSequence[str] | None = capDrop
        self.cgroupParent: str | None = cgroupParent
        self.cgroupns: str | None = cgroupns
        self.cidfile: str | None = cidfile
        self.containerId: str | None = containerId
        self.command: MutableSequence[str] = command
        self.cpuPeriod: int | None = cpuPeriod
        self.cpuQuota: int | None = cpuQuota
        self.cpuRTPeriod: int | None = cpuRTPeriod
        self.cpuRTRuntime: int | None = cpuRTRuntime
        self.cpuShares: int | None = cpuShares
        self.cpus: float | None = cpus
        self.cpusetCpus: str | None = cpusetCpus
        self.cpusetMems: str | None = cpusetMems
        self.detachKeys: str | None = detachKeys
        self.device: MutableSequence[str] | None = device
        self.deviceCgroupRule: MutableSequence[str] | None = deviceCgroupRule
        self.deviceReadBps: MutableSequence[str] | None = deviceReadBps
        self.deviceReadIops: MutableSequence[str] | None = deviceReadIops
        self.deviceWriteBps: MutableSequence[str] | None = deviceWriteBps
        self.deviceWriteIops: MutableSequence[str] | None = deviceWriteIops
        self.dns: MutableSequence[str] | None = dns
        self.dnsOptions: MutableSequence[str] | None = dnsOptions
        self.dnsSearch: MutableSequence[str] | None = dnsSearch
        self.domainname: str | None = domainname
        self.entrypoint: str | None = entrypoint
        self.env: MutableSequence[str] | None = env
        self.envFile: MutableSequence[str] | None = envFile
        self.expose: MutableSequence[str] | None = expose
        self.gpus: MutableSequence[str] | None = gpus
        self.groupAdd: MutableSequence[str] | None = groupAdd
        self.healthCmd: str | None = healthCmd
        self.healthInterval: str | None = healthInterval
        self.healthRetries: int | None = healthRetries
        self.healthStartPeriod: str | None = healthStartPeriod
        self.healthTimeout: str | None = healthTimeout
        self.hostname: str | None = hostname
        self.init: bool = init
        self.ip: str | None = ip
        self.ip6: str | None = ip6
        self.ipc: str | None = ipc
        self.isolation: str | None = isolation
        self.kernelMemory: int | None = kernelMemory
        self.label: MutableSequence[str] | None = label
        self.labelFile: MutableSequence[str] | None = labelFile
        self.link: MutableSequence[str] | None = link
        self.linkLocalIP: MutableSequence[str] | None = linkLocalIP
        self.logDriver: str | None = logDriver
        self.logOpts: MutableSequence[str] | None = logOpts
        self.macAddress: str | None = macAddress
        self.memory: int | None = memory
        self.memoryReservation: int | None = memoryReservation
        self.memorySwap: int | None = memorySwap
        self.memorySwappiness: int | None = memorySwappiness
        self.mount: MutableSequence[str] | None = mount
        self.network: MutableSequence[str] | None = network
        self.networkAlias: MutableSequence[str] | None = networkAlias
        self.noHealthcheck: bool = noHealthcheck
        self.oomKillDisable: bool = oomKillDisable
        self.oomScoreAdj: int | None = oomScoreAdj
        self.pid: str | None = pid
        self.pidsLimit: int | None = pidsLimit
        self.privileged: bool = privileged
        self.publish: MutableSequence[str] | None = publish
        self.publishAll: bool = publishAll
        self.readOnly: bool = readOnly
        self.restart: str | None = restart
        self.rm: bool = rm
        self.runtime: str | None = runtime
        self.securityOpts: MutableSequence[str] | None = securityOpts
        self.shmSize: int | None = shmSize
        self.sigProxy: bool = sigProxy
        self.stopSignal: str | None = stopSignal
        self.stopTimeout: int | None = stopTimeout
        self.storageOpts: MutableSequence[str] | None = storageOpts
        self.sysctl: MutableSequence[str] | None = sysctl
        self.tmpfs: MutableSequence[str] | None = tmpfs
        self.ulimit: MutableSequence[str] | None = ulimit
        self.user: str | None = user
        self.userns: str | None = userns
        self.uts: str | None = uts
        self.volume: MutableSequence[str] | None = volume
        self.volumeDriver: str | None = volumeDriver
        self.volumesFrom: MutableSequence[str] | None = volumesFrom
        self.workdir: str | None = workdir

    async def _get_instance(self, location: str) -> ContainerInstance:
        if location not in self._instances:
            raise WorkflowExecutionException(
                f"FAILED retrieving instance for location {location} "
                f"for deployment {self.deployment_name}"
            )
        return self._instances[location]

    async def deploy(self, external: bool) -> None:
        await super().deploy(external)
        # Check if Docker is installed in the wrapped connector
        await self._check_docker_installed()
        # If the deployment is not external, deploy the container
        if not external:
            await self._prepare_volumes(self.volume, self.mount)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Using Docker {await self._get_docker_version()}.")
            # Pull image if it doesn't exist
            _, returncode = await self.connector.run(
                location=self._get_inner_location(),
                command=["docker", "image", "inspect", self.image],
                capture_output=True,
            )
            if returncode != 0:
                await self.connector.run(
                    location=self._get_inner_location(),
                    command=["docker", "pull", "--quiet", self.image],
                )
            # Deploy the Docker container
            deploy_command = [
                "docker",
                "run",
                "--detach",
                "--interactive",
                get_option("add-host", self.addHost),
                get_option("blkio-weight", self.addHost),
                get_option("blkio-weight-device", self.blkioWeightDevice),
                get_option("cap-add", self.capAdd),
                get_option("cap-drop", self.capDrop),
                get_option("cgroup-parent", self.cgroupParent),
                get_option("cgroupns", self.cgroupns),
                get_option("cidfile", self.cidfile),
                get_option("cpu-period", self.cpuPeriod),
                get_option("cpu-quota", self.cpuQuota),
                get_option("cpu-rt-period", self.cpuRTPeriod),
                get_option("cpu-rt-runtime", self.cpuRTRuntime),
                get_option("cpu-shares", self.cpuShares),
                get_option("cpus", self.cpus),
                get_option("cpuset-cpus", self.cpusetCpus),
                get_option("cpuset-mems", self.cpusetMems),
                get_option("detach-keys", self.detachKeys),
                get_option("device", self.device),
                get_option("device-cgroup-rule", self.deviceCgroupRule),
                get_option("device-read-bps", self.deviceReadBps),
                get_option("device-read-iops", self.deviceReadIops),
                get_option("device-write-bps", self.deviceWriteBps),
                get_option("device-write-iops", self.deviceWriteIops),
                get_option("dns", self.dns),
                get_option("dns-option", self.dnsOptions),
                get_option("dns-search", self.dnsSearch),
                get_option("domainname", self.domainname),
                get_option("entrypoint", self.entrypoint),
                get_option("env", self.env),
                get_option("env-file", self.envFile),
                get_option("expose", self.expose),
                get_option("gpus", self.gpus),
                get_option("group-add", self.groupAdd),
                get_option("health-cmd", self.healthCmd),
                get_option("health-interval", self.healthInterval),
                get_option("health-retries", self.healthRetries),
                get_option("health-start-period", self.healthStartPeriod),
                get_option("health-timeout", self.healthTimeout),
                get_option("hostname", self.hostname),
                get_option("init", self.init),
                get_option("ip", self.ip),
                get_option("ip6", self.ip6),
                get_option("ipc", self.ipc),
                get_option("isolation", self.isolation),
                get_option("kernel-memory", self.kernelMemory),
                get_option("label", self.label),
                get_option("label-file", self.labelFile),
                get_option("link", self.link),
                get_option("link-local-ip", self.linkLocalIP),
                get_option("log-driver", self.logDriver),
                get_option("log-opt", self.logOpts),
                get_option("mac-address", self.macAddress),
                get_option("memory", self.memory),
                get_option("memory-reservation", self.memoryReservation),
                get_option("memory-swap", self.memorySwap),
                get_option("memory-swappiness", self.memorySwappiness),
                get_option("mount", self.mount),
                get_option("network", self.network),
                get_option("network-alias", self.networkAlias),
                get_option("no-healthcheck", self.noHealthcheck),
                get_option("oom-kill-disable", self.oomKillDisable),
                get_option("oom-score-adj", self.oomScoreAdj),
                get_option("pid", self.pid),
                get_option("pids-limit", self.pidsLimit),
                get_option("privileged", self.privileged),
                get_option("publish", self.publish),
                get_option("publish-all", self.publishAll),
                get_option("read-only", self.readOnly),
                get_option("restart", self.restart),
                get_option("rm", self.rm),
                get_option("runtime", self.runtime),
                get_option("security-opt", self.securityOpts),
                get_option("shm-size", self.shmSize),
                f"--sig-proxy={'true' if self.sigProxy else 'false'}",
                get_option("stop-signal", self.stopSignal),
                get_option("stop-timeout", self.stopTimeout),
                get_option("storage-opt", self.storageOpts),
                get_option("sysctl", self.sysctl),
                get_option("tmpfs", self.tmpfs),
                get_option("ulimit", self.ulimit),
                get_option("user", self.user),
                get_option("userns", self.userns),
                get_option("uts", self.uts),
                get_option("volume", self.volume),
                get_option("volume-driver", self.volumeDriver),
                get_option("volumes-from", self.volumesFrom),
                get_option("workdir", self.workdir),
                self.image,
                f"{' '.join(self.command) if self.command else ''}",
            ]
            stdout, returncode = await self.connector.run(
                location=self._get_inner_location(),
                command=deploy_command,
                capture_output=True,
            )
            if returncode == 0:
                self.containerId = stdout
            else:
                raise WorkflowExecutionException(
                    f"FAILED Deployment of {self.deployment_name} environment [{returncode}]:\n\t{stdout}"
                )
        # Otherwise, check if a containerId has been explicitly specified
        elif self.containerId is None:
            raise WorkflowDefinitionException(
                f"FAILED Deployment of {self.deployment_name} environment:\n\t"
                "external Docker deployments must specify the containerId of an existing Docker container"
            )
        # Populate instance
        await self._populate_instance(self.containerId)

    async def get_available_locations(
        self, service: str | None = None
    ) -> MutableMapping[str, AvailableLocation]:
        if self._inner_location is None:
            raise ValueError(
                f"The _inner_location parameter of {self.__class__.__name__} "
                "should not be null at this time"
            )
        instance = self._instances[self.containerId]
        return {
            self.containerId: AvailableLocation(
                name=self.containerId,
                deployment=self.deployment_name,
                hostname=instance.address,
                hardware=Hardware(
                    cores=instance.cores,
                    memory=instance.memory,
                    storage=instance.volumes,
                ),
                stacked=True,
                wraps=self._inner_location,
            )
        }

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("docker.json")
            .read_text("utf-8")
        )

    async def undeploy(self, external: bool) -> None:
        if not external:
            stdout, returncode = await self.connector.run(
                location=self._get_inner_location(),
                command=["docker", "stop", self.containerId],
                capture_output=True,
            )
            if returncode != 0:
                raise WorkflowExecutionException(
                    f"FAILED Undeployment of {self.deployment_name} environment [{returncode}]:\n\t{stdout}"
                )


class DockerComposeConnector(DockerBaseConnector):
    def __init__(
        self,
        deployment_name: str,
        config_dir: str,
        connector: Connector,
        service: str | None,
        files: MutableSequence[str],
        projectName: str | None = None,
        verbose: bool | None = False,
        logLevel: str | None = None,
        noAnsi: bool | None = False,
        host: str | None = None,
        tls: bool | None = False,
        tlscacert: str | None = None,
        tlscert: str | None = None,
        tlskey: str | None = None,
        tlsverify: bool | None = False,
        skipHostnameCheck: bool | None = False,
        projectDirectory: str | None = None,
        compatibility: bool | None = False,
        noDeps: bool | None = False,
        forceRecreate: bool | None = False,
        alwaysRecreateDeps: bool | None = False,
        noRecreate: bool | None = False,
        noBuild: bool | None = False,
        noStart: bool | None = False,
        build: bool | None = False,
        timeout: int | None = None,
        transferBufferSize: int = 2**16,
        renewAnonVolumes: bool | None = False,
        removeOrphans: bool | None = False,
        removeVolumes: bool | None = False,
        wait: bool = True,
    ) -> None:
        super().__init__(
            deployment_name=deployment_name,
            config_dir=config_dir,
            connector=connector,
            service=service,
            transferBufferSize=transferBufferSize,
        )
        self.files = [
            file if os.path.isabs(file) else os.path.join(self.config_dir, file)
            for file in files
        ]
        self.alwaysRecreateDeps: bool | None = alwaysRecreateDeps
        self.build: bool | None = build
        self.compatibility: bool | None = compatibility
        self.forceRecreate: bool | None = forceRecreate
        self.host: str | None = host
        self.logLevel: str | None = logLevel
        self.noAnsi: bool | None = noAnsi
        self.noBuild: bool | None = noBuild
        self.noDeps: bool | None = noDeps
        self.noRecreate: bool | None = noRecreate
        self.noStart: bool | None = noStart
        self.projectDirectory: str | None = projectDirectory
        self.projectName: str | None = projectName
        self.renewAnonVolumes: bool | None = renewAnonVolumes
        self.removeOrphans: bool | None = removeOrphans
        self.removeVolumes: bool | None = removeVolumes
        self.skipHostnameCheck: bool | None = skipHostnameCheck
        self.timeout: int | None = timeout
        self.tls: bool | None = tls
        self.tlscacert: str | None = tlscacert
        self.tlscert: str | None = tlscert
        self.tlskey: str | None = tlskey
        self.tlsverify: bool | None = tlsverify
        self.verbose: bool | None = verbose
        self.wait: bool | None = wait
        self._command: list[str] | None = None
        self._instance_lock: asyncio.Lock = asyncio.Lock()

    async def _check_docker_compose_installed(self) -> None:
        if self._wraps_local():
            returncode = 0 if which("docker-compose") is not None else 1
        else:
            _, returncode = await self.connector.run(
                location=self._get_inner_location(),
                command=["which", "docker-compose"],
                capture_output=True,
            )
        if returncode != 0:
            raise WorkflowExecutionException(
                "Docker Compose must be installed on the system to use the "
                f"{self.__class__.__name__} connector."
            )

    async def _get_base_command(self) -> list[str]:
        if self._command is None:
            compose_command = await self._get_docker_compose_command()
            # Get Docker Compose version
            version, _ = await self.connector.run(
                location=self._get_inner_location(),
                command=compose_command + ["version", "--short"],
                capture_output=True,
            )
            if version.startswith("v"):
                version = version[1:]
            # Check version compatibility
            major = int(version.split(".")[0])
            if not major >= 2:
                raise WorkflowExecutionException(
                    f"Docker Compose {version} is not compatible with DockerComposeConnector. "
                    f"Docker Compose version 2.x or later is required."
                )
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"Using Docker {await self._get_docker_version()} "
                    f"and Docker Compose {version}."
                )
            self._command = compose_command + [
                get_option("file", self.files),
                get_option("project-name", self.projectName),
                get_option("verbose", self.verbose),
                get_option("no-ansi", self.noAnsi),
                get_option("skip-hostname-check", self.skipHostnameCheck),
                get_option("project-directory", self.projectDirectory),
                get_option("compatibility", self.compatibility),
            ]
        return self._command

    async def _get_docker_compose_command(self) -> list[str]:
        options = [
            get_option("log-level", self.logLevel),
            get_option("host", self.host),
            get_option("tls", self.tls),
            get_option("tlscacert", self.tlscacert),
            get_option("tlscert", self.tlscert),
            get_option("tlskey", self.tlskey),
            get_option("tlsverify", self.tlsverify),
        ]
        # Check if `docker compose` is a valid command
        _, returncode = await self.connector.run(
            location=self._get_inner_location(),
            command=["docker", "compose"],
            capture_output=True,
        )
        if returncode == 0:
            return ["docker"] + options + ["compose"]
        # Otherwise, check if `docker-compose` is installed
        else:
            await self._check_docker_compose_installed()
            return ["docker-compose"] + options

    async def _get_instance(self, location: str) -> ContainerInstance:
        if location not in self._instances:
            async with self._instance_lock:
                if location not in self._instances:
                    await self._populate_instance(location)
        return self._instances[location]

    async def deploy(self, external: bool) -> None:
        await super().deploy(external)
        # Check if Docker is installed in the wrapped connector
        await self._check_docker_installed()
        # If the deployment is not external, deploy the manifest
        if not external:
            deploy_command = await self._get_base_command() + [
                "up",
                "--detach",
                get_option("always-recreate-deps", self.alwaysRecreateDeps),
                get_option("build", self.build),
                get_option("force-recreate", self.forceRecreate),
                get_option("no-build", self.noBuild),
                get_option("no-deps ", self.noDeps),
                get_option("no-recreate", self.noRecreate),
                get_option("no-start", self.noStart),
                get_option("timeout", self.timeout),
                get_option("wait", self.wait),
            ]
            stdout, returncode = await self.connector.run(
                location=self._get_inner_location(),
                command=deploy_command,
                capture_output=True,
            )
            if returncode != 0:
                raise WorkflowExecutionException(
                    f"FAILED Deployment of {self.deployment_name} environment [{returncode}]:\n\t{stdout}"
                )

    async def get_available_locations(
        self, service: str | None = None
    ) -> MutableMapping[str, AvailableLocation]:
        output, _ = await self.connector.run(
            location=self._get_inner_location(),
            command=await self._get_base_command()
            + ["ps", "--format", "json", service or ""],
            capture_output=True,
        )
        if ((json_start := output.find("{")) != -1) and (
            (json_end := output.rfind("}")) != -1
        ):
            if json_start != 0 and logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Docker Compose log: {output[:json_start].strip()}")
            locations = json.loads(output[json_start : json_end + 1])
        else:
            raise WorkflowExecutionException(
                f"Error retrieving locations for Docker Compose deployment {self.deployment_name}: "
                f"{output}"
            )
        if not isinstance(locations, MutableSequence):
            locations = [locations]
        instances = {
            loc["Name"]: v
            for loc, v in zip(
                locations,
                await asyncio.gather(
                    *(
                        asyncio.create_task(self._get_instance(location["Name"]))
                        for location in locations
                    )
                ),
                strict=True,
            )
        }
        return {
            k: AvailableLocation(
                name=k,
                deployment=self.deployment_name,
                hostname=instance.address,
                hardware=Hardware(
                    cores=instance.cores,
                    memory=instance.memory,
                    storage=instance.volumes,
                ),
                stacked=True,
                wraps=self._inner_location,
            )
            for k, instance in instances.items()
        }

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("docker-compose.json")
            .read_text("utf-8")
        )

    async def undeploy(self, external: bool) -> None:
        if not external:
            stdout, returncode = await self.connector.run(
                location=self._get_inner_location(),
                command=await self._get_base_command()
                + ["down", get_option("volumes", self.removeVolumes)],
                capture_output=True,
            )
            if returncode != 0:
                raise WorkflowExecutionException(
                    f"FAILED Undeployment of {self.deployment_name} environment [{returncode}]:\n\t{stdout}"
                )


class SingularityConnector(ContainerConnector):
    def __init__(
        self,
        deployment_name: str,
        config_dir: str,
        connector: Connector,
        service: str | None,
        image: str,
        addCaps: str | None = None,
        allowSetuid: bool = False,
        applyCgroups: str | None = None,
        bind: MutableSequence[str] | None = None,
        blkioWeight: int | None = None,
        blkioWeightDevice: MutableSequence[str] | None = None,
        boot: bool = False,
        cleanenv: bool = False,
        command: MutableSequence[str] | None = None,
        compat: bool = False,
        contain: bool = False,
        containall: bool = False,
        cpuShares: int | None = None,
        cpus: str | None = None,
        cpusetCpus: str | None = None,
        cpusetMems: str | None = None,
        disableCache: bool = False,
        dns: str | None = None,
        dockerHost: str | None = None,
        dropCaps: str | None = None,
        env: MutableSequence[str] | None = None,
        envFile: str | None = None,
        fakeroot: bool = False,
        fusemount: MutableSequence[str] | None = None,
        home: str | None = None,
        hostname: str | None = None,
        instanceName: str | None = None,
        ipc: bool = False,
        keepPrivs: bool = False,
        memory: str | None = None,
        memoryReservation: str | None = None,
        memorySwap: str | None = None,
        mount: MutableSequence[str] | None = None,
        net: bool = False,
        network: str | None = None,
        networkArgs: MutableSequence[str] | None = None,
        noEval: bool = False,
        noHome: bool = False,
        noHttps: bool = False,
        noInit: bool = False,
        noMount: MutableSequence[str] | None = None,
        noPrivs: bool = False,
        noUmask: bool = False,
        nv: bool = False,
        nvccli: bool = False,
        oomKillDisable: bool = False,
        overlay: MutableSequence[str] | None = None,
        pemPath: str | None = None,
        pidFile: str | None = None,
        pidsLimit: int | None = None,
        rocm: bool = False,
        scratch: MutableSequence[str] | None = None,
        security: MutableSequence[str] | None = None,
        transferBufferSize: int = 2**16,
        userns: bool = False,
        uts: bool = False,
        workdir: str | None = None,
        writable: bool = False,
        writableTmpfs: bool = False,
    ):
        super().__init__(
            deployment_name=deployment_name,
            config_dir=config_dir,
            connector=connector,
            service=service,
            transferBufferSize=transferBufferSize,
        )
        self.image: str = image
        self.addCaps: str | None = addCaps
        self.allowSetuid: bool = allowSetuid
        self.applyCgroups: str | None = applyCgroups
        self.bind: MutableSequence[str] | None = bind
        self.blkioWeight: int | None = blkioWeight
        self.blkioWeightDevice: MutableSequence[str] | None = blkioWeightDevice
        self.boot: bool = boot
        self.cleanenv: bool = cleanenv
        self.command: MutableSequence[str] = command or []
        self.compat: bool = compat
        self.contain: bool = contain
        self.containall: bool = containall
        self.cpuShares: int | None = cpuShares
        self.cpus: str | None = cpus
        self.cpusetCpus: str | None = cpusetCpus
        self.cpusetMems: str | None = cpusetMems
        self.disableCache: bool = disableCache
        self.dns: str | None = dns
        self.dropCaps: str | None = dropCaps
        self.dockerHost: str | None = dockerHost
        self.env: MutableSequence[str] | None = env
        self.envFile: str | None = envFile
        self.fakeroot: bool = fakeroot
        self.fusemount: MutableSequence[str] | None = fusemount
        self.home: str | None = home
        self.hostname: str | None = hostname
        self.ipc: bool = ipc
        self.instanceName: str | None = instanceName
        self.keepPrivs: bool = keepPrivs
        self.memory: str | None = memory
        self.memoryReservation: str | None = memoryReservation
        self.memorySwap: str | None = memorySwap
        self.mount: MutableSequence[str] | None = mount
        self.net: bool = net
        self.network: str | None = network
        self.networkArgs: MutableSequence[str] | None = networkArgs
        self.noEval: bool = noEval
        self.noHome: bool = noHome
        self.noHttps: bool = noHttps
        self.noInit: bool = noInit
        self.noMount: MutableSequence[str] | None = noMount or []
        self.noPrivs: bool = noPrivs
        self.noUmask: bool = noUmask
        self.nv: bool = nv
        self.nvccli: bool = nvccli
        self.oomKillDisable: bool = oomKillDisable
        self.overlay: MutableSequence[str] | None = overlay
        self.pemPath: str | None = pemPath
        self.pidFile: str | None = pidFile
        self.pidsLimit: int | None = pidsLimit
        self.rocm: bool = rocm
        self.scratch: MutableSequence[str] | None = scratch
        self.security: MutableSequence[str] | None = security
        self.userns: bool = userns
        self.uts: bool = uts
        self.workdir: str | None = workdir
        self.writable: bool = writable
        self.writableTmpfs: bool = writableTmpfs

    async def _check_singularity_installed(self) -> None:
        if self._wraps_local():
            returncode = 0 if which("singularity") is not None else 1
        else:
            _, returncode = await self.connector.run(
                location=self._get_inner_location(),
                command=["which", "singularity"],
                capture_output=True,
            )
        if returncode != 0:
            raise WorkflowExecutionException(
                "Singularity must be installed on the system to use the "
                f"{self.__class__.__name__} connector."
            )

    def _get_run_command(
        self, command: str, location: ExecutionLocation, interactive: bool = False
    ) -> MutableSequence[str]:
        return [
            "singularity",
            "exec",
            get_option("cleanenv", self.cleanenv),
            get_option("contain", self.contain),
            f"instance://{location.name}",
            "sh",
            "-c",
            f"'{command}'",
        ]

    async def _get_singularity_version(self) -> str:
        stdout, _ = await self.connector.run(
            location=self._get_inner_location(),
            command=["singularity", "--version"],
            capture_output=True,
        )
        return stdout

    async def _get_instance(self, location: str) -> ContainerInstance:
        if location not in self._instances:
            raise WorkflowExecutionException(
                f"FAILED retrieving instance for location {location} "
                f"for deployment {self.deployment_name}"
            )
        return self._instances[location]

    async def _populate_instance(self, name: str) -> None:
        # Build execution location
        location = ExecutionLocation(
            name=name,
            deployment=self.deployment_name,
            stacked=True,
            wraps=self._get_inner_location(),
        )
        # Get IP address
        ip_address = None
        stdout, returncode = await self.connector.run(
            location=self._get_inner_location(),
            command=[
                "singularity",
                "instance",
                "list",
                "--json",
            ],
            capture_output=True,
        )
        if returncode == 0:
            json_out = json.loads(stdout)
            for instance in json_out["instances"]:
                if instance["instance"] == name:
                    ip_address = instance["ip"]
                    break
            if ip_address is None:
                raise WorkflowExecutionException(
                    f"FAILED retrieving instance '{name}' from running Singularity instances "
                    f"in deployment {self.deployment_name}: [{returncode}] {stdout}"
                )
        else:
            raise WorkflowExecutionException(
                f"FAILED retrieving running Singularity instances for deployment {self.deployment_name}: "
                f"[{returncode}] {stdout}"
            )
        # Retrieve cores and memory (cgroups are not taken into account for Singularity/Apptainer)
        stdout, returncode = await self.run(
            location=location,
            command=["nproc"],
            capture_output=True,
        )
        if returncode == 0:
            cores = float(stdout)
        else:
            raise WorkflowExecutionException(
                f"FAILED retrieving cores from `nproc` for Singularity instance '{name}' "
                f"in deployment {self.deployment_name}: [{returncode}] {stdout}"
            )
        stdout, returncode = await self.run(
            location=location,
            command=[
                "cat",
                "/proc/meminfo",
                "|",
                "grep",
                "MemTotal",
                "|",
                "awk",
                "'{print $2}'",
            ],
            capture_output=True,
        )
        if returncode == 0:
            memory = float(stdout) / 2**10
        else:
            raise WorkflowExecutionException(
                f"FAILED retrieving memory from `/proc/meminfo` for Singularity instance '{name}' "
                f"in deployment {self.deployment_name}: [{returncode}]: {stdout}"
            )
        # Get inner location mount points
        if self._wraps_local():
            fs_mounts = {
                disk.device: disk.mountpoint
                for disk in psutil.disk_partitions(all=True)
                if disk.fstype not in FS_TYPES_TO_SKIP
                and os.access(disk.mountpoint, os.R_OK)
            }
        else:
            stdout, returncode = await self.connector.run(
                location=self._get_inner_location(),
                command=[
                    "cat",
                    "/proc/1/mountinfo",
                ],
                capture_output=True,
            )
            if returncode == 0:
                fs_mounts = {
                    line.split(" - ")[1].split()[1]: line.split()[4]
                    for line in stdout.splitlines()
                    if line.split(" - ")[1].split()[0] not in FS_TYPES_TO_SKIP
                }
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Host mount points: {fs_mounts}")
            else:
                raise WorkflowExecutionException(
                    f"FAILED retrieving volume mounts from `/proc/1/mountinfo` "
                    f"in deployment {self.connector.deployment_name}: [{returncode}]: {stdout}"
                )
        # Get the list of bind mounts for the container instance
        stdout, returncode = await self.run(
            location=location,
            command=[
                "cat",
                "/proc/1/mountinfo",
            ],
            capture_output=True,
        )
        if returncode == 0:
            binds = {}
            for line in stdout.splitlines():
                if (dst := line.split()[4]) != posixpath.sep and (
                    fs_type := line.split(" - ")[1].split()[0]
                ) not in FS_TYPES_TO_SKIP:
                    mount_source = line.split(" - ")[1].split()[1]
                    if mount_source.startswith("/dev"):
                        host_mount = line.split()[3]
                    elif fs_type.startswith("nfs"):
                        host_mount = None
                        for host_source in sorted(fs_mounts.keys(), reverse=True):
                            if mount_source.startswith(host_source):
                                host_mount = mount_source.split(":", 1)[1]
                                break
                    else:
                        host_mount = (
                            (os.path if self._wraps_local() else posixpath).join(
                                fs_mounts[mount_source], line.split()[3][1:]
                            )
                            if mount_source in fs_mounts
                            else None
                        )
                    if host_mount is not None:
                        binds[dst] = host_mount
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Container binds: {binds}")
        else:
            raise WorkflowExecutionException(
                f"FAILED retrieving bind mounts from `/proc/1/mountinfo` for Singularity instance '{name}' "
                f"in deployment {self.deployment_name}: [{returncode}]: {stdout}"
            )
        # Get storage sizes
        volumes = await _get_storage_from_binds(
            connector=self,
            location=location,
            name=name,
            entity="Singularity instance",
            binds=cast(MutableMapping[str, str], binds),
        )
        # Create instance
        self._instances[name] = ContainerInstance(
            address=ip_address,
            cores=cores,
            memory=memory,
            current_user=True,
            volumes=volumes,
        )

    async def deploy(self, external: bool) -> None:
        await super().deploy(external)
        # Check if Singularity is installed in the wrapped connector
        await self._check_singularity_installed()
        # If the deployment is not external, deploy the container
        if not external:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Using {await self._get_singularity_version()}.")
            await self._prepare_volumes(self.bind, self.mount)
            instance_name = random_name()
            deploy_command = [
                "singularity",
                "instance",
                "start",
                get_option("add-caps", self.addCaps),
                get_option("allow-setuid", self.allowSetuid),
                get_option("apply-cgroups", self.applyCgroups),
                get_option("bind", self.bind),
                get_option("blkio-weight", self.blkioWeight),
                get_option("blkio-weight-device", self.blkioWeightDevice),
                get_option("boot", self.boot),
                get_option("cleanenv", self.cleanenv),
                get_option("compat", self.compat),
                get_option("contain", self.contain),
                get_option("containall", self.containall),
                get_option("cpu-shares", self.cpuShares),
                get_option("cpus", self.cpus),
                get_option("cpuset-cpus", self.cpusetCpus),
                get_option("cpuset-mems", self.cpusetMems),
                get_option("disable-cache", self.disableCache),
                get_option("docker-host", self.dockerHost),
                get_option("dns", self.dns),
                get_option("drop-caps", self.dropCaps),
                get_option("env-file", self.envFile),
                get_option("fakeroot", self.fakeroot),
                get_option("fusemount", self.fusemount),
                get_option("home", self.home),
                get_option("hostname", self.hostname),
                get_option("ipc", self.ipc),
                get_option("keep-privs", self.keepPrivs),
                get_option("memory", self.memory),
                get_option("memory-reservation", self.memoryReservation),
                get_option("memory-swap", self.memorySwap),
                get_option("mount", self.mount),
                get_option("net", self.net),
                get_option("network", self.network),
                get_option("network-args", self.networkArgs),
                get_option("no-eval", self.noEval),
                get_option("no-home", self.noHome),
                get_option("no-https", self.noHttps),
                get_option("no-init", self.noInit),
                get_option("no-mount", self.noMount),
                get_option("no-privs", self.noPrivs),
                get_option("no-umask", self.noUmask),
                get_option("nv", self.nv),
                get_option("nvccli", self.nvccli),
                get_option("oom-kill-disable", self.oomKillDisable),
                get_option("overlay", self.overlay),
                get_option("pem-path", self.pemPath),
                get_option("pid-file", self.pidFile),
                get_option("pids-limit", self.pidsLimit),
                get_option("rocm", self.rocm),
                get_option("scratch", self.scratch),
                get_option("security", self.security),
                get_option("userns", self.userns),
                get_option("uts", self.uts),
                get_option("workdir", self.workdir),
                get_option("writable", self.writable),
                get_option("writable-tmpfs", self.writableTmpfs),
                self.image,
                instance_name,
                " ".join(self.command) if self.command else "",
            ]
            stdout, returncode = await self.connector.run(
                location=self._get_inner_location(),
                command=deploy_command,
                capture_output=True,
            )
            if returncode == 0:
                self.instanceName = instance_name
            else:
                raise WorkflowExecutionException(
                    f"FAILED Deployment of {self.deployment_name} environment:"
                    f"[{returncode}] {stdout}"
                )
        elif self.instanceName is None:
            raise WorkflowDefinitionException(
                f"FAILED Deployment of {self.deployment_name} environment: "
                "external Singularity deployments must specify the instanceName of an existing Singularity container"
            )
        # Populate instance
        await self._populate_instance(self.instanceName)

    async def get_available_locations(
        self, service: str | None = None
    ) -> MutableMapping[str, AvailableLocation]:
        instance = self._instances[self.instanceName]
        return {
            self.instanceName: AvailableLocation(
                name=self.instanceName,
                deployment=self.deployment_name,
                hostname=instance.address,
                hardware=Hardware(
                    cores=instance.cores,
                    memory=instance.memory,
                    storage=instance.volumes,
                ),
                stacked=True,
                wraps=self._inner_location,
            )
        }

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("singularity.json")
            .read_text("utf-8")
        )

    async def undeploy(self, external: bool) -> None:
        if not external:
            stdout, returncode = await self.connector.run(
                location=self._get_inner_location(),
                command=["singularity", "instance", "stop", self.instanceName],
                capture_output=True,
            )
            if returncode != 0:
                raise WorkflowExecutionException(
                    f"FAILED Undeployment of {self.deployment_name} environment [{returncode}]:\n\t{stdout}"
                )
