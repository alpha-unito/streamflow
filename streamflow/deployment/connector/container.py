from __future__ import annotations

import asyncio
import json
import logging
import os
import posixpath
import shlex
from abc import ABC, abstractmethod
from shutil import which
from typing import Any, MutableMapping, MutableSequence

import pkg_resources
from cachetools import Cache, TTLCache

from streamflow.core import utils
from streamflow.core.asyncache import cachedmethod
from streamflow.core.deployment import Connector, Location
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.scheduling import AvailableLocation
from streamflow.core.utils import get_local_to_remote_destination, get_option
from streamflow.deployment.connector.base import BaseConnector
from streamflow.log_handler import logger


def _check_docker_compose_installed():
    if which("docker-compose") is None:
        raise WorkflowExecutionException(
            "Docker Compose must be installed on the system to use the Docker Compose connector."
        )


def _check_docker_installed(connector):
    if which("docker") is None:
        raise WorkflowExecutionException(
            f"Docker must be installed on the system to use the {connector} connector."
        )


def _check_singularity_installed():
    if which("singularity") is None:
        raise WorkflowExecutionException(
            "Singularity must be installed on the system to use the Singularity connector."
        )


async def _exists_docker_image(image_name: str) -> bool:
    proc = await asyncio.create_subprocess_exec(
        *shlex.split(f"docker image inspect {image_name}"),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await proc.wait()
    return proc.returncode == 0


async def _get_docker_compose_version() -> str:
    proc = await asyncio.create_subprocess_exec(
        *shlex.split("docker-compose version --short"),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await proc.communicate()
    return stdout.decode().strip()


async def _get_docker_version() -> str:
    proc = await asyncio.create_subprocess_exec(
        *shlex.split("docker version --format '{{.Client.Version}}'"),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await proc.communicate()
    return stdout.decode().strip()


async def _get_singularity_version() -> str:
    proc = await asyncio.create_subprocess_exec(
        *shlex.split("singularity --version"),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await proc.communicate()
    return stdout.decode().strip()


def _prepare_volumes(binds: MutableSequence[str], mounts: MutableSequence[str]):
    for b in binds or []:
        src, dst = b.split(":", 2)
        if not os.path.exists(src):
            os.makedirs(src)
    for m in mounts or []:
        mount_type = next(part[5:] for part in m.split(",") if part.startswith("type="))
        if mount_type == "bind":
            src = next(part[4:] for part in m.split(",") if part.startswith("src="))
            if not os.path.exists(src):
                os.makedirs(src)


async def _pull_docker_image(image_name: str) -> None:
    proc = await asyncio.create_subprocess_exec(
        *shlex.split(f"docker pull --quiet {image_name}"),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await proc.wait()


class ContainerConnector(BaseConnector, ABC):
    def __init__(
        self,
        deployment_name: str,
        config_dir: str,
        transferBufferSize: int,
        locationsCacheSize: int = None,
        locationsCacheTTL: int = None,
        resourcesCacheSize: int = None,
        resourcesCacheTTL: int = None,
    ):
        super().__init__(deployment_name, config_dir, transferBufferSize)
        cacheSize = locationsCacheSize
        if cacheSize is None:
            cacheSize = resourcesCacheSize
            if cacheSize is not None:
                if logger.isEnabledFor(logging.WARN):
                    logger.warn(
                        "The `resourcesCacheSize` keyword is deprecated and will be removed in StreamFlow 0.3.0. "
                        "Use `locationsCacheSize` instead."
                    )
            else:
                cacheSize = 10
        cacheTTL = locationsCacheTTL
        if cacheTTL is None:
            cacheTTL = resourcesCacheTTL
            if cacheTTL is not None:
                if logger.isEnabledFor(logging.WARN):
                    logger.warn(
                        "The `resourcesCacheTTL` keyword is deprecated and will be removed in StreamFlow 0.3.0. "
                        "Use `locationsCacheTTL` instead."
                    )
            else:
                cacheTTL = 10
        self.locationsCache: Cache = TTLCache(maxsize=cacheSize, ttl=cacheTTL)

    async def _check_effective_location(
        self,
        common_paths: MutableMapping[str, Any],
        effective_locations: MutableSequence[Location],
        location: Location,
        path: str,
        source_location: Location | None = None,
    ) -> tuple[MutableMapping[str, Any], MutableSequence[Location]]:
        # Get all container mounts
        volumes = await self._get_volumes(location)
        for volume in volumes:
            # If path is in a persistent volume
            if path.startswith(volume["Destination"]):
                if path not in common_paths:
                    common_paths[path] = []
                # Check if path is shared with another location that has been already processed
                for i, common_path in enumerate(common_paths[path]):
                    if common_path["source"] == volume["Source"]:
                        # If path has already been processed, but the current location is the source, substitute it
                        if source_location and location.name == source_location.name:
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
                    {"source": volume["Source"], "location": location}
                )
                return common_paths, effective_locations
        # If path is not in a persistent volume, add the current location to the list
        effective_locations.append(location)
        return common_paths, effective_locations

    async def _copy_local_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[Location],
        read_only: bool = False,
    ) -> None:
        dst = await get_local_to_remote_destination(self, locations[0], src, dst)
        effective_locations = await self._get_effective_locations(locations, dst)
        copy_tasks = []
        for location in effective_locations:
            if read_only and await self._is_bind_transfer(location, src, dst):
                copy_tasks.append(
                    asyncio.create_task(
                        self.run(
                            location=location,
                            command=["ln", "-snf", posixpath.abspath(src), dst],
                        )
                    )
                )
                continue
            copy_tasks.append(
                asyncio.create_task(
                    self._copy_local_to_remote_single(
                        src=src, dst=dst, location=location, read_only=read_only
                    )
                )
            )
        await asyncio.gather(*copy_tasks)

    async def _copy_remote_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[Location],
        source_location: Location,
        source_connector: Connector | None = None,
        read_only: bool = False,
    ) -> None:
        source_connector = source_connector or self
        effective_locations = await self._get_effective_locations(
            locations, dst, source_location
        )
        non_bind_locations = []
        if (
            read_only
            and source_connector == self
            and await self._is_bind_transfer(source_location, src, src)
        ):
            for location in effective_locations:
                if await self._is_bind_transfer(location, dst, dst):
                    await self.run(
                        location=location,
                        command=["ln", "-snf", posixpath.abspath(src), dst],
                    )
                    continue
                non_bind_locations.append(location)
            await super()._copy_remote_to_remote(
                src=src,
                dst=dst,
                locations=non_bind_locations,
                source_connector=source_connector,
                source_location=source_location,
                read_only=read_only,
            )
        else:
            await super()._copy_remote_to_remote(
                src=src,
                dst=dst,
                locations=effective_locations,
                source_connector=source_connector,
                source_location=source_location,
                read_only=read_only,
            )

    async def _get_effective_locations(
        self,
        locations: MutableSequence[Location],
        dest_path: str,
        source_location: Location | None = None,
    ) -> MutableSequence[Location]:
        common_paths = {}
        effective_locations = []
        for location in locations:
            common_paths, effective_locations = await self._check_effective_location(
                common_paths, effective_locations, location, dest_path, source_location
            )
        return effective_locations

    @abstractmethod
    async def _get_bind_mounts(
        self, location: Location
    ) -> MutableSequence[MutableMapping[str, str]]:
        ...

    @abstractmethod
    async def _get_location(self, location_name: str) -> AvailableLocation | None:
        ...

    @abstractmethod
    async def _get_volumes(
        self, location: Location
    ) -> MutableSequence[MutableMapping[str, str]]:
        ...

    async def _is_bind_transfer(
        self, location: Location, host_path: str, instance_path: str
    ) -> bool:
        bind_mounts = await self._get_bind_mounts(location)
        host_binds = [b["Source"] for b in bind_mounts]
        instance_binds = [b["Destination"] for b in bind_mounts]
        for host_bind in host_binds:
            if host_path.startswith(host_bind):
                for instance_bind in instance_binds:
                    if instance_path.startswith(instance_bind):
                        return True
        return False


class DockerBaseConnector(ContainerConnector, ABC):
    async def _get_bind_mounts(
        self, location: Location
    ) -> MutableSequence[MutableMapping[str, str]]:
        return [v for v in await self._get_volumes(location) if v["Type"] == "bind"]

    async def _get_location(self, location_name: str) -> AvailableLocation:
        inspect_command = "".join(
            [
                "docker ",
                "inspect ",
                "--format ",
                "'{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ",
                location_name,
            ]
        )
        proc = await asyncio.create_subprocess_exec(
            *shlex.split(inspect_command),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        return AvailableLocation(
            name=location_name,
            deployment=self.deployment_name,
            hostname=stdout.decode().strip(),
        )

    def _get_run_command(
        self, command: str, location: Location, interactive: bool = False
    ):
        return f"docker exec {'-i' if interactive else ''} {location.name} sh -c '{command}'"

    async def _get_volumes(
        self, location: Location
    ) -> MutableSequence[MutableMapping[str, str]]:
        inspect_command = "".join(
            ["docker ", "inspect ", "--format ", "'{{json .Mounts}}' ", location.name]
        )
        proc = await asyncio.create_subprocess_exec(
            *shlex.split(inspect_command),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        return json.loads(stdout.decode().strip()) if stdout else []


class DockerConnector(DockerBaseConnector):
    def __init__(
        self,
        deployment_name: str,
        config_dir: str,
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
        containerIds: MutableSequence | None = None,
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
        disableContentTrust: bool = True,
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
        locationsCacheSize: int | None = None,
        locationsCacheTTL: int | None = None,
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
        replicas: int = 1,
        resourcesCacheSize: int | None = None,
        resourcesCacheTTL: int | None = None,
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
            transferBufferSize=transferBufferSize,
            locationsCacheSize=locationsCacheSize,
            locationsCacheTTL=locationsCacheTTL,
            resourcesCacheSize=resourcesCacheSize,
            resourcesCacheTTL=resourcesCacheTTL,
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
        self.containerIds: MutableSequence[str] = containerIds or []
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
        self.disableContentTrust: bool = disableContentTrust
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
        self.replicas: int = replicas
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

    async def deploy(self, external: bool) -> None:
        if not external:
            _prepare_volumes(self.volume, self.mount)
            _check_docker_installed("Docker")
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Using Docker {await _get_docker_version()}.")
            # Pull image if it doesn't exist
            if not await _exists_docker_image(self.image):
                await _pull_docker_image(self.image)
            # Deploy the Docker container
            for _ in range(0, self.replicas):
                deploy_command = (
                    f"docker run --detach --interactive "
                    f"{get_option('add-host', self.addHost)}"
                    f"{get_option('blkio-weight', self.addHost)}"
                    f"{get_option('blkio-weight-device', self.blkioWeightDevice)}"
                    f"{get_option('cap-add', self.capAdd)}"
                    f"{get_option('cap-drop', self.capDrop)}"
                    f"{get_option('cgroup-parent', self.cgroupParent)}"
                    f"{get_option('cgroupns', self.cgroupns)}"
                    f"{get_option('cidfile', self.cidfile)}"
                    f"{get_option('cpu-period', self.cpuPeriod)}"
                    f"{get_option('cpu-quota', self.cpuQuota)}"
                    f"{get_option('cpu-rt-period', self.cpuRTPeriod)}"
                    f"{get_option('cpu-rt-runtime', self.cpuRTRuntime)}"
                    f"{get_option('cpu-shares', self.cpuShares)}"
                    f"{get_option('cpus', self.cpus)}"
                    f"{get_option('cpuset-cpus', self.cpusetCpus)}"
                    f"{get_option('cpuset-mems', self.cpusetMems)}"
                    f"{get_option('detach-keys', self.detachKeys)}"
                    f"{get_option('device', self.device)}"
                    f"{get_option('device-cgroup-rule', self.deviceCgroupRule)}"
                    f"{get_option('device-read-bps', self.deviceReadBps)}"
                    f"{get_option('device-read-iops', self.deviceReadIops)}"
                    f"{get_option('device-write-bps', self.deviceWriteBps)}"
                    f"{get_option('device-write-iops', self.deviceWriteIops)}"
                    f"--disable-content-trust={'true' if self.disableContentTrust else 'false'} "
                    f"{get_option('dns', self.dns)}"
                    f"{get_option('dns-option', self.dnsOptions)}"
                    f"{get_option('dns-search', self.dnsSearch)}"
                    f"{get_option('domainname', self.domainname)}"
                    f"{get_option('entrypoint', self.entrypoint)}"
                    f"{get_option('env', self.env)}"
                    f"{get_option('env-file', self.envFile)}"
                    f"{get_option('expose', self.expose)}"
                    f"{get_option('gpus', self.gpus)}"
                    f"{get_option('group-add', self.groupAdd)}"
                    f"{get_option('health-cmd', self.healthCmd)}"
                    f"{get_option('health-interval', self.healthInterval)}"
                    f"{get_option('health-retries', self.healthRetries)}"
                    f"{get_option('health-start-period', self.healthStartPeriod)}"
                    f"{get_option('health-timeout', self.healthTimeout)}"
                    f"{get_option('hostname', self.hostname)}"
                    f"{get_option('init', self.init)}"
                    f"{get_option('ip', self.ip)}"
                    f"{get_option('ip6', self.ip6)}"
                    f"{get_option('ipc', self.ipc)}"
                    f"{get_option('isolation', self.isolation)}"
                    f"{get_option('kernel-memory', self.kernelMemory)}"
                    f"{get_option('label', self.label)}"
                    f"{get_option('label-file', self.labelFile)}"
                    f"{get_option('link', self.link)}"
                    f"{get_option('link-local-ip', self.linkLocalIP)}"
                    f"{get_option('log-driver', self.logDriver)}"
                    f"{get_option('log-opt', self.logOpts)}"
                    f"{get_option('mac-address', self.macAddress)}"
                    f"{get_option('memory', self.memory)}"
                    f"{get_option('memory-reservation', self.memoryReservation)}"
                    f"{get_option('memory-swap', self.memorySwap)}"
                    f"{get_option('memory-swappiness', self.memorySwappiness)}"
                    f"{get_option('mount', self.mount)}"
                    f"{get_option('network', self.network)}"
                    f"{get_option('network-alias', self.networkAlias)}"
                    f"{get_option('no-healthcheck', self.noHealthcheck)}"
                    f"{get_option('oom-kill-disable', self.oomKillDisable)}"
                    f"{get_option('oom-score-adj', self.oomScoreAdj)}"
                    f"{get_option('pid', self.pid)}"
                    f"{get_option('pids-limit', self.pidsLimit)}"
                    f"{get_option('privileged', self.privileged)}"
                    f"{get_option('publish', self.publish)}"
                    f"{get_option('publish-all', self.publishAll)}"
                    f"{get_option('read-only', self.readOnly)}"
                    f"{get_option('restart', self.restart)}"
                    f"{get_option('rm', self.rm)}"
                    f"{get_option('runtime', self.runtime)}"
                    f"{get_option('security-opt', self.securityOpts)}"
                    f"{get_option('shm-size', self.shmSize)}"
                    f"--sig-proxy={'true' if self.sigProxy else 'false'} "
                    f"{get_option('stop-signal', self.stopSignal)}"
                    f"{get_option('stop-timeout', self.stopTimeout)}"
                    f"{get_option('storage-opt', self.storageOpts)}"
                    f"{get_option('sysctl', self.sysctl)}"
                    f"{get_option('tmpfs', self.tmpfs)}"
                    f"{get_option('ulimit', self.ulimit)}"
                    f"{get_option('user', self.user)}"
                    f"{get_option('userns', self.userns)}"
                    f"{get_option('uts', self.uts)}"
                    f"{get_option('volume', self.volume)}"
                    f"{get_option('volume-driver', self.volumeDriver)}"
                    f"{get_option('volumes-from', self.volumesFrom)}"
                    f"{get_option('workdir', self.workdir)}"
                    f"{self.image} "
                    f"{' '.join(self.command) if self.command else ''}"
                )
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"EXECUTING command {deploy_command}")
                proc = await asyncio.create_subprocess_exec(
                    *shlex.split(deploy_command),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await proc.communicate()
                if proc.returncode == 0:
                    self.containerIds.append(stdout.decode().strip())
                else:
                    raise WorkflowExecutionException(stderr.decode().strip())

    @cachedmethod(lambda self: self.locationsCache)
    async def get_available_locations(
        self,
        service: str | None = None,
        input_directory: str | None = None,
        output_directory: str | None = None,
        tmp_directory: str | None = None,
    ) -> MutableMapping[str, AvailableLocation]:
        return {
            container_id: await self._get_location(container_id)
            for container_id in self.containerIds
        }

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "docker.json")
        )

    async def undeploy(self, external: bool) -> None:
        if not external and self.containerIds:
            for container_id in self.containerIds:
                undeploy_command = "".join(["docker ", "stop ", container_id])
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"EXECUTING command {undeploy_command}")
                proc = await asyncio.create_subprocess_exec(
                    *shlex.split(undeploy_command),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                await proc.wait()
            self.containerIds = []


class DockerComposeConnector(DockerBaseConnector):
    def __init__(
        self,
        deployment_name: str,
        config_dir: str,
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
        locationsCacheSize: int = None,
        locationsCacheTTL: int = None,
        resourcesCacheSize: int = None,
        resourcesCacheTTL: int = None,
    ) -> None:
        super().__init__(
            deployment_name=deployment_name,
            config_dir=config_dir,
            transferBufferSize=transferBufferSize,
            locationsCacheSize=locationsCacheSize,
            locationsCacheTTL=locationsCacheTTL,
            resourcesCacheSize=resourcesCacheSize,
            resourcesCacheTTL=resourcesCacheTTL,
        )
        self.files = [os.path.join(self.config_dir, file) for file in files]
        self.projectName = projectName
        self.verbose = verbose
        self.logLevel = logLevel
        self.noAnsi = noAnsi
        self.host = host
        self.noDeps = noDeps
        self.forceRecreate = forceRecreate
        self.alwaysRecreateDeps = alwaysRecreateDeps
        self.noRecreate = noRecreate
        self.noBuild = noBuild
        self.noStart = noStart
        self.build = build
        self.renewAnonVolumes = renewAnonVolumes
        self.removeOrphans = removeOrphans
        self.removeVolumes = removeVolumes
        self.skipHostnameCheck = skipHostnameCheck
        self.projectDirectory = projectDirectory
        self.compatibility = compatibility
        self.timeout = timeout
        self.tls = tls
        self.tlscacert = tlscacert
        self.tlscert = tlscert
        self.tlskey = tlskey
        self.tlsverify = tlsverify

    def _get_base_command(self) -> str:
        return (
            f"docker-compose "
            f"{get_option('file', self.files)}"
            f"{get_option('project-name', self.projectName)}"
            f"{get_option('verbose', self.verbose)}"
            f"{get_option('log-level', self.logLevel)}"
            f"{get_option('no-ansi', self.noAnsi)}"
            f"{get_option('host', self.host)}"
            f"{get_option('tls', self.tls)}"
            f"{get_option('tlscacert', self.tlscacert)}"
            f"{get_option('tlscert', self.tlscert)}"
            f"{get_option('tlskey', self.tlskey)}"
            f"{get_option('tlsverify', self.tlsverify)}"
            f"{get_option('skip-hostname-check', self.skipHostnameCheck)}"
            f"{get_option('project-directory', self.projectDirectory)}"
            f"{get_option('compatibility', self.compatibility)}"
        )

    async def deploy(self, external: bool) -> None:
        if not external:
            _check_docker_installed("Docker Compose")
            version = await _get_docker_compose_version()
            if version.startswith("v"):
                version = version[1:]
            major = int(version.split(".")[0])
            if not major >= 2:
                raise WorkflowExecutionException(
                    f"Docker Compose {version} is not compatible with DockerComposeConnector. "
                    f"Docker Compose version 2.x or later is required."
                )
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"Using Docker {await _get_docker_version()} "
                    f"and Docker Compose {version}."
                )
            deploy_command = (
                f"{self._get_base_command()} up --detach "
                f"{get_option('no-deps ', self.noDeps)}"
                f"{get_option('force-recreate', self.forceRecreate)}"
                f"{get_option('always-recreate-deps', self.alwaysRecreateDeps)}"
                f"{get_option('no-recreate', self.noRecreate)}"
                f"{get_option('no-build', self.noBuild)}"
                f"{get_option('no-start', self.noStart)}"
            )
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"EXECUTING command {deploy_command}")
            proc = await asyncio.create_subprocess_exec(
                *shlex.split(deploy_command),
                stderr=asyncio.subprocess.STDOUT,
                stdout=asyncio.subprocess.PIPE,
            )
            stdout, _ = await proc.communicate()
            if proc.returncode != 0:
                raise WorkflowExecutionException(
                    f"FAILED Deployment of {self.deployment_name} environment:\n\t{stdout.decode().strip()}"
                )

    @cachedmethod(lambda self: self.locationsCache)
    async def get_available_locations(
        self,
        service: str | None = None,
        input_directory: str | None = None,
        output_directory: str | None = None,
        tmp_directory: str | None = None,
    ) -> MutableMapping[str, AvailableLocation]:
        ps_command = self._get_base_command() + "".join(
            ["ps ", "--format ", "json ", service or ""]
        )
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"EXECUTING command {ps_command}")
        proc = await asyncio.create_subprocess_exec(
            *shlex.split(ps_command),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        stdout, _ = await proc.communicate()
        locations = json.loads(stdout.decode().strip())
        return {
            loc["Name"]: v
            for loc, v in zip(
                locations,
                await asyncio.gather(
                    *(
                        asyncio.create_task(self._get_location(location["Name"]))
                        for location in locations
                    )
                ),
            )
        }

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "docker-compose.json")
        )

    async def undeploy(self, external: bool) -> None:
        if not external:
            undeploy_command = (
                self._get_base_command()
                + f"down {get_option('volumes', self.removeVolumes)}"
            )
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"EXECUTING command {undeploy_command}")
            proc = await asyncio.create_subprocess_exec(
                *shlex.split(undeploy_command),
                stderr=asyncio.subprocess.STDOUT,
                stdout=asyncio.subprocess.PIPE,
            )
            stdout, _ = await proc.communicate()
            if proc.returncode != 0:
                raise WorkflowExecutionException(
                    f"FAILED Undeployment of {self.deployment_name} environment:\n\t{stdout.decode().strip()}"
                )


class SingularityConnector(ContainerConnector):
    def __init__(
        self,
        deployment_name: str,
        config_dir: str,
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
        instanceNames: MutableSequence[str] | None = None,
        ipc: bool = False,
        keepPrivs: bool = False,
        locationsCacheSize: int = None,
        locationsCacheTTL: int = None,
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
        replicas: int = 1,
        resourcesCacheSize: int = None,
        resourcesCacheTTL: int = None,
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
            transferBufferSize=transferBufferSize,
            locationsCacheSize=locationsCacheSize,
            locationsCacheTTL=locationsCacheTTL,
            resourcesCacheSize=resourcesCacheSize,
            resourcesCacheTTL=resourcesCacheTTL,
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
        self.instanceNames: MutableSequence[str] | None = instanceNames or []
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
        self.replicas: int = replicas
        self.rocm: bool = rocm
        self.scratch: MutableSequence[str] | None = scratch
        self.security: MutableSequence[str] | None = security
        self.userns: bool = userns
        self.uts: bool = uts
        self.workdir: str | None = workdir
        self.writable: bool = writable
        self.writableTmpfs: bool = writableTmpfs

    async def _get_bind_mounts(
        self, location: Location
    ) -> MutableSequence[MutableMapping[str, str]]:
        return await self._get_volumes(location)

    async def _get_location(self, location_name: str) -> AvailableLocation | None:
        inspect_command = "singularity instance list --json"
        proc = await asyncio.create_subprocess_exec(
            *shlex.split(inspect_command),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        if stdout:
            json_out = json.loads(stdout)
            for instance in json_out["instances"]:
                if instance["instance"] == location_name:
                    return AvailableLocation(
                        name=location_name,
                        deployment=self.deployment_name,
                        hostname=instance["ip"],
                    )
        return None

    async def _get_volumes(
        self, location: Location
    ) -> MutableSequence[MutableMapping[str, str]]:
        bind_mounts, _ = await self.run(
            location=location,
            command=["cat", "/proc/1/mountinfo", "|", "awk", "'{print $9,$4,$5}'"],
            capture_output=True,
        )
        # Exclude `overlay` and `tmpfs` mounts
        return [
            {
                "Source": line.split()[2]
                if line.split()[1] == "/"
                else line.split()[1],
                "Destination": line.split()[2],
            }
            for line in bind_mounts.splitlines()
            if line.split()[0] not in ["tmpfs", "overlay"]
        ]

    def _get_run_command(
        self, command: str, location: Location, interactive: bool = False
    ):
        return f"singularity exec instance://{location.name} sh -c '{command}'"

    async def deploy(self, external: bool) -> None:
        if not external:
            _check_singularity_installed()
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Using {await _get_singularity_version()}.")
            _prepare_volumes(self.bind, self.mount)
            for _ in range(0, self.replicas):
                instance_name = utils.random_name()
                deploy_command = (
                    f"singularity instance start "
                    f"{get_option('add-caps', self.addCaps)}"
                    f"{get_option('allow-setuid', self.allowSetuid)}"
                    f"{get_option('apply-cgroups', self.applyCgroups)}"
                    f"{get_option('bind', self.bind)}"
                    f"{get_option('blkio-weight', self.blkioWeight)}"
                    f"{get_option('blkio-weight-device', self.blkioWeightDevice)}"
                    f"{get_option('boot', self.boot)}"
                    f"{get_option('cleanenv', self.cleanenv)}"
                    f"{get_option('compat', self.compat)}"
                    f"{get_option('contain', self.contain)}"
                    f"{get_option('containall', self.containall)}"
                    f"{get_option('cpu-shares', self.cpuShares)}"
                    f"{get_option('cpus', self.cpus)}"
                    f"{get_option('cpuset-cpus', self.cpusetCpus)}"
                    f"{get_option('cpuset-mems', self.cpusetMems)}"
                    f"{get_option('disable-cache', self.disableCache)}"
                    f"{get_option('docker-host', self.dockerHost)}"
                    f"{get_option('dns', self.dns)}"
                    f"{get_option('drop-caps', self.dropCaps)}"
                    f"{get_option('env-file', self.envFile)}"
                    f"{get_option('fakeroot', self.fakeroot)}"
                    f"{get_option('fusemount', self.fusemount)}"
                    f"{get_option('home', self.home)}"
                    f"{get_option('hostname', self.hostname)}"
                    f"{get_option('ipc', self.ipc)}"
                    f"{get_option('keep-privs', self.keepPrivs)}"
                    f"{get_option('memory', self.memory)}"
                    f"{get_option('memory-reservation', self.memoryReservation)}"
                    f"{get_option('memory-swap', self.memorySwap)}"
                    f"{get_option('mount', self.mount)}"
                    f"{get_option('net', self.net)}"
                    f"{get_option('network', self.network)}"
                    f"{get_option('network-args', self.networkArgs)}"
                    f"{get_option('no-eval', self.noEval)}"
                    f"{get_option('no-home', self.noHome)}"
                    f"{get_option('no-https', self.noHttps)}"
                    f"{get_option('no-init', self.noInit)}"
                    f"{get_option('no-mount', self.noMount)}"
                    f"{get_option('no-privs', self.noPrivs)}"
                    f"{get_option('no-umask', self.noUmask)}"
                    f"{get_option('nv', self.nv)}"
                    f"{get_option('nvccli', self.nvccli)}"
                    f"{get_option('oom-kill-disable', self.oomKillDisable)}"
                    f"{get_option('overlay', self.overlay)}"
                    f"{get_option('pem-path', self.pemPath)}"
                    f"{get_option('pid-file', self.pidFile)}"
                    f"{get_option('pids-limit', self.pidsLimit)}"
                    f"{get_option('rocm', self.rocm)}"
                    f"{get_option('scratch', self.scratch)}"
                    f"{get_option('security', self.security)}"
                    f"{get_option('userns', self.userns)}"
                    f"{get_option('uts', self.uts)}"
                    f"{get_option('workdir', self.workdir)}"
                    f"{get_option('writable', self.writable)}"
                    f"{get_option('writable-tmpfs', self.writableTmpfs)}"
                    f"{self.image} "
                    f"{instance_name} "
                    f"{' '.join(self.command) if self.command else ''}"
                )
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"EXECUTING command {deploy_command}")
                proc = await asyncio.create_subprocess_exec(
                    *shlex.split(deploy_command),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                )
                stdout, _ = await proc.communicate()
                if proc.returncode == 0:
                    self.instanceNames.append(instance_name)
                else:
                    raise WorkflowExecutionException(stdout.decode().strip())

    @cachedmethod(lambda self: self.locationsCache)
    async def get_available_locations(
        self,
        service: str | None = None,
        input_directory: str | None = None,
        output_directory: str | None = None,
        tmp_directory: str | None = None,
    ) -> MutableMapping[str, AvailableLocation]:
        location_tasks = {}
        for instance_name in self.instanceNames:
            location_tasks[instance_name] = asyncio.create_task(
                self._get_location(instance_name)
            )
        return {
            k: v
            for k, v in zip(
                location_tasks.keys(), await asyncio.gather(*location_tasks.values())
            )
            if v is not None
        }

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "singularity.json")
        )

    async def undeploy(self, external: bool) -> None:
        if not external and self.instanceNames:
            for instance_name in self.instanceNames:
                undeploy_command = f"singularity instance stop {instance_name}"
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"EXECUTING command {undeploy_command}")
                proc = await asyncio.create_subprocess_exec(
                    *shlex.split(undeploy_command),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                )
                stdout, _ = await proc.communicate()
            self.instanceNames = []
