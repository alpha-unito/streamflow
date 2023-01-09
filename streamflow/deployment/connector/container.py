import asyncio
import json
import logging
import os
import posixpath
import shlex
from abc import ABC, abstractmethod
from typing import Any, MutableMapping, MutableSequence, Optional, Tuple

import pkg_resources
from cachetools import Cache, TTLCache

from streamflow.core import utils
from streamflow.core.asyncache import cachedmethod
from streamflow.core.deployment import Connector, Location
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.scheduling import AvailableLocation
from streamflow.deployment.connector.base import BaseConnector
from streamflow.log_handler import logger


async def _exists_docker_image(image_name: str) -> bool:
    exists_command = "".join(["docker ", "image ", "inspect ", image_name])
    proc = await asyncio.create_subprocess_exec(
        *shlex.split(exists_command),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    await proc.wait()
    return proc.returncode == 0


async def _pull_docker_image(image_name: str) -> None:
    exists_command = "".join(["docker ", "pull ", "--quiet ", image_name])
    proc = await asyncio.create_subprocess_exec(
        *shlex.split(exists_command),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL
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
        source_location: Optional[Location] = None,
    ) -> Tuple[MutableMapping[str, Any], MutableSequence[Location]]:
        # Get all container mounts
        volumes = await self._get_volumes(location.name)
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
        effective_locations = await self._get_effective_locations(locations, dst)
        copy_tasks = []
        for location in effective_locations:
            if read_only and await self._is_bind_transfer(location.name, src, dst):
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
        source_connector: Optional[Connector] = None,
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
            and await self._is_bind_transfer(source_location.name, src, src)
        ):
            for location in effective_locations:
                if await self._is_bind_transfer(location.name, dst, dst):
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
        source_location: Optional[Location] = None,
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
        self, location: str
    ) -> MutableSequence[MutableMapping[str, str]]:
        ...

    @abstractmethod
    async def _get_location(self, location_name: str) -> Optional[AvailableLocation]:
        ...

    @abstractmethod
    async def _get_volumes(
        self, location: str
    ) -> MutableSequence[MutableMapping[str, str]]:
        ...

    async def _is_bind_transfer(
        self, location: str, host_path: str, instance_path: str
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
        self, location: str
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
            stderr=asyncio.subprocess.PIPE
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
        return "".join(
            [
                "docker ",
                "exec {}".format("-i " if interactive else ""),
                "{} ".format(location.name),
                "sh -c '{}'".format(command),
            ]
        )

    async def _get_volumes(
        self, location: str
    ) -> MutableSequence[MutableMapping[str, str]]:
        inspect_command = "".join(
            ["docker ", "inspect ", "--format ", "'{{json .Mounts}}' ", location]
        )
        proc = await asyncio.create_subprocess_exec(
            *shlex.split(inspect_command),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        return json.loads(stdout.decode().strip()) if stdout else []


class DockerConnector(DockerBaseConnector):
    def __init__(
        self,
        deployment_name: str,
        config_dir: str,
        image: str,
        addHost: Optional[MutableSequence[str]] = None,
        blkioWeight: Optional[int] = None,
        blkioWeightDevice: Optional[MutableSequence[int]] = None,
        capAdd: Optional[MutableSequence[str]] = None,
        capDrop: Optional[MutableSequence[str]] = None,
        cgroupParent: Optional[str] = None,
        cidfile: Optional[str] = None,
        containerIds: Optional[MutableSequence] = None,
        cpuPeriod: Optional[int] = None,
        cpuQuota: Optional[int] = None,
        cpuRTPeriod: Optional[int] = None,
        cpuRTRuntime: Optional[int] = None,
        cpuShares: Optional[int] = None,
        cpus: Optional[float] = None,
        cpusetCpus: Optional[str] = None,
        cpusetMems: Optional[str] = None,
        detachKeys: Optional[str] = None,
        device: Optional[MutableSequence[str]] = None,
        deviceCgroupRule: Optional[MutableSequence[str]] = None,
        deviceReadBps: Optional[MutableSequence[str]] = None,
        deviceReadIops: Optional[MutableSequence[str]] = None,
        deviceWriteBps: Optional[MutableSequence[str]] = None,
        deviceWriteIops: Optional[MutableSequence[str]] = None,
        disableContentTrust: bool = True,
        dns: Optional[MutableSequence[str]] = None,
        dnsOptions: Optional[MutableSequence[str]] = None,
        dnsSearch: Optional[MutableSequence[str]] = None,
        domainname: Optional[str] = None,
        entrypoint: Optional[str] = None,
        env: Optional[MutableSequence[str]] = None,
        envFile: Optional[MutableSequence[str]] = None,
        expose: Optional[MutableSequence[str]] = None,
        gpus: Optional[MutableSequence[str]] = None,
        groupAdd: Optional[MutableSequence[str]] = None,
        healthCmd: Optional[str] = None,
        healthInterval: Optional[str] = None,
        healthRetries: Optional[int] = None,
        healthStartPeriod: Optional[str] = None,
        healthTimeout: Optional[str] = None,
        hostname: Optional[str] = None,
        init: bool = True,
        ip: Optional[str] = None,
        ip6: Optional[str] = None,
        ipc: Optional[str] = None,
        isolation: Optional[str] = None,
        kernelMemory: Optional[int] = None,
        label: Optional[MutableSequence[str]] = None,
        labelFile: Optional[MutableSequence[str]] = None,
        link: Optional[MutableSequence[str]] = None,
        linkLocalIP: Optional[MutableSequence[str]] = None,
        locationsCacheSize: int = None,
        locationsCacheTTL: int = None,
        logDriver: Optional[str] = None,
        logOpts: Optional[MutableSequence[str]] = None,
        macAddress: Optional[str] = None,
        memory: Optional[int] = None,
        memoryReservation: Optional[int] = None,
        memorySwap: Optional[int] = None,
        memorySwappiness: Optional[int] = None,
        mount: Optional[MutableSequence[str]] = None,
        network: Optional[MutableSequence[str]] = None,
        networkAlias: Optional[MutableSequence[str]] = None,
        noHealthcheck: bool = False,
        oomKillDisable: bool = False,
        oomScoreAdj: Optional[int] = None,
        pid: Optional[str] = None,
        pidsLimit: Optional[int] = None,
        privileged: bool = False,
        publish: Optional[MutableSequence[str]] = None,
        publishAll: bool = False,
        readOnly: bool = False,
        replicas: int = 1,
        resourcesCacheSize: int = None,
        resourcesCacheTTL: int = None,
        restart: Optional[str] = None,
        rm: bool = True,
        runtime: Optional[str] = None,
        securityOpts: Optional[MutableSequence[str]] = None,
        shmSize: Optional[int] = None,
        sigProxy: bool = True,
        stopSignal: Optional[str] = None,
        stopTimeout: Optional[int] = None,
        storageOpts: Optional[MutableSequence[str]] = None,
        sysctl: Optional[MutableSequence[str]] = None,
        tmpfs: Optional[MutableSequence[str]] = None,
        transferBufferSize: int = 2**16,
        ulimit: Optional[MutableSequence[str]] = None,
        user: Optional[str] = None,
        userns: Optional[str] = None,
        uts: Optional[str] = None,
        volume: Optional[MutableSequence[str]] = None,
        volumeDriver: Optional[str] = None,
        volumesFrom: Optional[MutableSequence[str]] = None,
        workdir: Optional[str] = None,
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
        self.addHost: Optional[MutableSequence[str]] = addHost
        self.blkioWeight: Optional[int] = blkioWeight
        self.blkioWeightDevice: Optional[MutableSequence[int]] = blkioWeightDevice
        self.capAdd: Optional[MutableSequence[str]] = capAdd
        self.capDrop: Optional[MutableSequence[str]] = capDrop
        self.cgroupParent: Optional[str] = cgroupParent
        self.cidfile: Optional[str] = cidfile
        self.containerIds: MutableSequence[str] = containerIds or []
        self.cpuPeriod: Optional[int] = cpuPeriod
        self.cpuQuota: Optional[int] = cpuQuota
        self.cpuRTPeriod: Optional[int] = cpuRTPeriod
        self.cpuRTRuntime: Optional[int] = cpuRTRuntime
        self.cpuShares: Optional[int] = cpuShares
        self.cpus: Optional[float] = cpus
        self.cpusetCpus: Optional[str] = cpusetCpus
        self.cpusetMems: Optional[str] = cpusetMems
        self.detachKeys: Optional[str] = detachKeys
        self.device: Optional[MutableSequence[str]] = device
        self.deviceCgroupRule: Optional[MutableSequence[str]] = deviceCgroupRule
        self.deviceReadBps: Optional[MutableSequence[str]] = deviceReadBps
        self.deviceReadIops: Optional[MutableSequence[str]] = deviceReadIops
        self.deviceWriteBps: Optional[MutableSequence[str]] = deviceWriteBps
        self.deviceWriteIops: Optional[MutableSequence[str]] = deviceWriteIops
        self.disableContentTrust: bool = disableContentTrust
        self.dns: Optional[MutableSequence[str]] = dns
        self.dnsOptions: Optional[MutableSequence[str]] = dnsOptions
        self.dnsSearch: Optional[MutableSequence[str]] = dnsSearch
        self.domainname: Optional[str] = domainname
        self.entrypoint: Optional[str] = entrypoint
        self.env: Optional[MutableSequence[str]] = env
        self.envFile: Optional[MutableSequence[str]] = envFile
        self.expose: Optional[MutableSequence[str]] = expose
        self.gpus: Optional[MutableSequence[str]] = gpus
        self.groupAdd: Optional[MutableSequence[str]] = groupAdd
        self.healthCmd: Optional[str] = healthCmd
        self.healthInterval: Optional[str] = healthInterval
        self.healthRetries: Optional[int] = healthRetries
        self.healthStartPeriod: Optional[str] = healthStartPeriod
        self.healthTimeout: Optional[str] = healthTimeout
        self.hostname: Optional[str] = hostname
        self.init: bool = init
        self.ip: Optional[str] = ip
        self.ip6: Optional[str] = ip6
        self.ipc: Optional[str] = ipc
        self.isolation: Optional[str] = isolation
        self.kernelMemory: Optional[int] = kernelMemory
        self.label: Optional[MutableSequence[str]] = label
        self.labelFile: Optional[MutableSequence[str]] = labelFile
        self.link: Optional[MutableSequence[str]] = link
        self.linkLocalIP: Optional[MutableSequence[str]] = linkLocalIP
        self.logDriver: Optional[str] = logDriver
        self.logOpts: Optional[MutableSequence[str]] = logOpts
        self.macAddress: Optional[str] = macAddress
        self.memory: Optional[int] = memory
        self.memoryReservation: Optional[int] = memoryReservation
        self.memorySwap: Optional[int] = memorySwap
        self.memorySwappiness: Optional[int] = memorySwappiness
        self.mount: Optional[MutableSequence[str]] = mount
        self.network: Optional[MutableSequence[str]] = network
        self.networkAlias: Optional[MutableSequence[str]] = networkAlias
        self.noHealthcheck: bool = noHealthcheck
        self.oomKillDisable: bool = oomKillDisable
        self.oomScoreAdj: Optional[int] = oomScoreAdj
        self.pid: Optional[str] = pid
        self.pidsLimit: Optional[int] = pidsLimit
        self.privileged: bool = privileged
        self.publish: Optional[MutableSequence[str]] = publish
        self.publishAll: bool = publishAll
        self.readOnly: bool = readOnly
        self.replicas: int = replicas
        self.restart: Optional[str] = restart
        self.rm: bool = rm
        self.runtime: Optional[str] = runtime
        self.securityOpts: Optional[MutableSequence[str]] = securityOpts
        self.shmSize: Optional[int] = shmSize
        self.sigProxy: bool = sigProxy
        self.stopSignal: Optional[str] = stopSignal
        self.stopTimeout: Optional[int] = stopTimeout
        self.storageOpts: Optional[MutableSequence[str]] = storageOpts
        self.sysctl: Optional[MutableSequence[str]] = sysctl
        self.tmpfs: Optional[MutableSequence[str]] = tmpfs
        self.ulimit: Optional[MutableSequence[str]] = ulimit
        self.user: Optional[str] = user
        self.userns: Optional[str] = userns
        self.uts: Optional[str] = uts
        self.volume: Optional[MutableSequence[str]] = volume
        self.volumeDriver: Optional[str] = volumeDriver
        self.volumesFrom: Optional[MutableSequence[str]] = volumesFrom
        self.workdir: Optional[str] = workdir

    async def deploy(self, external: bool) -> None:
        if not external:
            # Pull image if it doesn't exist
            if not await _exists_docker_image(self.image):
                await _pull_docker_image(self.image)
            # Deploy the Docker container
            for _ in range(0, self.replicas):
                deploy_command = "".join(
                    [
                        "docker ",
                        "run ",
                        "--detach ",
                        "--interactive ",
                        self.get_option("add-host", self.addHost),
                        self.get_option("blkio-weight", self.addHost),
                        self.get_option("blkio-weight-device", self.blkioWeightDevice),
                        self.get_option("cap-add", self.capAdd),
                        self.get_option("cap-drop", self.capDrop),
                        self.get_option("cgroup-parent", self.cgroupParent),
                        self.get_option("cidfile", self.cidfile),
                        self.get_option("cpu-period", self.cpuPeriod),
                        self.get_option("cpu-quota", self.cpuQuota),
                        self.get_option("cpu-rt-period", self.cpuRTPeriod),
                        self.get_option("cpu-rt-runtime", self.cpuRTRuntime),
                        self.get_option("cpu-shares", self.cpuShares),
                        self.get_option("cpus", self.cpus),
                        self.get_option("cpuset-cpus", self.cpusetCpus),
                        self.get_option("cpuset-mems", self.cpusetMems),
                        self.get_option("detach-keys", self.detachKeys),
                        self.get_option("device", self.device),
                        self.get_option("device-cgroup-rule", self.deviceCgroupRule),
                        self.get_option("device-read-bps", self.deviceReadBps),
                        self.get_option("device-read-iops", self.deviceReadIops),
                        self.get_option("device-write-bps", self.deviceWriteBps),
                        self.get_option("device-write-iops", self.deviceWriteIops),
                        "--disable-content-trust={disableContentTrust} ".format(
                            disableContentTrust="true"
                            if self.disableContentTrust
                            else "false"
                        ),
                        self.get_option("dns", self.dns),
                        self.get_option("dns-option", self.dnsOptions),
                        self.get_option("dns-search", self.dnsSearch),
                        self.get_option("domainname", self.domainname),
                        self.get_option("entrypoint", self.entrypoint),
                        self.get_option("env", self.env),
                        self.get_option("env-file", self.envFile),
                        self.get_option("expose", self.expose),
                        self.get_option("gpus", self.gpus),
                        self.get_option("group-add", self.groupAdd),
                        self.get_option("health-cmd", self.healthCmd),
                        self.get_option("health-interval", self.healthInterval),
                        self.get_option("health-retries", self.healthRetries),
                        self.get_option("health-start-period", self.healthStartPeriod),
                        self.get_option("health-timeout", self.healthTimeout),
                        self.get_option("hostname", self.hostname),
                        self.get_option("init", self.init),
                        self.get_option("ip", self.ip),
                        self.get_option("ip6", self.ip6),
                        self.get_option("ipc", self.ipc),
                        self.get_option("isolation", self.isolation),
                        self.get_option("kernel-memory", self.kernelMemory),
                        self.get_option("label", self.label),
                        self.get_option("label-file", self.labelFile),
                        self.get_option("link", self.link),
                        self.get_option("link-local-ip", self.linkLocalIP),
                        self.get_option("log-driver", self.logDriver),
                        self.get_option("log-opt", self.logOpts),
                        self.get_option("mac-address", self.macAddress),
                        self.get_option("memory", self.memory),
                        self.get_option("memory-reservation", self.memoryReservation),
                        self.get_option("memory-swap", self.memorySwap),
                        self.get_option("memory-swappiness", self.memorySwappiness),
                        self.get_option("mount", self.mount),
                        self.get_option("network", self.network),
                        self.get_option("network-alias", self.networkAlias),
                        self.get_option("no-healthcheck", self.noHealthcheck),
                        self.get_option("oom-kill-disable", self.oomKillDisable),
                        self.get_option("oom-score-adj", self.oomScoreAdj),
                        self.get_option("pid", self.pid),
                        self.get_option("pids-limit", self.pidsLimit),
                        self.get_option("privileged", self.privileged),
                        self.get_option("publish", self.publish),
                        self.get_option("publish-all", self.publishAll),
                        self.get_option("read-only", self.readOnly),
                        self.get_option("restart", self.restart),
                        self.get_option("rm", self.rm),
                        self.get_option("runtime", self.runtime),
                        self.get_option("security-opt", self.securityOpts),
                        self.get_option("shm-size", self.shmSize),
                        "--sig-proxy={sigProxy} ".format(
                            sigProxy="true" if self.sigProxy else "false"
                        ),
                        self.get_option("stop-signal", self.stopSignal),
                        self.get_option("stop-timeout", self.stopTimeout),
                        self.get_option("storage-opt", self.storageOpts),
                        self.get_option("sysctl", self.sysctl),
                        self.get_option("tmpfs", self.tmpfs),
                        self.get_option("ulimit", self.ulimit),
                        self.get_option("user", self.user),
                        self.get_option("userns", self.userns),
                        self.get_option("uts", self.uts),
                        self.get_option("volume", self.volume),
                        self.get_option("volume-driver", self.volumeDriver),
                        self.get_option("volumes-from", self.volumesFrom),
                        self.get_option("workdir", self.workdir),
                        self.image,
                    ]
                )
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("EXECUTING command {}".format(deploy_command))
                proc = await asyncio.create_subprocess_exec(
                    *shlex.split(deploy_command),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await proc.communicate()
                if proc.returncode == 0:
                    self.containerIds.append(stdout.decode().strip())
                else:
                    raise WorkflowExecutionException(stderr.decode().strip())

    @cachedmethod(lambda self: self.locationsCache)
    async def get_available_locations(
        self,
        service: Optional[str] = None,
        input_directory: Optional[str] = None,
        output_directory: Optional[str] = None,
        tmp_directory: Optional[str] = None,
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
                    logger.debug(
                        "EXECUTING command {command}".format(command=undeploy_command)
                    )
                proc = await asyncio.create_subprocess_exec(
                    *shlex.split(undeploy_command),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                await proc.wait()
            self.containerIds = []


class DockerComposeConnector(DockerBaseConnector):
    def __init__(
        self,
        deployment_name: str,
        config_dir: str,
        files: MutableSequence[str],
        projectName: Optional[str] = None,
        verbose: Optional[bool] = False,
        logLevel: Optional[str] = None,
        noAnsi: Optional[bool] = False,
        host: Optional[str] = None,
        tls: Optional[bool] = False,
        tlscacert: Optional[str] = None,
        tlscert: Optional[str] = None,
        tlskey: Optional[str] = None,
        tlsverify: Optional[bool] = False,
        skipHostnameCheck: Optional[bool] = False,
        projectDirectory: Optional[str] = None,
        compatibility: Optional[bool] = False,
        noDeps: Optional[bool] = False,
        forceRecreate: Optional[bool] = False,
        alwaysRecreateDeps: Optional[bool] = False,
        noRecreate: Optional[bool] = False,
        noBuild: Optional[bool] = False,
        noStart: Optional[bool] = False,
        build: Optional[bool] = False,
        timeout: Optional[int] = None,
        transferBufferSize: int = 2**16,
        renewAnonVolumes: Optional[bool] = False,
        removeOrphans: Optional[bool] = False,
        removeVolumes: Optional[bool] = False,
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

    def base_command(self) -> str:
        return "".join(
            [
                "docker-compose ",
                self.get_option("file", self.files),
                self.get_option("project-name", self.projectName),
                self.get_option("verbose", self.verbose),
                self.get_option("log-level", self.logLevel),
                self.get_option("no-ansi", self.noAnsi),
                self.get_option("host", self.host),
                self.get_option("tls", self.tls),
                self.get_option("tlscacert", self.tlscacert),
                self.get_option("tlscert", self.tlscert),
                self.get_option("tlskey", self.tlskey),
                self.get_option("tlsverify", self.tlsverify),
                self.get_option("skip-hostname-check", self.skipHostnameCheck),
                self.get_option("project-directory", self.projectDirectory),
                self.get_option("compatibility", self.compatibility),
            ]
        )

    async def deploy(self, external: bool) -> None:
        if not external:
            deploy_command = self.base_command() + "".join(
                [
                    "up ",
                    "--detach ",
                    self.get_option("no-deps ", self.noDeps),
                    self.get_option("force-recreate", self.forceRecreate),
                    self.get_option("always-recreate-deps", self.alwaysRecreateDeps),
                    self.get_option("no-recreate", self.noRecreate),
                    self.get_option("no-build", self.noBuild),
                    self.get_option("no-start", self.noStart),
                ]
            )
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("EXECUTING command {}".format(deploy_command))
            proc = await asyncio.create_subprocess_exec(*shlex.split(deploy_command))
            await proc.wait()

    @cachedmethod(lambda self: self.locationsCache)
    async def get_available_locations(
        self,
        service: Optional[str] = None,
        input_directory: Optional[str] = None,
        output_directory: Optional[str] = None,
        tmp_directory: Optional[str] = None,
    ) -> MutableMapping[str, AvailableLocation]:
        ps_command = self.base_command() + "".join(["ps ", service or ""])
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("EXECUTING command {command}".format(command=ps_command))
        proc = await asyncio.create_subprocess_exec(
            *shlex.split(ps_command),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        lines = (line for line in stdout.decode().strip().split("\n"))
        locations = {}
        for line in lines:
            if line.startswith("---------"):
                break
        for line in lines:
            location_name = line.split()[0].strip()
            locations[location_name] = await self._get_location(location_name)
        return locations

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "docker-compose.json")
        )

    async def undeploy(self, external: bool) -> None:
        if not external:
            undeploy_command = self.base_command() + "".join(
                ["down ", "{removeVolumes}"]
            ).format(removeVolumes=self.get_option("volumes", self.removeVolumes))
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "EXECUTING command {command}".format(command=undeploy_command)
                )
            proc = await asyncio.create_subprocess_exec(*shlex.split(undeploy_command))
            await proc.wait()


class SingularityBaseConnector(ContainerConnector, ABC):
    async def _get_bind_mounts(
        self, location: Location
    ) -> MutableSequence[MutableMapping[str, str]]:
        return await self._get_volumes(location)

    async def _get_location(self, location_name: str) -> Optional[AvailableLocation]:
        inspect_command = "singularity instance list --json"
        proc = await asyncio.create_subprocess_exec(
            *shlex.split(inspect_command),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
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

    def _get_run_command(
        self, command: str, location: Location, interactive: bool = False
    ):
        return "".join(
            [
                "singularity ",
                "exec ",
                "instance://",
                location.name,
                " sh -c '{}'".format(command),
            ]
        )

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
            {"Source": line.split()[1], "Destination": line.split()[2]}
            for line in bind_mounts.splitlines()
            if line.split()[0] not in ["tmpfs", "overlay"]
        ]


class SingularityConnector(SingularityBaseConnector):
    def __init__(
        self,
        deployment_name: str,
        config_dir: str,
        image: str,
        transferBufferSize: int = 2**16,
        addCaps: Optional[str] = None,
        allowSetuid: bool = False,
        applyCgroups: Optional[str] = None,
        bind: Optional[MutableSequence[str]] = None,
        boot: bool = False,
        cleanenv: bool = False,
        contain: bool = False,
        containall: bool = False,
        disableCache: bool = False,
        dns: Optional[str] = None,
        dropCaps: Optional[str] = None,
        env: Optional[MutableSequence[str]] = None,
        envFile: Optional[str] = None,
        fakeroot: bool = False,
        fusemount: Optional[MutableSequence[str]] = None,
        home: Optional[str] = None,
        hostname: Optional[str] = None,
        instanceNames: Optional[MutableSequence[str]] = None,
        keepPrivs: bool = False,
        locationsCacheSize: int = None,
        locationsCacheTTL: int = None,
        net: bool = False,
        network: Optional[str] = None,
        networkArgs: Optional[MutableSequence[str]] = None,
        noHome: bool = False,
        noInit: bool = False,
        noMount: Optional[MutableSequence[str]] = None,
        noPrivs: bool = False,
        noUmask: bool = False,
        nohttps: bool = False,
        nv: bool = False,
        overlay: Optional[MutableSequence[str]] = None,
        pemPath: Optional[str] = None,
        pidFile: Optional[str] = None,
        replicas: int = 1,
        resourcesCacheSize: int = None,
        resourcesCacheTTL: int = None,
        rocm: bool = False,
        scratch: Optional[MutableSequence[str]] = None,
        security: Optional[MutableSequence[str]] = None,
        userns: bool = False,
        uts: bool = False,
        workdir: Optional[str] = None,
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
        self.addCaps: Optional[str] = addCaps
        self.allowSetuid: bool = allowSetuid
        self.applyCgroups: Optional[str] = applyCgroups
        self.bind: Optional[MutableSequence[str]] = bind
        self.boot: bool = boot
        self.cleanenv: bool = cleanenv
        self.contain: bool = contain
        self.containall: bool = containall
        self.disableCache: bool = disableCache
        self.dns: Optional[str] = dns
        self.dropCaps: Optional[str] = dropCaps
        self.env: Optional[MutableSequence[str]] = env
        self.envFile: Optional[str] = envFile
        self.fakeroot: bool = fakeroot
        self.fusemount: Optional[MutableSequence[str]] = fusemount
        self.home: Optional[str] = home
        self.hostname: Optional[str] = hostname
        self.instanceNames: MutableSequence[str] = instanceNames or []
        self.keepPrivs: bool = keepPrivs
        self.net: bool = net
        self.network: Optional[str] = network
        self.networkArgs: Optional[MutableSequence[str]] = networkArgs
        self.noHome: bool = noHome
        self.noInit: bool = noInit
        self.noMount: Optional[MutableSequence[str]] = noMount or []
        self.noPrivs: bool = noPrivs
        self.noUmask: bool = noUmask
        self.nohttps: bool = nohttps
        self.nv: bool = nv
        self.overlay: Optional[MutableSequence[str]] = overlay
        self.pemPath: Optional[str] = pemPath
        self.pidFile: Optional[str] = pidFile
        self.replicas: int = replicas
        self.rocm: bool = rocm
        self.scratch: Optional[MutableSequence[str]] = scratch
        self.security: Optional[MutableSequence[str]] = security
        self.userns: bool = userns
        self.uts: bool = uts
        self.workdir: Optional[str] = workdir
        self.writable: bool = writable
        self.writableTmpfs: bool = writableTmpfs

    async def deploy(self, external: bool) -> None:
        if not external:
            for _ in range(0, self.replicas):
                instance_name = utils.random_name()
                deploy_command = "".join(
                    [
                        "singularity ",
                        "instance ",
                        "start ",
                        self.get_option("add-caps", self.addCaps),
                        self.get_option("allow-setuid", self.allowSetuid),
                        self.get_option("apply-cgroups", self.applyCgroups),
                        self.get_option("bind", self.bind),
                        self.get_option("boot", self.boot),
                        self.get_option("cleanenv", self.cleanenv),
                        self.get_option("contain", self.contain),
                        self.get_option("containall", self.containall),
                        self.get_option("disable-cache", self.disableCache),
                        self.get_option("dns", self.dns),
                        self.get_option("drop-caps", self.dropCaps),
                        self.get_option("env", self.env),
                        self.get_option("env-file", self.envFile),
                        self.get_option("fakeroot", self.fakeroot),
                        self.get_option("fusemount", self.fusemount),
                        self.get_option("home", self.home),
                        self.get_option("hostname", self.hostname),
                        self.get_option("keep-privs", self.keepPrivs),
                        self.get_option("net", self.net),
                        self.get_option("network", self.network),
                        self.get_option("network-args", self.networkArgs),
                        self.get_option("no-home", self.noHome),
                        self.get_option("no-init", self.noInit),
                        self.get_option("no-mount", self.noMount),
                        self.get_option("no-privs", self.noPrivs),
                        self.get_option("no-umask", self.noUmask),
                        self.get_option("nohttps", self.nohttps),
                        self.get_option("nv", self.nv),
                        self.get_option("overlay", self.overlay),
                        self.get_option("pem-path", self.pemPath),
                        self.get_option("pid-file", self.pidFile),
                        self.get_option("rocm", self.rocm),
                        self.get_option("scratch", self.scratch),
                        self.get_option("security", self.security),
                        self.get_option("userns", self.userns),
                        self.get_option("uts", self.uts),
                        self.get_option("workdir", self.workdir),
                        self.get_option("writable", self.writable),
                        self.get_option("writable-tmpfs", self.writableTmpfs),
                        "{} ".format(self.image),
                        instance_name,
                    ]
                )
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("EXECUTING command {}".format(deploy_command))
                proc = await asyncio.create_subprocess_exec(
                    *shlex.split(deploy_command),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await proc.communicate()
                if proc.returncode == 0:
                    self.instanceNames.append(instance_name)
                else:
                    raise WorkflowExecutionException(stderr.decode().strip())

    @cachedmethod(lambda self: self.locationsCache)
    async def get_available_locations(
        self,
        service: Optional[str] = None,
        input_directory: Optional[str] = None,
        output_directory: Optional[str] = None,
        tmp_directory: Optional[str] = None,
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
                undeploy_command = "".join(
                    ["singularity ", "instance ", "stop ", instance_name]
                )
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "EXECUTING command {command}".format(command=undeploy_command)
                    )
                proc = await asyncio.create_subprocess_exec(
                    *shlex.split(undeploy_command),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                await proc.wait()
            self.instanceNames = []
