import asyncio
import json
import os
import posixpath
import shlex
from abc import ABC, abstractmethod
from typing import Any, MutableMapping, MutableSequence, Optional, Tuple

import pkg_resources
from cachetools import Cache, TTLCache

from streamflow.core import utils
from streamflow.core.asyncache import cachedmethod
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import Connector
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.scheduling import Location
from streamflow.deployment.connector.base import BaseConnector
from streamflow.log_handler import logger


async def _exists_docker_image(image_name: str) -> bool:
    exists_command = "".join(
        "docker "
        "image "
        "inspect "
        "{image_name}"
    ).format(
        image_name=image_name
    )
    proc = await asyncio.create_subprocess_exec(
        *shlex.split(exists_command),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)
    await proc.wait()
    return proc.returncode == 0


async def _pull_docker_image(image_name: str) -> None:
    exists_command = "".join(
        "docker "
        "pull "
        "--quiet "
        "{image_name}"
    ).format(
        image_name=image_name
    )
    proc = await asyncio.create_subprocess_exec(
        *shlex.split(exists_command),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL)
    await proc.wait()


class ContainerConnector(BaseConnector, ABC):

    def __init__(self,
                 deployment_name: str,
                 context: StreamFlowContext,
                 transferBufferSize: int,
                 locationsCacheSize: int = None,
                 locationsCacheTTL: int = None,
                 resourcesCacheSize: int = None,
                 resourcesCacheTTL: int = None):
        super().__init__(deployment_name, context, transferBufferSize)
        cacheSize = locationsCacheSize
        if cacheSize is None:
            cacheSize = resourcesCacheSize
            if cacheSize is not None:
                logger.warn("The `resourcesCacheSize` keyword is deprecated and will be removed in StreamFlow 0.3.0. "
                            "Use `locationsCacheSize` instead.")
            else:
                cacheSize = 10
        cacheTTL = locationsCacheTTL
        if cacheTTL is None:
            cacheTTL = resourcesCacheTTL
            if cacheTTL is not None:
                logger.warn("The `resourcesCacheTTL` keyword is deprecated and will be removed in StreamFlow 0.3.0. "
                            "Use `locationsCacheTTL` instead.")
            else:
                cacheTTL = 10
        self.locationsCache: Cache = TTLCache(maxsize=cacheSize, ttl=cacheTTL)

    async def _check_effective_location(self,
                                        common_paths: MutableMapping[str, Any],
                                        effective_locations: MutableSequence[str],
                                        location: str,
                                        path: str,
                                        source_location: Optional[str] = None
                                        ) -> Tuple[MutableMapping[str, Any], MutableSequence[str]]:
        # Get all container mounts
        volumes = await self._get_volumes(location)
        for volume in volumes:
            # If path is in a persistent volume
            if path.startswith(volume['Destination']):
                if path not in common_paths:
                    common_paths[path] = []
                # Check if path is shared with another location that has been already processed
                for i, common_path in enumerate(common_paths[path]):
                    if common_path['source'] == volume['Source']:
                        # If path has already been processed, but the current location is the source, substitute it
                        if location == source_location:
                            effective_locations.remove(common_path['location'])
                            common_path['location'] = location
                            common_paths[path][i] = common_path
                            effective_locations.append(location)
                            return common_paths, effective_locations
                        # Otherwise simply skip current location
                        else:
                            return common_paths, effective_locations
                # If this is the first location encountered with the persistent path, add it to the list
                effective_locations.append(location)
                common_paths[path].append({'source': volume['Source'], 'location': location})
                return common_paths, effective_locations
        # If path is not in a persistent volume, add the current location to the list
        effective_locations.append(location)
        return common_paths, effective_locations

    async def _copy_local_to_remote(self,
                                    src: str,
                                    dst: str,
                                    locations: MutableSequence[str],
                                    read_only: bool = False) -> None:
        effective_locations = await self._get_effective_locations(locations, dst)
        copy_tasks = []
        for location in effective_locations:
            if read_only and await self._is_bind_transfer(location, src, dst):
                copy_tasks.append(asyncio.create_task(
                    self.run(
                        location=location,
                        command=["ln", "-snf", posixpath.abspath(src), dst])))
                continue
            copy_tasks.append(asyncio.create_task(
                self._copy_local_to_remote_single(
                    src=src,
                    dst=dst,
                    location=location,
                    read_only=read_only)))
        await asyncio.gather(*copy_tasks)

    async def _copy_remote_to_remote(self,
                                     src: str,
                                     dst: str,
                                     locations: MutableSequence[str],
                                     source_location: str,
                                     source_connector: Optional[Connector] = None,
                                     read_only: bool = False) -> None:
        source_connector = source_connector or self
        effective_locations = await self._get_effective_locations(locations, dst, source_location)
        non_bind_locations = []
        if read_only and source_connector == self and await self._is_bind_transfer(source_location, src, src):
            for location in effective_locations:
                if await self._is_bind_transfer(location, dst, dst):
                    await self.run(
                        location=location,
                        command=["ln", "-snf", posixpath.abspath(src), dst])
                    continue
                non_bind_locations.append(location)
            await super()._copy_remote_to_remote(
                src=src,
                dst=dst,
                locations=non_bind_locations,
                source_connector=source_connector,
                source_location=source_location,
                read_only=read_only)
        else:
            await super()._copy_remote_to_remote(
                src=src,
                dst=dst,
                locations=effective_locations,
                source_connector=source_connector,
                source_location=source_location,
                read_only=read_only)

    async def _get_effective_locations(self,
                                       locations: MutableSequence[str],
                                       dest_path: str,
                                       source_location: Optional[str] = None) -> MutableSequence[str]:
        common_paths = {}
        effective_locations = []
        for location in locations:
            common_paths, effective_locations = await self._check_effective_location(
                common_paths,
                effective_locations,
                location,
                dest_path,
                source_location)
        return effective_locations

    @abstractmethod
    async def _get_bind_mounts(self,
                               location: str) -> MutableSequence[MutableMapping[str, str]]:
        ...

    @abstractmethod
    async def _get_location(self,
                            location_name: str) -> Optional[Location]:
        ...

    @abstractmethod
    async def _get_volumes(self,
                           location: str) -> MutableSequence[MutableMapping[str, str]]:
        ...

    async def _is_bind_transfer(self,
                                location: str,
                                host_path: str,
                                instance_path: str) -> bool:
        bind_mounts = await self._get_bind_mounts(location)
        host_binds = [b['Source'] for b in bind_mounts]
        instance_binds = [b['Destination'] for b in bind_mounts]
        for host_bind in host_binds:
            if host_path.startswith(host_bind):
                for instance_bind in instance_binds:
                    if instance_path.startswith(instance_bind):
                        return True
        return False


class DockerBaseConnector(ContainerConnector, ABC):

    async def _get_bind_mounts(self,
                               location: str) -> MutableSequence[MutableMapping[str, str]]:
        return [v for v in await self._get_volumes(location) if v['Type'] == 'bind']

    async def _get_location(self,
                            location_name: str) -> Location:
        inspect_command = "".join([
            "docker ",
            "inspect ",
            "--format ",
            "'{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ",
            location_name])
        proc = await asyncio.create_subprocess_exec(
            *shlex.split(inspect_command),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        stdout, _ = await proc.communicate()
        return Location(name=location_name, hostname=stdout.decode().strip())

    def _get_run_command(self,
                         command: str,
                         location: str,
                         interactive: bool = False):
        return "".join([
            "docker "
            "exec ",
            "-i " if interactive else "",
            "{location} ",
            "sh -c '{command}'"
        ]).format(
            location=location,
            command=command
        )

    async def _get_volumes(self,
                           location: str) -> MutableSequence[MutableMapping[str, str]]:
        inspect_command = "".join([
            "docker ",
            "inspect ",
            "--format ",
            "'{{json .Mounts }}' ",
            location])
        proc = await asyncio.create_subprocess_exec(
            *shlex.split(inspect_command),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        stdout, _ = await proc.communicate()
        return json.loads(stdout.decode().strip())


class DockerConnector(DockerBaseConnector):

    def __init__(self,
                 deployment_name: str,
                 context: StreamFlowContext,
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
                 transferBufferSize: int = 2 ** 16,
                 ulimit: Optional[MutableSequence[str]] = None,
                 user: Optional[str] = None,
                 userns: Optional[str] = None,
                 uts: Optional[str] = None,
                 volume: Optional[MutableSequence[str]] = None,
                 volumeDriver: Optional[str] = None,
                 volumesFrom: Optional[MutableSequence[str]] = None,
                 workdir: Optional[str] = None):
        super().__init__(
            deployment_name=deployment_name,
            context=context,
            transferBufferSize=transferBufferSize,
            locationsCacheSize=locationsCacheSize,
            locationsCacheTTL=locationsCacheTTL,
            resourcesCacheSize=resourcesCacheSize,
            resourcesCacheTTL=resourcesCacheTTL)
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
                deploy_command = "".join([
                    "docker ",
                    "run "
                    "--detach "
                    "--interactive "
                    "{addHost}"
                    "{blkioWeight}"
                    "{blkioWeightDevice}"
                    "{capAdd}"
                    "{capDrop}"
                    "{cgroupParent}"
                    "{cidfile}"
                    "{cpuPeriod}"
                    "{cpuQuota}"
                    "{cpuRTPeriod}"
                    "{cpuRTRuntime}"
                    "{cpuShares}"
                    "{cpus}"
                    "{cpusetCpus}"
                    "{cpusetMems}"
                    "{detachKeys}"
                    "{device}"
                    "{deviceCgroupRule}"
                    "{deviceReadBps}"
                    "{deviceReadIops}"
                    "{deviceWriteBps}"
                    "{deviceWriteIops}"
                    "{disableContentTrust}"
                    "{dns}"
                    "{dnsOptions}"
                    "{dnsSearch}"
                    "{domainname}"
                    "{entrypoint}"
                    "{env}"
                    "{envFile}"
                    "{expose}"
                    "{gpus}"
                    "{groupAdd}"
                    "{healthCmd}"
                    "{healthInterval}"
                    "{healthRetries}"
                    "{healthStartPeriod}"
                    "{healthTimeout}"
                    "{hostname}"
                    "{init}"
                    "{ip}"
                    "{ip6}"
                    "{ipc}"
                    "{isolation}"
                    "{kernelMemory}"
                    "{label}"
                    "{labelFile}"
                    "{link}"
                    "{linkLocalIP}"
                    "{logDriver}"
                    "{logOpts}"
                    "{macAddress}"
                    "{memory}"
                    "{memoryReservation}"
                    "{memorySwap}"
                    "{memorySwappiness}"
                    "{mount}"
                    "{network}"
                    "{networkAlias}"
                    "{noHealthcheck}"
                    "{oomKillDisable}"
                    "{oomScoreAdj}"
                    "{pid}"
                    "{pidsLimit}"
                    "{privileged}"
                    "{publish}"
                    "{publishAll}"
                    "{readOnly}"
                    "{restart}"
                    "{rm}"
                    "{runtime}"
                    "{securityOpts}"
                    "{shmSize}"
                    "{sigProxy}"
                    "{stopSignal}"
                    "{stopTimeout}"
                    "{storageOpts}"
                    "{sysctl}"
                    "{tmpfs}"
                    "{ulimit}"
                    "{user}"
                    "{userns}"
                    "{uts}"
                    "{volume}"
                    "{volumeDriver}"
                    "{volumesFrom}"
                    "{workdir}"
                    "{image}"
                ]).format(
                    addHost=self.get_option("add-host", self.addHost),
                    blkioWeight=self.get_option("blkio-weight", self.addHost),
                    blkioWeightDevice=self.get_option("blkio-weight-device", self.blkioWeightDevice),
                    capAdd=self.get_option("cap-add", self.capAdd),
                    capDrop=self.get_option("cap-drop", self.capDrop),
                    cgroupParent=self.get_option("cgroup-parent", self.cgroupParent),
                    cidfile=self.get_option("cidfile", self.cidfile),
                    cpuPeriod=self.get_option("cpu-period", self.cpuPeriod),
                    cpuQuota=self.get_option("cpu-quota", self.cpuQuota),
                    cpuRTPeriod=self.get_option("cpu-rt-period", self.cpuRTPeriod),
                    cpuRTRuntime=self.get_option("cpu-rt-runtime", self.cpuRTRuntime),
                    cpuShares=self.get_option("cpu-shares", self.cpuShares),
                    cpus=self.get_option("cpus", self.cpus),
                    cpusetCpus=self.get_option("cpuset-cpus", self.cpusetCpus),
                    cpusetMems=self.get_option("cpuset-mems", self.cpusetMems),
                    detachKeys=self.get_option("detach-keys", self.detachKeys),
                    device=self.get_option("device", self.device),
                    deviceCgroupRule=self.get_option("device-cgroup-rule", self.deviceCgroupRule),
                    deviceReadBps=self.get_option("device-read-bps", self.deviceReadBps),
                    deviceReadIops=self.get_option("device-read-iops", self.deviceReadIops),
                    deviceWriteBps=self.get_option("device-write-bps", self.deviceWriteBps),
                    deviceWriteIops=self.get_option("device-write-iops", self.deviceWriteIops),
                    disableContentTrust="--disable-content-trust={disableContentTrust} ".format(
                        disableContentTrust="true" if self.disableContentTrust else "false"),
                    dns=self.get_option("dns", self.dns),
                    dnsOptions=self.get_option("dns-option", self.dnsOptions),
                    dnsSearch=self.get_option("dns-search", self.dnsSearch),
                    domainname=self.get_option("domainname", self.domainname),
                    entrypoint=self.get_option("entrypoint", self.entrypoint),
                    env=self.get_option("env", self.env),
                    envFile=self.get_option("env-file", self.envFile),
                    expose=self.get_option("expose", self.expose),
                    gpus=self.get_option("gpus", self.gpus),
                    groupAdd=self.get_option("group-add", self.groupAdd),
                    healthCmd=self.get_option("health-cmd", self.healthCmd),
                    healthInterval=self.get_option("health-interval", self.healthInterval),
                    healthRetries=self.get_option("health-retries", self.healthRetries),
                    healthStartPeriod=self.get_option("health-start-period", self.healthStartPeriod),
                    healthTimeout=self.get_option("health-timeout", self.healthTimeout),
                    hostname=self.get_option("hostname", self.hostname),
                    init=self.get_option("init", self.init),
                    ip=self.get_option("ip", self.ip),
                    ip6=self.get_option("ip6", self.ip6),
                    ipc=self.get_option("ipc", self.ipc),
                    isolation=self.get_option("isolation", self.isolation),
                    kernelMemory=self.get_option("kernel-memory", self.kernelMemory),
                    label=self.get_option("label", self.label),
                    labelFile=self.get_option("label-file", self.labelFile),
                    link=self.get_option("link", self.link),
                    linkLocalIP=self.get_option("link-local-ip", self.linkLocalIP),
                    logDriver=self.get_option("log-driver", self.logDriver),
                    logOpts=self.get_option("log-opt", self.logOpts),
                    macAddress=self.get_option("mac-address", self.macAddress),
                    memory=self.get_option("memory", self.memory),
                    memoryReservation=self.get_option("memory-reservation", self.memoryReservation),
                    memorySwap=self.get_option("memory-swap", self.memorySwap),
                    memorySwappiness=self.get_option("memory-swappiness", self.memorySwappiness),
                    mount=self.get_option("mount", self.mount),
                    network=self.get_option("network", self.network),
                    networkAlias=self.get_option("network-alias", self.networkAlias),
                    noHealthcheck=self.get_option("no-healthcheck", self.noHealthcheck),
                    oomKillDisable=self.get_option("oom-kill-disable", self.oomKillDisable),
                    oomScoreAdj=self.get_option("oom-score-adj", self.oomScoreAdj),
                    pid=self.get_option("pid", self.pid),
                    pidsLimit=self.get_option("pids-limit", self.pidsLimit),
                    privileged=self.get_option("privileged", self.privileged),
                    publish=self.get_option("publish", self.publish),
                    publishAll=self.get_option("publish-all", self.publishAll),
                    readOnly=self.get_option("read-only", self.readOnly),
                    restart=self.get_option("restart", self.restart),
                    rm=self.get_option("rm", self.rm),
                    runtime=self.get_option("runtime", self.runtime),
                    securityOpts=self.get_option("security-opt", self.securityOpts),
                    shmSize=self.get_option("shm-size", self.shmSize),
                    sigProxy="--sig-proxy={sigProxy} ".format(sigProxy="true" if self.sigProxy else "false"),
                    stopSignal=self.get_option("stop-signal", self.stopSignal),
                    stopTimeout=self.get_option("stop-timeout", self.stopTimeout),
                    storageOpts=self.get_option("storage-opt", self.storageOpts),
                    sysctl=self.get_option("sysctl", self.sysctl),
                    tmpfs=self.get_option("tmpfs", self.tmpfs),
                    ulimit=self.get_option("ulimit", self.ulimit),
                    user=self.get_option("user", self.user),
                    userns=self.get_option("userns", self.userns),
                    uts=self.get_option("uts", self.uts),
                    volume=self.get_option("volume", self.volume),
                    volumeDriver=self.get_option("volume-driver", self.volumeDriver),
                    volumesFrom=self.get_option("volumes-from", self.volumesFrom),
                    workdir=self.get_option("workdir", self.workdir),
                    image=self.image
                )
                logger.debug("Executing command {command}".format(command=deploy_command))
                proc = await asyncio.create_subprocess_exec(
                    *shlex.split(deploy_command),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE)
                stdout, stderr = await proc.communicate()
                if proc.returncode == 0:
                    self.containerIds.append(stdout.decode().strip())
                else:
                    raise WorkflowExecutionException(stderr.decode().strip())

    @cachedmethod(lambda self: self.locationsCache)
    async def get_available_locations(self,
                                      service: str,
                                      input_directory: Optional[str] = None,
                                      output_directory: Optional[str] = None,
                                      tmp_directory: Optional[str] = None) -> MutableMapping[str, Location]:
        return {container_id: await self._get_location(container_id) for container_id in self.containerIds}

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join('schemas', 'docker.json'))

    async def undeploy(self, external: bool) -> None:
        if not external and self.containerIds:
            for container_id in self.containerIds:
                undeploy_command = "".join([
                    "docker ",
                    "stop "
                    "{containerId}"
                ]).format(
                    containerId=container_id
                )
                logger.debug("Executing command {command}".format(command=undeploy_command))
                proc = await asyncio.create_subprocess_exec(
                    *shlex.split(undeploy_command),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE)
                await proc.wait()
            self.containerIds = []


class DockerComposeConnector(DockerBaseConnector):

    def __init__(self,
                 deployment_name: str,
                 context: StreamFlowContext,
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
                 transferBufferSize: int = 2 ** 16,
                 renewAnonVolumes: Optional[bool] = False,
                 removeOrphans: Optional[bool] = False,
                 removeVolumes: Optional[bool] = False,
                 locationsCacheSize: int = None,
                 locationsCacheTTL: int = None,
                 resourcesCacheSize: int = None,
                 resourcesCacheTTL: int = None) -> None:
        super().__init__(
            deployment_name=deployment_name,
            context=context,
            transferBufferSize=transferBufferSize,
            locationsCacheSize=locationsCacheSize,
            locationsCacheTTL=locationsCacheTTL,
            resourcesCacheSize=resourcesCacheSize,
            resourcesCacheTTL=resourcesCacheTTL)
        self.files = [os.path.join(context.config_dir, file) for file in files]
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
        return "".join([
            "docker-compose ",
            "{files}",
            "{projectName}",
            "{verbose}",
            "{logLevel}",
            "{noAnsi}",
            "{host}",
            "{tls}",
            "{tlscacert}",
            "{tlscert}",
            "{tlskey}",
            "{tlsverify}",
            "{skipHostnameCheck}",
            "{projectDirectory}",
            "{compatibility} "
        ]).format(
            files=self.get_option("file", self.files),
            projectName=self.get_option("project-name", self.projectName),
            verbose=self.get_option("verbose", self.verbose),
            logLevel=self.get_option("log-level", self.logLevel),
            noAnsi=self.get_option("no-ansi", self.noAnsi),
            host=self.get_option("host", self.host),
            tls=self.get_option("tls", self.tls),
            tlscacert=self.get_option("tlscacert", self.tlscacert),
            tlscert=self.get_option("tlscert", self.tlscert),
            tlskey=self.get_option("tlskey", self.tlskey),
            tlsverify=self.get_option("tlsverify", self.tlsverify),
            skipHostnameCheck=self.get_option("skip-hostname-check", self.skipHostnameCheck),
            projectDirectory=self.get_option("project-directory", self.projectDirectory),
            compatibility=self.get_option("compatibility", self.compatibility)
        )

    async def deploy(self, external: bool) -> None:
        if not external:
            deploy_command = self.base_command() + "".join([
                "up ",
                "--detach ",
                "{noDeps}",
                "{forceRecreate}",
                "{alwaysRecreateDeps}",
                "{noRecreate}",
                "{noBuild}",
                "{noStart}"
            ]).format(
                noDeps=self.get_option("no-deps ", self.noDeps),
                forceRecreate=self.get_option("force-recreate", self.forceRecreate),
                alwaysRecreateDeps=self.get_option("always-recreate-deps", self.alwaysRecreateDeps),
                noRecreate=self.get_option("no-recreate", self.noRecreate),
                noBuild=self.get_option("no-build", self.noBuild),
                noStart=self.get_option("no-start", self.noStart)
            )
            logger.debug("Executing command {command}".format(command=deploy_command))
            proc = await asyncio.create_subprocess_exec(*shlex.split(deploy_command))
            await proc.wait()

    @cachedmethod(lambda self: self.locationsCache)
    async def get_available_locations(self,
                                      service: str,
                                      input_directory: Optional[str] = None,
                                      output_directory: Optional[str] = None,
                                      tmp_directory: Optional[str] = None) -> MutableMapping[str, Location]:
        ps_command = self.base_command() + "".join([
            "ps ",
            service or ""])
        logger.debug("Executing command {command}".format(command=ps_command))
        proc = await asyncio.create_subprocess_exec(
            *shlex.split(ps_command),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        stdout, _ = await proc.communicate()
        lines = (line for line in stdout.decode().strip().split('\n'))
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
            __name__, os.path.join('schemas', 'docker-compose.json'))

    async def undeploy(self, external: bool) -> None:
        if not external:
            undeploy_command = self.base_command() + "".join([
                "down ",
                "{removeVolumes}"
            ]).format(
                removeVolumes=self.get_option("volumes", self.removeVolumes)
            )
            logger.debug("Executing command {command}".format(command=undeploy_command))
            proc = await asyncio.create_subprocess_exec(*shlex.split(undeploy_command))
            await proc.wait()


class SingularityBaseConnector(ContainerConnector, ABC):

    async def _get_bind_mounts(self, location: str) -> MutableSequence[MutableMapping[str, str]]:
        return await self._get_volumes(location)

    async def _get_location(self, location_name: str) -> Optional[Location]:
        inspect_command = "singularity instance list --json"
        proc = await asyncio.create_subprocess_exec(
            *shlex.split(inspect_command),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        stdout, _ = await proc.communicate()
        if stdout:
            json_out = json.loads(stdout)
            for instance in json_out["instances"]:
                if instance["instance"] == location_name:
                    return Location(name=location_name, hostname=instance["ip"])
        else:
            return None

    def _get_run_command(self,
                         command: str,
                         location: str,
                         interactive: bool = False):
        return "".join([
            "singularity "
            "exec ",
            "instance://{location} "
            "sh -c '{command}'"
        ]).format(
            location=location,
            command=command
        )

    async def _get_volumes(self,
                           location: str) -> MutableSequence[MutableMapping[str, str]]:
        bind_mounts, _ = await self.run(
            location=location,
            command=["cat", "/proc/1/mountinfo", "|", "awk", "'{print $9,$4,$5}'"],
            capture_output=True)
        # Exclude `overlay` and `tmpfs` mounts
        return [{'Source': line.split()[1], 'Destination': line.split()[2]} for line in bind_mounts.splitlines()
                if line.split()[0] not in ['tmpfs', 'overlay']]


class SingularityConnector(SingularityBaseConnector):

    def __init__(self,
                 deployment_name: str,
                 context: StreamFlowContext,
                 image: str,
                 transferBufferSize: int = 2 ** 16,
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
                 writableTmpfs: bool = False):
        super().__init__(
            deployment_name=deployment_name,
            context=context,
            transferBufferSize=transferBufferSize,
            locationsCacheSize=locationsCacheSize,
            locationsCacheTTL=locationsCacheTTL,
            resourcesCacheSize=resourcesCacheSize,
            resourcesCacheTTL=resourcesCacheTTL)
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
                deploy_command = "".join([
                    "singularity ",
                    "instance "
                    "start "
                    "{addCaps}"
                    "{allowSetuid}"
                    "{applyCgroups}"
                    "{bind}"
                    "{boot}"
                    "{cleanenv}"
                    "{contain}"
                    "{containall}"
                    "{disableCache}"
                    "{dns}"
                    "{dropCaps}"
                    "{env}"
                    "{envFile}"
                    "{fakeroot}"
                    "{fusemount}"
                    "{home}"
                    "{hostname}"
                    "{keepPrivs}"
                    "{net}"
                    "{network}"
                    "{networkArgs}"
                    "{noHome}"
                    "{noInit}"
                    "{noMount}"
                    "{noPrivs}"
                    "{noUmask}"
                    "{nohttps}"
                    "{nv}"
                    "{overlay}"
                    "{pemPath}"
                    "{pidFile}"
                    "{rocm}"
                    "{scratch}"
                    "{security}"
                    "{userns}"
                    "{uts}"
                    "{workdir}"
                    "{writable}"
                    "{writableTmpfs}"
                    "{image} "
                    "{name}"
                ]).format(
                    addCaps=self.get_option("add-caps", self.addCaps),
                    allowSetuid=self.get_option("allow-setuid", self.allowSetuid),
                    applyCgroups=self.get_option("apply-cgroups", self.applyCgroups),
                    bind=self.get_option("bind", self.bind),
                    boot=self.get_option("boot", self.boot),
                    cleanenv=self.get_option("cleanenv", self.cleanenv),
                    contain=self.get_option("contain", self.contain),
                    containall=self.get_option("containall", self.containall),
                    disableCache=self.get_option("disable-cache", self.disableCache),
                    dns=self.get_option("dns", self.dns),
                    dropCaps=self.get_option("drop-caps", self.dropCaps),
                    env=self.get_option("env", self.env),
                    envFile=self.get_option("env-file", self.envFile),
                    fakeroot=self.get_option("fakeroot", self.fakeroot),
                    fusemount=self.get_option("fusemount", self.fusemount),
                    home=self.get_option("home", self.home),
                    hostname=self.get_option("hostname", self.hostname),
                    keepPrivs=self.get_option("keep-privs", self.keepPrivs),
                    net=self.get_option("net", self.net),
                    network=self.get_option("network", self.network),
                    networkArgs=self.get_option("network-args", self.networkArgs),
                    noHome=self.get_option("no-home", self.noHome),
                    noInit=self.get_option("no-init", self.noInit),
                    noMount=self.get_option("no-mount", self.noMount),
                    noPrivs=self.get_option("no-privs", self.noPrivs),
                    noUmask=self.get_option("no-umask", self.noUmask),
                    nohttps=self.get_option("nohttps", self.nohttps),
                    nv=self.get_option("nv", self.nv),
                    overlay=self.get_option("overlay", self.overlay),
                    pemPath=self.get_option("pem-path", self.pemPath),
                    pidFile=self.get_option("pid-file", self.pidFile),
                    rocm=self.get_option("rocm", self.rocm),
                    scratch=self.get_option("scratch", self.scratch),
                    security=self.get_option("security", self.security),
                    userns=self.get_option("userns", self.userns),
                    uts=self.get_option("uts", self.uts),
                    workdir=self.get_option("workdir", self.workdir),
                    writable=self.get_option("writable", self.writable),
                    writableTmpfs=self.get_option("writable-tmpfs", self.writableTmpfs),
                    image=self.image,
                    name=instance_name
                )
                logger.debug("Executing command {command}".format(command=deploy_command))
                proc = await asyncio.create_subprocess_exec(
                    *shlex.split(deploy_command),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE)
                stdout, stderr = await proc.communicate()
                if proc.returncode == 0:
                    self.instanceNames.append(instance_name)
                else:
                    raise WorkflowExecutionException(stderr.decode().strip())

    @cachedmethod(lambda self: self.locationsCache)
    async def get_available_locations(self,
                                      service: str,
                                      input_directory: Optional[str] = None,
                                      output_directory: Optional[str] = None,
                                      tmp_directory: Optional[str] = None) -> MutableMapping[str, Location]:
        location_tasks = {}
        for instance_name in self.instanceNames:
            location_tasks[instance_name] = asyncio.create_task(self._get_location(instance_name))
        return {k: v for k, v in zip(location_tasks.keys(), await asyncio.gather(*location_tasks.values()))
                if v is not None}

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join('schemas', 'singularity.json'))

    async def undeploy(self, external: bool) -> None:
        if not external and self.instanceNames:
            for instance_name in self.instanceNames:
                undeploy_command = "".join([
                    "singularity ",
                    "instance "
                    "stop "
                    "{containerId}"
                ]).format(
                    containerId=instance_name
                )
                logger.debug("Executing command {command}".format(command=undeploy_command))
                proc = await asyncio.create_subprocess_exec(
                    *shlex.split(undeploy_command),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE)
                await proc.wait()
            self.instanceNames = []
