from __future__ import annotations

import os
import posixpath
from typing import MutableSequence

import pkg_resources

from streamflow.core import utils
from streamflow.core.deployment import DeploymentConfig, Target
from streamflow.cwl.requirement.docker.translator import CWLDockerTranslator


class DockerCWLDockerTranslator(CWLDockerTranslator):
    def __init__(
        self,
        config_dir: str,
        wrapper: bool,
        addHost: MutableSequence[str] | None = None,
        blkioWeight: int | None = None,
        blkioWeightDevice: MutableSequence[int] | None = None,
        capAdd: MutableSequence[str] | None = None,
        capDrop: MutableSequence[str] | None = None,
        cgroupParent: str | None = None,
        cgroupns: str | None = None,
        cidfile: str | None = None,
        command: MutableSequence[str] | None = None,
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
        logDriver: str = "none",
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
        super().__init__(config_dir=config_dir, wrapper=wrapper)
        self.addHost: MutableSequence[str] | None = addHost
        self.blkioWeight: int | None = blkioWeight
        self.blkioWeightDevice: MutableSequence[int] | None = blkioWeightDevice
        self.capAdd: MutableSequence[str] | None = capAdd
        self.capDrop: MutableSequence[str] | None = capDrop
        self.cgroupParent: str | None = cgroupParent
        self.cgroupns: str | None = cgroupns
        self.cidfile: str | None = cidfile
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
        self.locationsCacheSize: int | None = locationsCacheSize
        self.locationsCacheTTL: int | None = locationsCacheTTL
        self.logDriver: str = logDriver
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
        self.transferBufferSize: int = transferBufferSize
        self.ulimit: MutableSequence[str] | None = ulimit
        self.user: str | None = user
        self.userns: str | None = userns
        self.uts: str | None = uts
        self.volume: MutableSequence[str] | None = volume
        self.volumeDriver: str | None = volumeDriver
        self.volumesFrom: MutableSequence[str] | None = volumesFrom
        self.workdir: str | None = workdir

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "docker.json")
        )

    def get_target(
        self,
        image: str,
        output_directory: str | None,
        network_access: bool,
        target: Target,
    ) -> Target:
        volume = list(self.volume) if self.volume else []
        volume.append(f"{target.workdir}:/tmp/streamflow")
        if output_directory is not None:
            if target.deployment.type == "local":
                volume.append(
                    f"{os.path.join(target.workdir, utils.random_name())}:{output_directory}"
                )
            else:
                volume.append(
                    f"{posixpath.join(target.workdir, utils.random_name())}:{output_directory}"
                )
        return Target(
            deployment=DeploymentConfig(
                name=utils.random_name(),
                type="docker",
                config={
                    "image": image,
                    "addHost": self.addHost,
                    "blkioWeight": self.blkioWeight,
                    "blkioWeightDevice": self.blkioWeightDevice,
                    "capAdd": self.capAdd,
                    "capDrop": self.capDrop,
                    "cgroupParent": self.cgroupParent,
                    "cgroupns": self.cgroupns,
                    "cidfile": self.cidfile,
                    "command": self.command,
                    "cpuPeriod": self.cpuPeriod,
                    "cpuQuota": self.cpuQuota,
                    "cpuRTPeriod": self.cpuRTPeriod,
                    "cpuRTRuntime": self.cpuRTRuntime,
                    "cpuShares": self.cpuShares,
                    "cpus": self.cpus,
                    "cpusetCpus": self.cpusetCpus,
                    "cpusetMems": self.cpusetMems,
                    "detachKeys": self.detachKeys,
                    "device": self.device,
                    "deviceCgroupRule": self.deviceCgroupRule,
                    "deviceReadBps": self.deviceReadBps,
                    "deviceReadIops": self.deviceReadIops,
                    "deviceWriteBps": self.deviceWriteBps,
                    "deviceWriteIops": self.deviceWriteIops,
                    "disableContentTrust": self.disableContentTrust,
                    "dns": self.dns,
                    "dnsOptions": self.dnsOptions,
                    "dnsSearch": self.dnsSearch,
                    "domainname": self.domainname,
                    "entrypoint": self.entrypoint,
                    "env": self.env,
                    "envFile": self.envFile,
                    "expose": self.expose,
                    "gpus": self.gpus,
                    "groupAdd": self.groupAdd,
                    "healthCmd": self.healthCmd,
                    "healthInterval": self.healthInterval,
                    "healthRetries": self.healthRetries,
                    "healthStartPeriod": self.healthStartPeriod,
                    "healthTimeout": self.healthTimeout,
                    "hostname": self.hostname,
                    "init": self.init,
                    "ip": self.ip,
                    "ip6": self.ip6,
                    "ipc": self.ipc,
                    "isolation": self.isolation,
                    "kernelMemory": self.kernelMemory,
                    "label": self.label,
                    "labelFile": self.labelFile,
                    "link": self.link,
                    "linkLocalIP": self.linkLocalIP,
                    "locationsCacheSize": self.locationsCacheSize,
                    "locationsCacheTTL": self.locationsCacheTTL,
                    "logDriver": self.logDriver,
                    "logOpts": self.logOpts,
                    "macAddress": self.macAddress,
                    "memory": self.memory,
                    "memoryReservation": self.memoryReservation,
                    "memorySwap": self.memorySwap,
                    "memorySwappiness": self.memorySwappiness,
                    "mount": self.mount,
                    "network": self.network if network_access else "none",
                    "networkAlias": self.networkAlias,
                    "noHealthcheck": self.noHealthcheck,
                    "oomKillDisable": self.oomKillDisable,
                    "oomScoreAdj": self.oomScoreAdj,
                    "pid": self.pid,
                    "pidsLimit": self.pidsLimit,
                    "privileged": self.privileged,
                    "publish": self.publish,
                    "publishAll": self.publishAll,
                    "readOnly": self.readOnly,
                    "replicas": self.replicas,
                    "restart": self.restart,
                    "rm": self.rm,
                    "runtime": self.runtime,
                    "securityOpts": self.securityOpts,
                    "shmSize": self.shmSize,
                    "sigProxy": self.sigProxy,
                    "stopSignal": self.stopSignal,
                    "stopTimeout": self.stopTimeout,
                    "storageOpts": self.storageOpts,
                    "sysctl": self.sysctl,
                    "tmpfs": self.tmpfs,
                    "transferBufferSize": self.transferBufferSize,
                    "ulimit": self.ulimit,
                    "user": self.user,
                    "userns": self.userns,
                    "uts": self.uts,
                    "volume": volume,
                    "volumeDriver": self.volumeDriver,
                    "volumesFrom": self.volumesFrom,
                },
                workdir="/tmp/streamflow",  # nosec
                wraps=target if self.wrapper else None,
            ),
            service=image,
        )
