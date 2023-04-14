from __future__ import annotations

import os
import posixpath
from typing import MutableSequence

import pkg_resources

from streamflow.core import utils
from streamflow.core.deployment import DeploymentConfig, Target
from streamflow.cwl.requirement.docker.translator import CWLDockerTranslator


class SingularityCWLDockerTranslator(CWLDockerTranslator):
    def __init__(
        self,
        config_dir: str,
        wrapper: bool,
        transferBufferSize: int = 2**16,
        addCaps: str | None = None,
        allowSetuid: bool = False,
        applyCgroups: str | None = None,
        bind: MutableSequence[str] | None = None,
        blkioWeight: int | None = None,
        blkioWeightDevice: MutableSequence[str] | None = None,
        boot: bool = False,
        cleanenv: bool = True,
        command: MutableSequence[str] | None = None,
        compat: bool = False,
        contain: bool = True,
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
        rocm: bool = False,
        scratch: MutableSequence[str] | None = None,
        security: MutableSequence[str] | None = None,
        userns: bool = False,
        uts: bool = False,
        workdir: str | None = None,
        writable: bool = False,
        writableTmpfs: bool = False,
    ):
        super().__init__(config_dir=config_dir, wrapper=wrapper)
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
        self.locationsCacheSize: int | None = locationsCacheSize
        self.locationsCacheTTL: int | None = locationsCacheTTL
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
        self.transferBufferSize: int = transferBufferSize
        self.userns: bool = userns
        self.uts: bool = uts
        self.workdir: str | None = workdir
        self.writable: bool = writable
        self.writableTmpfs: bool = writableTmpfs

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "singularity.json")
        )

    def get_target(
        self,
        image: str,
        output_directory: str | None,
        network_access: bool,
        target: Target,
    ) -> Target:
        bind = list(self.bind) if self.bind else []
        bind.append(f"{target.workdir}:/tmp/streamflow")
        if output_directory is not None:
            if target.deployment.type == "local":
                bind.append(
                    f"{os.path.join(target.workdir, utils.random_name())}:{output_directory}"
                )
            else:
                bind.append(
                    f"{posixpath.join(target.workdir, utils.random_name())}:{output_directory}"
                )
        return Target(
            deployment=DeploymentConfig(
                name=utils.random_name(),
                type="singularity",
                config={
                    "image": f"docker://{image}",
                    "addCaps": self.addCaps,
                    "allowSetuid": self.allowSetuid,
                    "applyCgroups": self.applyCgroups,
                    "bind": bind,
                    "blkioWeight": self.blkioWeight,
                    "blkioWeightDevice": self.blkioWeightDevice,
                    "boot": self.boot,
                    "cleanenv": self.cleanenv,
                    "command": self.command,
                    "compat": self.compat,
                    "contain": self.contain,
                    "containall": self.containall,
                    "cpuShares": self.cpuShares,
                    "cpus": self.cpus,
                    "cpusetCpus": self.cpusetCpus,
                    "cpusetMems": self.cpusetMems,
                    "disableCache": self.disableCache,
                    "dns": self.dns,
                    "dropCaps": self.dropCaps,
                    "dockerHost": self.dockerHost,
                    "env": self.env,
                    "envFile": self.envFile,
                    "fakeroot": self.fakeroot,
                    "fusemount": self.fusemount,
                    "home": self.home,
                    "hostname": self.hostname,
                    "ipc": self.ipc,
                    "keepPrivs": self.keepPrivs,
                    "locationsCacheSize": self.locationsCacheSize,
                    "locationsCacheTTL": self.locationsCacheTTL,
                    "memory": self.memory,
                    "memoryReservation": self.memoryReservation,
                    "memorySwap": self.memorySwap,
                    "mount": self.mount,
                    "net": self.net if network_access else True,
                    "network": self.network if network_access else "none",
                    "networkArgs": self.networkArgs,
                    "noEval": self.noEval,
                    "noHome": self.noHome,
                    "noHttps": self.noHttps,
                    "noInit": self.noInit,
                    "noMount": self.noMount,
                    "noPrivs": self.noPrivs,
                    "noUmask": self.noUmask,
                    "nv": self.nv,
                    "nvccli": self.nvccli,
                    "oomKillDisable": self.oomKillDisable,
                    "overlay": self.overlay,
                    "pemPath": self.pemPath,
                    "pidFile": self.pidFile,
                    "pidsLimit": self.pidsLimit,
                    "replicas": self.replicas,
                    "rocm": self.rocm,
                    "scratch": self.scratch,
                    "security": self.security,
                    "transferBufferSize": self.transferBufferSize,
                    "userns": self.userns,
                    "uts": self.uts,
                    "writable": self.writable,
                    "writableTmpfs": self.writableTmpfs,
                },
                workdir="/tmp/streamflow",  # nosec
                wraps=target if self.wrapper else None,
            ),
            service=image,
        )
