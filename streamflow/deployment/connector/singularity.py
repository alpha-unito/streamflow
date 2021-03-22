import asyncio
import io
import json
import os
import posixpath
import shlex
import tarfile
import tempfile
from abc import ABC
from pathlib import Path
from typing import MutableSequence, Optional, Union, MutableMapping, Tuple, Any, cast

from typing_extensions import Text

from streamflow.core import utils
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.scheduling import Resource
from streamflow.deployment.connector.base import BaseConnector
from streamflow.log_handler import logger


async def _get_resource(resource_name: Text) -> Resource:
    inspect_command = "singularity instance list --json"
    proc = await asyncio.create_subprocess_exec(
        *shlex.split(inspect_command),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)
    stdout, _ = await proc.communicate()
    json_out = json.loads(stdout)
    for instance in json_out["instances"]:
        if instance["instance"] == resource_name:
            return Resource(name=resource_name, hostname=instance["ip"])


class SingularityBaseConnector(BaseConnector, ABC):

    def __init__(self,
                 streamflow_config_dir: Text,
                 transferBufferSize: int = 2 ** 16):
        super().__init__(streamflow_config_dir)
        self.transferBufferSize: int = transferBufferSize

    async def _copy_local_to_remote(self,
                                    src: Text,
                                    dst: Text,
                                    resources: MutableSequence[Text]) -> None:
        with tempfile.TemporaryFile() as tar_buffer:
            with tarfile.open(fileobj=tar_buffer, mode='w') as tar:
                tar.add(src, arcname=dst)
            tar_buffer.seek(0)
            await asyncio.gather(*[asyncio.create_task(
                self._copy_local_to_remote_single(resource, cast(io.BufferedRandom, tar_buffer))
            ) for resource in resources])

    async def _copy_local_to_remote_single(self,
                                           resource: Text,
                                           tar_buffer: io.BufferedRandom) -> None:
        resource_buffer = io.BufferedReader(tar_buffer.raw)
        command = "".join([
            "singularity "
            "exec ",
            "instance://{service} "
            "tar "
            "xf "
            "- "
            "-C "
            "/"
        ]).format(
            service=resource
        )
        proc = await asyncio.create_subprocess_exec(
            *shlex.split(command),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL)
        while content := resource_buffer.read(self.transferBufferSize):
            proc.stdin.write(content)
            await proc.stdin.drain()
        proc.stdin.close()
        await proc.wait()

    async def _copy_remote_to_local(self,
                                    src: Text,
                                    dst: Text,
                                    resource: Text) -> None:
        command = "".join([
            "singularity "
            "exec ",
            "instance://{service} "
            "tar "
            "cPf "
            "- "
            "{src}"
        ]).format(
            service=resource,
            src=src
        )
        proc = await asyncio.create_subprocess_exec(
            *shlex.split(command),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        with io.BytesIO() as byte_buffer:
            while data := await proc.stdout.read(self.transferBufferSize):
                byte_buffer.write(data)
            await proc.wait()
            utils.create_tar_from_byte_stream(byte_buffer, src, dst)

    async def run(self,
                  resource: Text,
                  command: MutableSequence[Text],
                  environment: MutableMapping[Text, Text] = None,
                  workdir: Optional[Text] = None,
                  stdin: Optional[Union[int, Text]] = None,
                  stdout: Union[int, Text] = asyncio.subprocess.STDOUT,
                  stderr: Union[int, Text] = asyncio.subprocess.STDOUT,
                  capture_output: bool = False,
                  job_name: Optional[Text] = None) -> Optional[Tuple[Optional[Any], int]]:
        encoded_command = self.create_encoded_command(
            command, resource, environment, workdir, stdin, stdout, stderr)
        run_command = "".join([
            "singularity "
            "exec ",
            "instance://{service} "
            "sh -c '{command}'"
        ]).format(
            service=resource,
            command=encoded_command
        )
        proc = await asyncio.create_subprocess_exec(
            *shlex.split(run_command),
            stdout=asyncio.subprocess.PIPE if capture_output else None,
            stderr=asyncio.subprocess.PIPE if capture_output else None)
        stdout, stderr = await proc.communicate()
        if capture_output:
            return stdout.decode().strip(), proc.returncode


class SingularityConnector(SingularityBaseConnector):

    def __init__(self,
                 streamflow_config_dir: Text,
                 image: Text,
                 transferBufferSize: Optional[int] = 2 ** 16,
                 addCaps: Optional[Text] = None,
                 allowSetuid: bool = False,
                 applyCgroups: Optional[Text] = None,
                 bind: Optional[MutableSequence[Text]] = None,
                 boot: bool = False,
                 cleanenv: bool = False,
                 contain: bool = False,
                 containall: bool = False,
                 disableCache: bool = False,
                 dns: Optional[Text] = None,
                 dropCaps: Optional[Text] = None,
                 env: Optional[MutableSequence[Text]] = None,
                 envFile: Optional[Text] = None,
                 fakeroot: bool = False,
                 fusemount: Optional[MutableSequence[Text]] = None,
                 home: Optional[Text] = None,
                 hostname: Optional[Text] = None,
                 instanceNames: Optional[MutableSequence[Text]] = None,
                 keepPrivs: bool = False,
                 net: bool = False,
                 network: Optional[Text] = None,
                 networkArgs: Optional[MutableSequence[Text]] = None,
                 noHome: bool = False,
                 noInit: bool = False,
                 noMount: Optional[MutableSequence[Text]] = None,
                 noPrivs: bool = False,
                 noUmask: bool = False,
                 nohttps: bool = False,
                 nv: bool = False,
                 overlay: Optional[MutableSequence[Text]] = None,
                 pemPath: Optional[Text] = None,
                 pidFile: Optional[Text] = None,
                 replicas: int = 1,
                 rocm: bool = False,
                 scratch: Optional[MutableSequence[Text]] = None,
                 security: Optional[MutableSequence[Text]] = None,
                 userns: bool = False,
                 uts: bool = False,
                 workdir: Optional[Text] = None,
                 writable: bool = False,
                 writableTmpfs: bool = False):
        super().__init__(streamflow_config_dir, transferBufferSize)
        self.image: Text = image
        self.addCaps: Optional[Text] = addCaps
        self.allowSetuid: bool = allowSetuid
        self.applyCgroups: Optional[Text] = applyCgroups
        self.bind: Optional[MutableSequence[Text]] = bind
        self.boot: bool = boot
        self.cleanenv: bool = cleanenv
        self.contain: bool = contain
        self.containall: bool = containall
        self.disableCache: bool = disableCache
        self.dns: Optional[Text] = dns
        self.dropCaps: Optional[Text] = dropCaps
        self.env: Optional[MutableSequence[Text]] = env
        self.envFile: Optional[Text] = envFile
        self.fakeroot: bool = fakeroot
        self.fusemount: Optional[MutableSequence[Text]] = fusemount
        self.home: Optional[Text] = home
        self.hostname: Optional[Text] = hostname
        self.instanceNames: MutableSequence[Text] = instanceNames or []
        self.keepPrivs: bool = keepPrivs
        self.net: bool = net
        self.network: Optional[Text] = network
        self.networkArgs: Optional[MutableSequence[Text]] = networkArgs
        self.noHome: bool = noHome
        self.noInit: bool = noInit
        self.noMount: Optional[MutableSequence[Text]] = noMount
        self.noPrivs: bool = noPrivs
        self.noUmask: bool = noUmask
        self.nohttps: bool = nohttps
        self.nv: bool = nv
        self.overlay: Optional[MutableSequence[Text]] = overlay
        self.pemPath: Optional[Text] = pemPath
        self.pidFile: Optional[Text] = pidFile
        self.replicas: int = replicas
        self.rocm: bool = rocm
        self.scratch: Optional[MutableSequence[Text]] = scratch
        self.security: Optional[MutableSequence[Text]] = security
        self.userns: bool = userns
        self.uts: bool = uts
        self.workdir: Optional[Text] = workdir
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

    async def get_available_resources(self, service: Text) -> MutableMapping[Text, Resource]:
        return {instance_name: await _get_resource(instance_name) for instance_name in self.instanceNames}

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
