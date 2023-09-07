from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import shlex
from abc import ABC, abstractmethod
from functools import partial
from typing import Any, MutableMapping, MutableSequence, cast

import cachetools
import pkg_resources

from streamflow.core import utils
from streamflow.core.asyncache import cachedmethod
from streamflow.core.deployment import Connector, Location
from streamflow.core.exception import (
    WorkflowDefinitionException,
    WorkflowExecutionException,
)
from streamflow.core.scheduling import AvailableLocation
from streamflow.core.utils import get_option
from streamflow.deployment.connector.ssh import SSHConnector
from streamflow.deployment.template import CommandTemplateMap
from streamflow.deployment.wrapper import ConnectorWrapper
from streamflow.log_handler import logger


class QueueManagerService:
    def __init__(self, file: str | None = None):
        self.file: str | None = file


class SlurmService(QueueManagerService):
    def __init__(
        self,
        file: str | None = None,
        account: str | None = None,
        acctgFreq: str | None = None,
        array: str | None = None,
        batch: str | None = None,
        bb: str | None = None,
        bbf: str | None = None,
        begin: str | None = None,
        clusterConstraint: str | None = None,
        clusters: str | None = None,
        constraint: str | None = None,
        container: str | None = None,
        containerId: str | None = None,
        contiguous: bool = False,
        coreSpec: int | None = None,
        coresPerSocket: int | None = None,
        cpuFreq: str | None = None,
        cpusPerGpu: int | None = None,
        cpusPerTask: int | None = None,
        deadline: str | None = None,
        delayBoot: str | None = None,
        exclude: str | None = None,
        exclusive: bool | str = False,
        export: str | None = None,
        exportFile: int | str | None = None,
        extraNodeInfo: str | None = None,
        getUserEnv: bool | str = False,
        gid: int | str | None = None,
        gpuBind: str | None = None,
        gpuFreq: str | None = None,
        gpus: str | None = None,
        gpusPerNode: str | None = None,
        gpusPerSocket: str | None = None,
        gpusPerTask: str | None = None,
        gres: str | None = None,
        gresFlags: str | None = None,
        hint: str | None = None,
        ignorePBS: bool = False,
        jobName: str | None = None,
        licenses: str | None = None,
        mailType: str | None = None,
        mailUser: str | None = None,
        mcsLabel: str | None = None,
        mem: str | None = None,
        memBind: str | None = None,
        memPerCpu: str | None = None,
        memPerGpu: str | None = None,
        mincpus: int | None = None,
        network: str | None = None,
        nice: int | None = None,
        noKill: bool = False,
        noRequeue: bool = False,
        nodefile: str | None = None,
        nodelist: str | None = None,
        nodes: str | None = None,
        ntasks: int | None = None,
        ntasksPerCore: int | None = None,
        ntasksPerGpu: int | None = None,
        ntasksPerNode: int | None = None,
        ntasksPerSocket: int | None = None,
        openMode: str | None = None,
        overcommit: bool = False,
        oversubscribe: bool = False,
        partition: str | None = None,
        power: str | None = None,
        prefer: str | None = None,
        priority: str | None = None,
        profile: str | None = None,
        propagate: str | None = None,
        qos: str | None = None,
        reboot: bool = False,
        requeue: bool = False,
        reservation: str | None = None,
        signal: str | None = None,
        socketsPerNode: int | None = None,
        spreadJob: bool = False,
        switches: str | None = None,
        threadSpec: int | None = None,
        threadsPerCore: int | None = None,
        time: str | None = None,
        timeMin: str | None = None,
        tmp: int | None = None,
        tresPerTask: str | None = None,
        uid: int | str | None = None,
        useMinNodes: bool = False,
        waitAllNodes: bool = False,
        wckey: str | None = None,
    ):
        super().__init__(file)
        self.account: str | None = account
        self.acctgFreq: str | None = acctgFreq
        self.array: str | None = array
        self.batch: str | None = batch
        self.bb: str | None = bb
        self.bbf: str | None = bbf
        self.begin: str | None = begin
        self.clusterConstraint: str | None = clusterConstraint
        self.clusters: str | None = clusters
        self.constraint: str | None = constraint
        self.container: str | None = container
        self.containerId: str | None = containerId
        self.contiguous: bool = contiguous
        self.coreSpec: int | None = coreSpec
        self.coresPerSocket: int | None = coresPerSocket
        self.cpuFreq: str | None = cpuFreq
        self.cpusPerGpu: int | None = cpusPerGpu
        self.cpusPerTask: int | None = cpusPerTask
        self.deadline: str | None = deadline
        self.delayBoot: str | None = delayBoot
        self.exclude: str | None = exclude
        self.exclusive: bool | str = exclusive
        self.export: str | None = export
        self.exportFile: int | str | None = exportFile
        self.extraNodeInfo: str | None = extraNodeInfo
        self.getUserEnv: bool | str = getUserEnv
        self.gid: int | str | None = gid
        self.gpuBind: str | None = gpuBind
        self.gpuFreq: str | None = gpuFreq
        self.gpus: str | None = gpus
        self.gpusPerNode: str | None = gpusPerNode
        self.gpusPerSocket: str | None = gpusPerSocket
        self.gpusPerTask: str | None = gpusPerTask
        self.gres: str | None = gres
        self.gresFlags: str | None = gresFlags
        self.hint: str | None = hint
        self.ignorePBS: bool = ignorePBS
        self.jobName: str | None = jobName
        self.licenses: str | None = licenses
        self.mailType: str | None = mailType
        self.mailUser: str | None = mailUser
        self.mcsLabel: str | None = mcsLabel
        self.mem: str | None = mem
        self.memBind: str | None = memBind
        self.memPerCpu: str | None = memPerCpu
        self.memPerGpu: str | None = memPerGpu
        self.mincpus: int | None = mincpus
        self.network: str | None = network
        self.nice: int | None = nice
        self.noKill: bool = noKill
        self.noRequeue: bool = noRequeue
        self.nodefile: str | None = nodefile
        self.nodelist: str | None = nodelist
        self.nodes: str | None = nodes
        self.ntasks: int | None = ntasks
        self.ntasksPerCore: int | None = ntasksPerCore
        self.ntasksPerGpu: int | None = ntasksPerGpu
        self.ntasksPerNode: int | None = ntasksPerNode
        self.ntasksPerSocket: int | None = ntasksPerSocket
        self.openMode: str | None = openMode
        self.overcommit: bool = overcommit
        self.oversubscribe: bool = oversubscribe
        self.partition: str | None = partition
        self.power: str | None = power
        self.prefer: str | None = prefer
        self.priority: str | None = priority
        self.profile: str | None = profile
        self.propagate: str | None = propagate
        self.qos: str | None = qos
        self.reboot: bool = reboot
        self.requeue: bool = requeue
        self.reservation: str | None = reservation
        self.signal: str | None = signal
        self.socketsPerNode: int | None = socketsPerNode
        self.spreadJob: bool = spreadJob
        self.switches: str | None = switches
        self.threadSpec: int | None = threadSpec
        self.threadsPerCore: int | None = threadsPerCore
        self.time: str | None = time
        self.timeMin: str | None = timeMin
        self.tmp: int | None = tmp
        self.tresPerTask: str | None = tresPerTask
        self.uid: int | str | None = uid
        self.useMinNodes: bool = useMinNodes
        self.waitAllNodes: bool = waitAllNodes
        self.wckey: str | None = wckey


class PBSService(QueueManagerService):
    def __init__(
        self,
        file: str | None = None,
        account: str | None = None,
        additionalAttributes: MutableMapping[str, str] | None = None,
        begin: str | None = None,
        checkpoint: str | None = None,
        destination: str | None = None,
        exportAllVariables: bool = False,
        jobName: str | None = None,
        mailOptions: str | None = None,
        prefix: str | None = None,
        priority: int | None = None,
        rerunnable: bool = True,
        resources: MutableMapping[str, str] | None = None,
        shellList: str | None = None,
        userList: str | None = None,
        variableList: str | None = None,
    ):
        super().__init__(file)
        self.account: str | None = account
        self.additionalAttributes: MutableMapping[str, str] = additionalAttributes or {}
        self.begin: str | None = begin
        self.checkpoint: str | None = checkpoint
        self.destination: str | None = destination
        self.exportAllVariables: bool = exportAllVariables
        self.jobName: str | None = jobName
        self.mailOptions: str | None = mailOptions
        self.prefix: str | None = prefix
        self.priority: int | None = priority
        self.rerunnable: bool = rerunnable
        self.resources: MutableMapping[str, str] = resources or {}
        self.shellList: str | None = shellList
        self.userList: str | None = userList
        self.variableList: str | None = variableList


class FluxService(QueueManagerService):
    def __init__(
        self,
        file: str | None = None,
        beginTime: str | None = None,
        brokerOpts: MutableSequence[str] | None = None,
        cores: int | None = None,
        coresPerSlot: int | None = None,
        coresPerTask: int | None = None,
        env: MutableSequence[str] | None = None,
        envFile: MutableSequence[str] | None = None,
        envRemove: MutableSequence[str] | None = None,
        exclusive: bool = False,
        flags: str | None = None,
        gpusPerNode: int | None = None,
        gpusPerSlot: int | None = None,
        gpusPerTask: int | None = None,
        jobName: str | None = None,
        labelIO: bool = False,
        nodes: int | None = None,
        nslots: int | None = None,
        ntasks: int | None = None,
        queue: str | None = None,
        requires: str | None = None,
        rlimit: MutableSequence[str] | None = None,
        setattr: MutableMapping[str, str] | None = None,
        setopt: MutableMapping[str, str] | None = None,
        taskmap: str | None = None,
        tasksPerCore: int | None = None,
        tasksPerNode: int | None = None,
        timeLimit: str | None = None,
        unbuffered: bool = False,
        urgency: int | None = None,
    ):
        super().__init__(file)
        self.beginTime: str | None = beginTime
        self.brokerOpts: MutableSequence[str] | None = brokerOpts or []
        self.cores: int | None = cores
        self.coresPerSlot: int | None = coresPerSlot
        self.coresPerTask: int | None = coresPerTask
        self.env: MutableSequence[str] | None = env or []
        self.envFile: MutableSequence[str] | None = envFile or []
        self.envRemove: MutableSequence[str] | None = envRemove or []
        self.exclusive: bool = exclusive
        self.flags: str | None = flags
        self.gpusPerNode: int | None = gpusPerNode
        self.gpusPerSlot: int | None = gpusPerSlot
        self.gpusPerTask: int | None = gpusPerTask
        self.jobName: str | None = jobName
        self.labelIO: bool = labelIO
        self.nodes: int | None = nodes
        self.nslots: int | None = nslots
        self.ntasks: int | None = ntasks
        self.queue: str | None = queue
        self.requires: str | None = requires
        self.rlimit: MutableSequence[str] | None = rlimit or []
        self.setattr: MutableMapping[str, str] | None = setattr or {}
        self.setopt: MutableMapping[str, str] | None = setopt or {}
        self.taskmap: str | None = taskmap
        self.tasksPerCore: int | None = tasksPerCore
        self.tasksPerNode: int | None = tasksPerNode
        self.timeLimit: str | None = timeLimit
        self.unbuffered: bool = unbuffered
        self.urgency: int | None = urgency


class QueueManagerConnector(ConnectorWrapper, ABC):
    def __init__(
        self,
        deployment_name: str,
        config_dir: str,
        connector: Connector,
        hostname: str | None = None,
        username: str | None = None,
        checkHostKey: bool = True,
        dataTransferConnection: str | MutableMapping[str, Any] | None = None,
        file: str | None = None,
        maxConcurrentJobs: int | None = 1,
        maxConcurrentSessions: int | None = 10,
        maxConnections: int | None = 1,
        passwordFile: str | None = None,
        pollingInterval: int = 5,
        services: MutableMapping[str, QueueManagerService] | None = None,
        sshKey: str | None = None,
        sshKeyPassphraseFile: str | None = None,
        transferBufferSize: int = 2**16,
    ) -> None:
        self._inner_ssh_connector: bool = False
        if hostname is not None:
            if logger.isEnabledFor(logging.WARN):
                logger.warn(
                    "Inline SSH options are deprecated and will be removed in StreamFlow 0.3.0. "
                    f"Define a standalone `SSHConnector` and link the `{self.__class__.__name__}` "
                    "to it using the `wraps` property."
                )
            self._inner_ssh_connector = True
            connector: Connector = SSHConnector(
                deployment_name=f"{deployment_name}-ssh",
                config_dir=config_dir,
                checkHostKey=checkHostKey,
                dataTransferConnection=dataTransferConnection,
                maxConcurrentSessions=maxConcurrentSessions,
                maxConnections=maxConnections,
                nodes=[hostname],
                passwordFile=passwordFile,
                sshKey=sshKey,
                sshKeyPassphraseFile=sshKeyPassphraseFile,
                transferBufferSize=transferBufferSize,
                username=username,
            )
        super().__init__(deployment_name, config_dir, connector)
        files_map: MutableMapping[str, Any] = {}
        self.services = (
            {k: self._service_class(**v) for k, v in services.items()}
            if services
            else {}
        )
        if self.services:
            for name, service in self.services.items():
                if service.file is not None:
                    with open(os.path.join(self.config_dir, service.file)) as f:
                        files_map[name] = f.read()
        if file is not None:
            if logger.isEnabledFor(logging.WARN):
                logger.warn(
                    "The `file` keyword is deprecated and will be removed in StreamFlow 0.3.0. "
                    "Use `services` instead."
                )
            with open(os.path.join(self.config_dir, file)) as f:
                self.template_map: CommandTemplateMap = CommandTemplateMap(
                    default=f.read(), template_map=files_map
                )
        else:
            self.template_map: CommandTemplateMap = CommandTemplateMap(
                default="#!/bin/sh\n\n{{streamflow_command}}",
                template_map=files_map,
            )
        self.maxConcurrentJobs: int = maxConcurrentJobs
        self.pollingInterval: int = pollingInterval
        self.scheduledJobs: MutableSequence[str] = []
        self.jobsCache: cachetools.Cache = cachetools.TTLCache(
            maxsize=1, ttl=self.pollingInterval
        )
        self.jobsCacheLock: asyncio.Lock = asyncio.Lock()

    def _format_stream(self, stream: int | str) -> str:
        if stream == asyncio.subprocess.DEVNULL:
            stream = "/dev/null"
        elif stream == asyncio.subprocess.PIPE:
            raise WorkflowExecutionException(
                f"The `{self.__class__.__name__}` does not support stream pipe redirection."
            )
        return shlex.quote(stream)

    async def _get_location(self):
        locations = await self.connector.get_available_locations()
        if len(locations) != 1:
            raise WorkflowDefinitionException(
                f"QueueManager connectors support only nested connectors with a single location. "
                f"{self.connector.deployment_name} returned {len(locations)} available locations."
            )
        return list(locations.values())[0]

    @abstractmethod
    async def _get_output(self, job_id: str, location: Location) -> str:
        ...

    @abstractmethod
    async def _get_returncode(self, job_id: str, location: Location) -> int:
        ...

    @abstractmethod
    async def _get_running_jobs(self, location: Location) -> bool:
        ...

    @abstractmethod
    async def _remove_jobs(self, location: Location) -> None:
        ...

    @abstractmethod
    async def _run_batch_command(
        self,
        command: str,
        job_name: str,
        location: Location,
        workdir: str | None = None,
        stdin: int | str | None = None,
        stdout: int | str = asyncio.subprocess.STDOUT,
        stderr: int | str = asyncio.subprocess.STDOUT,
        timeout: int | None = None,
    ) -> str:
        ...

    @property
    @abstractmethod
    def _service_class(self) -> type[QueueManagerService]:
        ...

    async def get_available_locations(
        self,
        service: str | None = None,
        input_directory: str | None = None,
        output_directory: str | None = None,
        tmp_directory: str | None = None,
    ) -> MutableMapping[str, AvailableLocation]:
        if service is not None and service not in self.services:
            raise WorkflowDefinitionException(
                f"Invalid service {service} for deployment {self.deployment_name}."
            )
        hostname = (await self._get_location()).hostname
        return {
            hostname: AvailableLocation(
                name=hostname,
                deployment=self.deployment_name,
                service=service,
                hostname=hostname,
                slots=self.maxConcurrentJobs,
            )
        }

    async def run(
        self,
        location: Location,
        command: MutableSequence[str],
        environment: MutableMapping[str, str] = None,
        workdir: str | None = None,
        stdin: int | str | None = None,
        stdout: int | str = asyncio.subprocess.STDOUT,
        stderr: int | str = asyncio.subprocess.STDOUT,
        capture_output: bool = False,
        timeout: int | None = None,
        job_name: str | None = None,
    ) -> tuple[Any | None, int] | None:
        if job_name:
            command = utils.create_command(
                class_name=self.__class__.__name__,
                command=command,
                environment=environment,
                workdir=workdir,
            )
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "EXECUTING command {command} on {location} {job}".format(
                        command=command,
                        location=location,
                        job=f"for job {job_name}" if job_name else "",
                    )
                )
            command = utils.encode_command(command)
            command = self.template_map.get_command(
                command=command,
                template=location.service,
                environment=environment,
                workdir=workdir,
            )
            job_id = await self._run_batch_command(
                command=command,
                job_name=job_name,
                location=location,
                workdir=workdir,
                stdin=stdin,
                stdout=stdout,
                stderr=stderr,
                timeout=timeout,
            )
            if logger.isEnabledFor(logging.INFO):
                logger.info(f"Scheduled job {job_name} with job id {job_id}")
            self.scheduledJobs.append(job_id)
            async with self.jobsCacheLock:
                self.jobsCache.clear()
            while True:
                async with self.jobsCacheLock:
                    running_jobs = await self._get_running_jobs(location)
                if job_id not in running_jobs:
                    break
                await asyncio.sleep(self.pollingInterval)
            self.scheduledJobs.remove(job_id)
            return (
                await self._get_output(job_id, location)
                if stdout == asyncio.subprocess.STDOUT
                else None,
                await self._get_returncode(job_id, location),
            )
        else:
            return await super().run(
                location=location,
                command=command,
                environment=environment,
                workdir=workdir,
                stdin=stdin,
                stdout=stdout,
                stderr=stderr,
                job_name=job_name,
                timeout=timeout,
                capture_output=capture_output,
            )

    async def undeploy(self, external: bool) -> None:
        await self._remove_jobs(await self._get_location())
        self.scheduledJobs = {}
        if self._inner_ssh_connector:
            if logger.isEnabledFor(logging.INFO):
                logger.warn(
                    f"UNDEPLOYING inner SSH connector for {self.deployment_name} deployment."
                )
            await self.connector.undeploy(external)
            if logger.isEnabledFor(logging.INFO):
                logger.warn(
                    f"COMPLETED Undeployment of inner SSH connector for {self.deployment_name} deployment."
                )


class SlurmConnector(QueueManagerConnector):
    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "slurm.json")
        )

    async def _get_output(self, job_id: str, location: Location) -> str:
        command = [
            "scontrol",
            "show",
            "-o",
            "job",
            job_id,
            "|",
            "sed",
            "-n",
            "'s/^.*StdOut=\\([^[:space:]]*\\).*/\\1/p'",
        ]
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Running command {' '.join(command)}")
        stdout, _ = await self.connector.run(
            location=location,
            command=command,
            capture_output=True,
        )
        if output_path := stdout.strip():
            stdout, _ = await self.connector.run(
                location=location, command=["cat", output_path], capture_output=True
            )
            return stdout.strip()
        else:
            return ""

    async def _get_returncode(self, job_id: str, location: Location) -> int:
        command = [
            "scontrol",
            "show",
            "-o",
            "job",
            job_id,
            "|",
            "sed",
            "-n",
            "'s/^.*ExitCode=\\([0-9]\\+\\):.*/\\1/p'",
        ]
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Running command {' '.join(command)}")
        stdout, _ = await self.connector.run(
            location=location,
            command=command,
            capture_output=True,
        )
        return int(stdout.strip())

    @cachedmethod(
        lambda self: self.jobsCache,
        key=partial(cachetools.keys.hashkey, "running_jobs"),
    )
    async def _get_running_jobs(self, location: Location) -> MutableSequence[str]:
        command = [
            "squeue",
            "-h",
            "-j",
            ",".join(self.scheduledJobs),
            "-t",
            ",".join(
                [
                    "PENDING",
                    "RUNNING",
                    "SUSPENDED",
                    "COMPLETING",
                    "CONFIGURING",
                    "RESIZING",
                    "REVOKED",
                    "SPECIAL_EXIT",
                ]
            ),
            "-O",
            "JOBID",
        ]
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Running command {' '.join(command)}")
        stdout, _ = await self.connector.run(
            location=location,
            command=command,
            capture_output=True,
        )
        return [j.strip() for j in stdout.strip().splitlines()]

    @property
    def _service_class(self) -> type[QueueManagerService]:
        return SlurmService

    async def _remove_jobs(self, location: Location) -> None:
        await self.connector.run(
            location=location, command=["scancel", " ".join(self.scheduledJobs)]
        )

    async def _run_batch_command(
        self,
        command: str,
        job_name: str,
        location: Location,
        workdir: str | None = None,
        stdin: int | str | None = None,
        stdout: int | str = asyncio.subprocess.STDOUT,
        stderr: int | str = asyncio.subprocess.STDOUT,
        timeout: int | None = None,
    ) -> str:
        batch_command = [
            "echo",
            base64.b64encode(command.encode("utf-8")).decode("utf-8"),
            "|",
            "base64",
            "-d",
            "|",
            "sbatch",
            "--parsable",
        ]
        if stdin is not None and stdin != asyncio.subprocess.DEVNULL:
            batch_command.append(get_option("input", shlex.quote(stdin)))
        if stderr != asyncio.subprocess.STDOUT and stderr != stdout:
            batch_command.append(get_option("error", self._format_stream(stderr)))
        if stdout != asyncio.subprocess.STDOUT:
            batch_command.append(get_option("output", self._format_stream(stdout)))
        if timeout:
            batch_command.append(
                get_option("time", utils.format_seconds_to_hhmmss(timeout))
            )
        if service := cast(SlurmService, self.services.get(location.service)):
            batch_command.extend(
                [
                    get_option("account", service.account),
                    get_option("acctg-freq", service.acctgFreq),
                    get_option("array", service.array),
                    get_option("batch", service.batch),
                    get_option("bb", service.bb),
                    get_option("bbf", service.bbf),
                    get_option("begin", service.begin),
                    get_option("chdir", workdir),
                    get_option("cluster-constraint", service.clusterConstraint),
                    get_option("clusters", service.clusters),
                    get_option("constraint", service.constraint),
                    get_option("container", service.container),
                    get_option("container-id", service.containerId),
                    get_option("contiguous", service.contiguous),
                    get_option("core-spec", service.coreSpec),
                    get_option("cores-per-socket", service.coresPerSocket),
                    get_option("cpu-freq", service.cpuFreq),
                    get_option("cpus-per-gpu", service.cpusPerGpu),
                    get_option("cpus-per-task", service.cpusPerTask),
                    get_option("deadline", service.deadline),
                    get_option("delay-boot", service.delayBoot),
                    get_option("exclude", service.exclude),
                    get_option("exclusive", service.exclusive),
                    get_option("export", service.export),
                    get_option("export-file", service.exportFile),
                    get_option("extra-node-info", service.extraNodeInfo),
                    get_option("get-user-env", service.getUserEnv),
                    get_option("gid", service.gid),
                    get_option("gpu-bind", service.gpuBind),
                    get_option("gpu-freq", service.gpuFreq),
                    get_option("gpus", service.gpus),
                    get_option("gpus-per-node", service.gpusPerNode),
                    get_option("gpus-per-socket", service.gpusPerSocket),
                    get_option("gpus-per-task", service.gpusPerTask),
                    get_option("gres", service.gres),
                    get_option("gres-flags", service.gresFlags),
                    get_option("hint", service.hint),
                    get_option("ignore-pbs", service.ignorePBS),
                    get_option("job-name", service.jobName),
                    get_option("licenses", service.licenses),
                    get_option("mail-type", service.mailType),
                    get_option("mail-user", service.mailUser),
                    get_option("mcs-label", service.mcsLabel),
                    get_option("mem", service.mem),
                    get_option("mem-bind", service.memBind),
                    get_option("mem-per-cpu", service.memPerCpu),
                    get_option("mem-per-gpu", service.memPerGpu),
                    get_option("minpucs", service.mincpus),
                    get_option("network", service.network),
                    get_option("nice", service.nice),
                    get_option("no-kill", service.noKill),
                    get_option("no-requeue", service.noRequeue),
                    get_option("nodefile", service.nodefile),
                    get_option("nodelist", service.nodelist),
                    get_option("nodes", service.nodes),
                    get_option("ntasks", service.ntasks),
                    get_option("ntasks-per-core", service.ntasksPerCore),
                    get_option("ntasks-per-gpu", service.ntasksPerGpu),
                    get_option("ntasks-per-node", service.ntasksPerNode),
                    get_option("ntasks-per-socket", service.ntasksPerSocket),
                    get_option("open-mode", service.openMode),
                    get_option("overcommit", service.overcommit),
                    get_option("oversubscribe", service.oversubscribe),
                    get_option("partition", service.partition),
                    get_option("power", service.power),
                    get_option("prefer", service.prefer),
                    get_option("priority", service.priority),
                    get_option("profile", service.profile),
                    get_option("propagate", service.propagate),
                    get_option("qos", service.qos),
                    get_option("reboot", service.reboot),
                    get_option("requeue", service.requeue),
                    get_option("reservation", service.reservation),
                    get_option("signal", service.signal),
                    get_option("sockets-per-node", service.socketsPerNode),
                    get_option("spread-job", service.spreadJob),
                    get_option("switches", service.switches),
                    get_option("thread-spec", service.threadSpec),
                    get_option("threads-per-core", service.threadsPerCore),
                    get_option("time-min", service.timeMin),
                    get_option("tmp", service.tmp),
                    get_option("tres-per-task", service.tresPerTask),
                    get_option("uid", service.uid),
                    get_option("use-min_nodes", service.useMinNodes),
                    get_option("wait-all-nodes", 1 if service.waitAllNodes else 0),
                    get_option("wckey", service.wckey),
                ]
            )
            if not timeout:
                batch_command.append(get_option("time", service.time))
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Running command {' '.join(batch_command)}")
        stdout, returncode = await self.connector.run(
            location=location, command=batch_command, capture_output=True
        )
        if returncode == 0:
            return stdout.strip()
        else:
            raise WorkflowExecutionException(
                f"Error submitting job {job_name} to Slurm: {stdout.strip()}"
            )


class PBSConnector(QueueManagerConnector):
    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "pbs.json")
        )

    async def _get_output(self, job_id: str, location: Location) -> str:
        result = json.loads(await self._run_qstat_command(job_id, location))
        output_path = result["Jobs"][job_id]["Output_Path"]
        if ":" in output_path:
            output_path = "".join(output_path.split(":")[1:])
        if output_path:
            stdout, _ = await self.connector.run(
                location=location, command=["cat", output_path], capture_output=True
            )
            return stdout.strip()
        else:
            return ""

    async def _get_returncode(self, job_id: str, location: Location) -> int:
        result = json.loads(await self._run_qstat_command(job_id, location))
        return int(result["Jobs"][job_id]["Exit_status"])

    @cachedmethod(
        lambda self: self.jobsCache,
        key=partial(cachetools.keys.hashkey, "running_jobs"),
    )
    async def _get_running_jobs(self, location: Location) -> MutableSequence[str]:
        command = [
            "qstat",
            " ".join(self.scheduledJobs),
            "-xf",
            "-Fjson",
        ]
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Running command {command}")
        stdout, _ = await self.connector.run(
            location=location,
            command=command,
            capture_output=True,
        )
        result = json.loads(stdout.strip())
        return [
            j
            for j in self.scheduledJobs
            if j not in result["Jobs"]  # Job id has not been processed yet
            or result["Jobs"][j]["job_state"] not in ["E", "F"]  # Job finished
        ]

    async def _remove_jobs(self, location: Location) -> None:
        await self.connector.run(
            location=location, command=["qdel", " ".join(self.scheduledJobs)]
        )

    async def _run_batch_command(
        self,
        command: str,
        job_name: str,
        location: Location,
        workdir: str | None = None,
        stdin: int | str | None = None,
        stdout: int | str = asyncio.subprocess.STDOUT,
        stderr: int | str = asyncio.subprocess.STDOUT,
        timeout: int | None = None,
    ) -> str:
        batch_command = ["sh", "-c"]
        if workdir is not None:
            batch_command.extend(["cd", workdir, "&&"])
        resources = (
            {"walltime": utils.format_seconds_to_hhmmss(timeout)} if timeout else {}
        )
        batch_command.extend(
            [
                "echo",
                base64.b64encode(command.encode("utf-8")).decode("utf-8"),
                "|",
                "base64",
                "-d",
                "|",
                "qsub",
                get_option(
                    "o",
                    (
                        stdout
                        if stdout != asyncio.subprocess.STDOUT
                        else utils.random_name()
                    ),
                ),
            ]
        )
        if stdin is not None and stdin != asyncio.subprocess.DEVNULL:
            batch_command.append(get_option("i", stdin))
        if stderr != asyncio.subprocess.STDOUT and stderr != stdout:
            batch_command.append(get_option("e", self._format_stream(stderr)))
        if stderr == stdout:
            batch_command.append(get_option("j", "oe"))
        if service := cast(PBSService, self.services.get(location.service)):
            resources = {**service.resources, **resources}
            batch_command.extend(
                [
                    get_option("a", service.begin),
                    get_option("A", service.account),
                    get_option("c", service.checkpoint),
                    get_option("C", service.prefix),
                    get_option("m", service.mailOptions),
                    get_option("N", service.jobName),
                    get_option("p", service.priority),
                    get_option("q", service.destination),
                    get_option("r", "y" if service.rerunnable else "n"),
                    get_option("S", service.shellList),
                    get_option("u", service.userList),
                    get_option("v", service.variableList),
                    get_option("V", service.exportAllVariables),
                    get_option(
                        "W",
                        ",".join([f"{k}={v}" for k, v in service.additionalAttributes]),
                    ),
                ]
            )
        if resources:
            batch_command.append(
                get_option("l", ",".join([f"{k}={v}" for k, v in resources]))
            )
        batch_command.append("-")
        stdout, returncode = await self.connector.run(
            location=location, command=batch_command, capture_output=True
        )
        if returncode == 0:
            return stdout.strip()
        else:
            raise WorkflowExecutionException(
                f"Error submitting job {job_name} to PBS: {stdout.strip()}"
            )

    async def _run_qstat_command(self, job_id: str, location: Location) -> str:
        command = [
            "qstat",
            job_id,
            "-xf",
            "-Fjson",
        ]
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Running command {command}")
        stdout, _ = await self.connector.run(
            location=location,
            command=command,
            capture_output=True,
        )
        return stdout.strip()

    @property
    def _service_class(self) -> type[QueueManagerService]:
        return PBSService


class FluxConnector(QueueManagerConnector):
    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "flux.json")
        )

    async def _get_output(self, job_id: str, location: Location) -> str:
        # This will hang if the job is not complete
        command = [
            "flux",
            "job",
            "attach",
            job_id,
        ]
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Running command {' '.join(command)}")
        stdout, _ = await self.connector.run(
            location=location,
            command=command,
            capture_output=True,
        )
        if output_path := stdout.strip():
            stdout, _ = await self.connector.run(
                location=location, command=["cat", output_path], capture_output=True
            )
            return stdout.strip()
        else:
            return ""

    async def _get_returncode(self, job_id: str, location: Location) -> int:
        command = [
            "flux",
            "jobs",
            "--no-header",
            "-o",
            "{returncode}",
            job_id,
        ]
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Running command {' '.join(command)}")
        stdout, _ = await self.connector.run(
            location=location,
            command=command,
            capture_output=True,
        )
        return int(stdout.strip())

    @cachedmethod(
        lambda self: self.jobsCache,
        key=partial(cachetools.keys.hashkey, "running_jobs"),
    )
    async def _get_running_jobs(self, location: Location) -> MutableSequence[str]:
        # If we add the job id, the filter is ignored
        command = [
            "flux",
            "jobs",
            "--no-header",
            "--filter=pending,running",
            '-o "{id}"',
        ]
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Running command {' '.join(command)}")
        stdout, _ = await self.connector.run(
            location=location,
            command=command,
            capture_output=True,
        )
        # Filter down to the job ids we are interested in
        return [
            j.strip()
            for j in stdout.strip().splitlines()
            if j.strip() in self.scheduledJobs
        ]

    async def _remove_jobs(self, location: Location) -> None:
        await self.connector.run(
            location=location,
            command=["flux", "job", "cancel", " ".join(self.scheduledJobs)],
        )

    async def _run_batch_command(
        self,
        command: str,
        job_name: str,
        location: Location,
        workdir: str | None = None,
        stdin: int | str | None = None,
        stdout: int | str = asyncio.subprocess.STDOUT,
        stderr: int | str = asyncio.subprocess.STDOUT,
        timeout: int | None = None,
    ) -> str:
        batch_command = [
            "echo",
            base64.b64encode(command.encode("utf-8")).decode("utf-8"),
            "|",
            "base64",
            "-d",
            "|",
            "flux",
            "batch",
        ]
        if workdir is not None:
            batch_command.append(get_option("cwd", workdir))
        if stdin is not None and stdin != asyncio.subprocess.DEVNULL:
            batch_command.append(get_option("input", shlex.quote(stdin)))
        if stdout != asyncio.subprocess.STDOUT:
            batch_command.append(get_option("output", self._format_stream(stdout)))
        if stderr != asyncio.subprocess.STDOUT and stderr != stdout:
            batch_command.append(get_option("error", self._format_stream(stderr)))
        if timeout:
            batch_command.append(
                get_option("time-limit", utils.format_seconds_to_hhmmss(timeout))
            )
        nodes = 1
        if service := cast(FluxService, self.services.get(location.service)):
            nodes = service.nodes
            batch_command.extend(
                [
                    get_option("begin-time", service.beginTime),
                    get_option("broker-opts", service.brokerOpts),
                    get_option("cores", service.cores),
                    get_option("cores-per-slot", service.coresPerSlot),
                    get_option("cores-per-task", service.coresPerTask),
                    get_option("env", service.env),
                    get_option("env-file", service.envFile),
                    get_option("env-remove", service.envRemove),
                    get_option("exclusive", service.exclusive),
                    get_option("flags", service.flags),
                    get_option("gpus-per-node", service.gpusPerNode),
                    get_option("gpus-per-slot", service.gpusPerSlot),
                    get_option("gpus-per-task", service.gpusPerTask),
                    get_option("job-name", service.jobName),
                    get_option("label-io", service.labelIO),
                    get_option("nslots", service.nslots),
                    get_option("ntasks", service.ntasks),
                    get_option("queue", service.queue),
                    get_option("requires", service.requires),
                    get_option("rlimit", service.rlimit),
                    get_option(
                        "setattr", ",".join([f"{k}={v}" for k, v in service.setattr])
                    ),
                    get_option(
                        "setopt", ",".join([f"{k}={v}" for k, v in service.setopt])
                    ),
                    get_option("taskmap", service.taskmap),
                    get_option("tasks-per-core", service.tasksPerCore),
                    get_option("tasks-per-node", service.tasksPerNode),
                    get_option("unbuffered", service.unbuffered),
                    get_option("urgency", service.urgency),
                ]
            )
            if not timeout:
                batch_command.append(get_option("time-limit", service.timeLimit))
        batch_command.append(get_option("nodes", nodes))
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Running command {' '.join(batch_command)}")
        stdout, returncode = await self.connector.run(
            location=location, command=batch_command, capture_output=True
        )
        if returncode == 0:
            return stdout.strip()
        else:
            raise WorkflowExecutionException(
                f"Error submitting job {job_name} to Flux: {stdout.strip()}"
            )

    @property
    def _service_class(self) -> type[QueueManagerService]:
        return FluxService
