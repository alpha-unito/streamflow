import asyncio
import base64
import os
from abc import ABC, abstractmethod
from asyncio import Lock
from asyncio.subprocess import STDOUT
from functools import partial
from typing import Any, MutableMapping, MutableSequence, Optional, Tuple, Union

import cachetools
import pkg_resources
from cachetools import Cache, TTLCache

from streamflow.core import utils
from streamflow.core.asyncache import cachedmethod
from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import WorkflowDefinitionException
from streamflow.core.scheduling import Location
from streamflow.deployment.connector.ssh import SSHConnector
from streamflow.log_handler import logger


class QueueManagerConnector(SSHConnector, ABC):

    def __init__(self,
                 deployment_name: str,
                 context: StreamFlowContext,
                 hostname: str,
                 username: str,
                 checkHostKey: bool = True,
                 dataTransferConnection: Optional[Union[str, MutableMapping[str, Any]]] = None,
                 file: Optional[str] = None,
                 maxConcurrentJobs: Optional[int] = 1,
                 maxConcurrentSessions: Optional[int] = 10,
                 passwordFile: Optional[str] = None,
                 pollingInterval: int = 5,
                 services: Optional[MutableMapping[str, str]] = None,
                 sshKey: Optional[str] = None,
                 sshKeyPassphraseFile: Optional[str] = None,
                 transferBufferSize: int = 2 ** 16) -> None:
        super().__init__(
            deployment_name=deployment_name,
            context=context,
            checkHostKey=checkHostKey,
            dataTransferConnection=dataTransferConnection,
            file=file,
            maxConcurrentSessions=maxConcurrentSessions,
            nodes=[hostname],
            passwordFile=passwordFile,
            services=services,
            sshKey=sshKey,
            sshKeyPassphraseFile=sshKeyPassphraseFile,
            transferBufferSize=transferBufferSize,
            username=username)
        self.hostname: str = hostname
        self.maxConcurrentJobs: int = maxConcurrentJobs
        self.pollingInterval: int = pollingInterval
        self.scheduledJobs: MutableSequence[str] = []
        self.jobsCache: Cache = TTLCache(maxsize=1, ttl=self.pollingInterval)
        self.jobsCacheLock: Lock = Lock()

    @abstractmethod
    async def _get_output(self,
                          job_id: str,
                          location: str) -> str:
        ...

    @abstractmethod
    async def _get_returncode(self,
                              job_id: str,
                              location: str) -> int:
        ...

    @abstractmethod
    async def _get_running_jobs(self,
                                location: str) -> bool:
        ...

    @abstractmethod
    async def _remove_jobs(self,
                           location: str) -> None:
        ...

    @abstractmethod
    async def _run_batch_command(self,
                                 command: str,
                                 job_name: str,
                                 location: str,
                                 workdir: Optional[str] = None,
                                 stdin: Optional[Union[int, str]] = None,
                                 stdout: Union[int, str] = asyncio.subprocess.STDOUT,
                                 stderr: Union[int, str] = asyncio.subprocess.STDOUT) -> str:
        ...

    async def get_available_locations(self,
                                      service: str,
                                      input_directory: Optional[str] = None,
                                      output_directory: Optional[str] = None,
                                      tmp_directory: Optional[str] = None) -> MutableMapping[str, Location]:
        if service is not None and service not in self.templates:
            raise WorkflowDefinitionException("Invalid service {} for deployment {}.".format(
                service, self.deployment_name))
        return {self.hostname: Location(
            name=self.hostname,
            hostname=self.hostname,
            slots=self.maxConcurrentJobs)}

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join('schemas', 'queue_manager.json'))

    async def run(self,
                  location: str,
                  command: MutableSequence[str],
                  environment: MutableMapping[str, str] = None,
                  workdir: Optional[str] = None,
                  stdin: Optional[Union[int, str]] = None,
                  stdout: Union[int, str] = asyncio.subprocess.STDOUT,
                  stderr: Union[int, str] = asyncio.subprocess.STDOUT,
                  capture_output: bool = False,
                  job_name: Optional[str] = None) -> Optional[Tuple[Optional[Any], int]]:
        if job_name:
            command = utils.create_command(
                command=command,
                environment=environment,
                workdir=workdir)
            logger.debug("Executing command {command} on {location} {job}".format(
                command=command,
                location=location,
                job="for job {job}".format(job=job_name) if job_name else ""))
            command = utils.encode_command(command)
            command = self._get_command_from_template(
                command=command,
                environment=environment,
                template=self.context.scheduler.get_service(job_name),
                workdir=workdir)
            job_id = await self._run_batch_command(
                command=command,
                job_name=job_name,
                location=location,
                workdir=workdir,
                stdin=stdin,
                stdout=stdout,
                stderr=stderr)
            logger.info("Scheduled job {job} with job id {job_id}".format(
                job=job_name,
                job_id=job_id))
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
                await self._get_output(job_id, location) if stdout == STDOUT else None,
                await self._get_returncode(job_id, location))
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
                capture_output=capture_output)

    async def undeploy(self, external: bool) -> None:
        await self._remove_jobs(self.hostname)
        self.scheduledJobs = {}
        await super().undeploy(external)


class SlurmConnector(QueueManagerConnector):

    async def _get_output(self,
                          job_id: str,
                          location: str) -> str:
        async with self._get_ssh_client(location) as ssh_client:
            output_path = (await ssh_client.run(
                "scontrol show -o job {job_id} | "
                "sed -n 's/^.*StdOut=\\([^[:space:]]*\\).*/\\1/p'".format(
                    job_id=job_id
                ))).stdout.strip()
            return ((await ssh_client.run('cat {output}'.format(output=output_path))).stdout.strip()
                    if output_path else "")

    async def _get_returncode(self,
                              job_id: str,
                              location: str) -> int:
        async with self._get_ssh_client(location) as ssh_client:
            return int((await ssh_client.run(
                "scontrol show -o job {job_id} | "
                "sed -n 's/^.*ExitCode=\\([0-9]\\+\\):.*/\\1/p'".format(
                    job_id=job_id
                ))).stdout.strip())

    @cachedmethod(lambda self: self.jobsCache, key=partial(cachetools.keys.hashkey, 'running_jobs'))
    async def _get_running_jobs(self,
                                location: str) -> MutableSequence[str]:
        async with self._get_ssh_client(location) as ssh_client:
            return [j.strip() for j in (await ssh_client.run(
                "squeue -h -j {job_ids} -t {states} -O JOBID".format(
                    job_ids=",".join(self.scheduledJobs),
                    states=",".join(['PENDING', 'RUNNING', 'SUSPENDED', 'COMPLETING', 'CONFIGURING',
                                     'RESIZING', 'REVOKED', 'SPECIAL_EXIT'])
                ))).stdout.strip().splitlines()]

    async def _remove_jobs(self,
                           location: str) -> None:
        async with self._get_ssh_client(location) as ssh_client:
            await ssh_client.run(
                "scancel {job_ids}".format(job_ids=" ".join(self.scheduledJobs)))

    async def _run_batch_command(self,
                                 command: str,
                                 job_name: str,
                                 location: str,
                                 workdir: Optional[str] = None,
                                 stdin: Optional[Union[int, str]] = None,
                                 stdout: Union[int, str] = asyncio.subprocess.STDOUT,
                                 stderr: Union[int, str] = asyncio.subprocess.STDOUT) -> str:
        batch_command = "sh -c 'echo {command} | " \
                        "base64 -d | " \
                        "sbatch --parsable {workdir} {stdin} {stdout} {stderr}'".format(
                            workdir=("-D {workdir}".format(workdir=workdir)
                                     if workdir is not None else ""),
                            stdin=("-i \"{stdin}\"".format(stdin=stdin)
                                   if stdin is not None else ""),
                            stdout=("-o \"{stdout}\"".format(stdout=stdout)
                                    if stdout != STDOUT else ""),
                            stderr=("-e \"{stderr}\"".format(stderr=stderr)
                                    if stderr != STDOUT and stderr != stdout else ""),
                            command=base64.b64encode(command.encode('utf-8')).decode('utf-8'))
        async with self._get_ssh_client(location) as ssh_client:
            result = await ssh_client.run(batch_command)
        return result.stdout.strip()


class PBSConnector(QueueManagerConnector):

    async def _get_output(self,
                          job_id: str,
                          location: str) -> str:
        async with self._get_ssh_client(location) as ssh_client:
            output_path = (await ssh_client.run(
                "qstat {job_id} -xf | "
                "sed -n 's/^\\s*Output_Path\\s=\\s.*:\\(.*\\)\\s*$/\\1/p'".format(
                    job_id=job_id
                ))).stdout.strip()
            return ((await ssh_client.run('cat {output}'.format(output=output_path))).stdout.strip()
                    if output_path else "")

    async def _get_returncode(self,
                              job_id: str,
                              location: str) -> int:
        async with self._get_ssh_client(location) as ssh_client:
            return int((await ssh_client.run(
                "qstat {job_id} -xf | "
                "sed -n 's/^\\s*Exit_status\\s=\\s\\([0-9]\\+\\)\\s*$/\\1/p'".format(
                    job_id=job_id
                ))).stdout.strip())

    @cachedmethod(lambda self: self.jobsCache, key=partial(cachetools.keys.hashkey, 'running_jobs'))
    async def _get_running_jobs(self,
                                location: str) -> MutableSequence[str]:
        async with self._get_ssh_client(location) as ssh_client:
            return (await ssh_client.run(
                "qstat -awx {job_ids} | "
                "grep '{grep_ids}' | "
                "awk '{{if($10 != \"E\" && $10 != \"F\") {{print $1}}}}'".format(
                    job_ids=" ".join(self.scheduledJobs),
                    grep_ids="\\|".join(self.scheduledJobs)
                ))).stdout.strip().splitlines()

    async def _remove_jobs(self,
                           location: str) -> None:
        async with self._get_ssh_client(location) as ssh_client:
            await ssh_client.run(
                "qdel {job_ids}".format(job_ids=" ".join(self.scheduledJobs)))

    async def _run_batch_command(self,
                                 command: str,
                                 job_name: str,
                                 location: str,
                                 workdir: Optional[str] = None,
                                 stdin: Optional[Union[int, str]] = None,
                                 stdout: Union[int, str] = asyncio.subprocess.STDOUT,
                                 stderr: Union[int, str] = asyncio.subprocess.STDOUT) -> str:
        batch_command = "sh -c '{workdir} echo {command} | base64 -d | qsub {stdin} {stdout} {stderr} -'".format(
            workdir=("cd {workdir} &&".format(workdir=workdir)
                     if workdir is not None else ""),
            stdin=("-i \"{stdin}\"".format(stdin=stdin)
                   if stdin is not None else ""),
            stdout="-o \"{stdout}\"".format(stdout=stdout if stdout != STDOUT else utils.random_name()),
            stderr=("-e \"{stderr}\"".format(stderr=stderr)
                    if stderr != STDOUT and stderr != stdout else ""),
            command=base64.b64encode(command.encode('utf-8')).decode('utf-8'))
        async with self._get_ssh_client(location) as ssh_client:
            result = await ssh_client.run(batch_command)
        return result.stdout.strip()
