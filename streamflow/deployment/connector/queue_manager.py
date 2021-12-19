import asyncio
from abc import ABC, abstractmethod
from asyncio import Lock
from asyncio.subprocess import STDOUT
from functools import partial
from typing import Optional, MutableSequence, MutableMapping, Tuple, Any, Union

import cachetools
from cachetools import Cache, TTLCache

from streamflow.core import utils
from streamflow.core.asyncache import cachedmethod
from streamflow.core.scheduling import Resource
from streamflow.deployment.connector.ssh import SSHConnector
from streamflow.log_handler import logger


class QueueManagerConnector(SSHConnector, ABC):

    def __init__(self,
                 streamflow_config_dir: str,
                 file: str,
                 hostname: str,
                 username: str,
                 checkHostKey: bool = True,
                 dataTransferConnection: Optional[Union[str, MutableMapping[str, Any]]] = None,
                 maxConcurrentJobs: Optional[int] = 1,
                 passwordFile: Optional[str] = None,
                 pollingInterval: int = 5,
                 sshKey: Optional[str] = None,
                 sshKeyPassphraseFile: Optional[str] = None,
                 transferBufferSize: int = 2**16) -> None:
        super().__init__(
            streamflow_config_dir=streamflow_config_dir,
            checkHostKey=checkHostKey,
            dataTransferConnection=dataTransferConnection,
            file=file,
            nodes=[hostname],
            passwordFile=passwordFile,
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
                          resource: str) -> str:
        ...

    @abstractmethod
    async def _get_returncode(self,
                              job_id: str,
                              resource: str) -> int:
        ...

    @abstractmethod
    async def _get_running_jobs(self,
                                resource: str) -> bool:
        ...

    @abstractmethod
    async def _remove_jobs(self,
                           resource: str) -> None:
        ...

    @abstractmethod
    async def _run_batch_command(self,
                                 helper_file: str,
                                 job_name: str,
                                 resource: str,
                                 workdir: Optional[str] = None,
                                 stdin: Optional[Union[int, str]] = None,
                                 stdout: Union[int, str] = asyncio.subprocess.STDOUT,
                                 stderr: Union[int, str] = asyncio.subprocess.STDOUT) -> str:
        ...

    async def _run(self,
                   resource: str,
                   command: MutableSequence[str],
                   environment: MutableMapping[str, str] = None,
                   workdir: Optional[str] = None,
                   stdin: Optional[Union[int, str]] = None,
                   stdout: Union[int, str] = asyncio.subprocess.STDOUT,
                   stderr: Union[int, str] = asyncio.subprocess.STDOUT,
                   job_name: Optional[str] = None,
                   capture_output: bool = False,
                   encode: bool = True,
                   interactive: bool = False,
                   stream: bool = False) -> Union[Optional[Tuple[Optional[Any], int]], asyncio.subprocess.Process]:
        # TODO: find a smarter way to identify detachable jobs when implementing stacked connectors
        if job_name:
            command = utils.create_command(
                command=command,
                environment=environment,
                workdir=workdir)
            logger.debug("Executing command {command} on {resource} {job}".format(
                command=command,
                resource=resource,
                job="for job {job}".format(job=job_name) if job_name else ""))
            helper_file = await self._build_helper_file(command, resource, environment, workdir)
            job_id = await self._run_batch_command(
                helper_file=helper_file,
                job_name=job_name,
                resource=resource,
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
                    running_jobs = await self._get_running_jobs(resource)
                if job_id not in running_jobs:
                    break
                await asyncio.sleep(self.pollingInterval)
            self.scheduledJobs.remove(job_id)
            return (
                await self._get_output(job_id, resource) if stdout == STDOUT else None,
                await self._get_returncode(job_id, resource))
        else:
            return await super()._run(
                resource=resource,
                command=command,
                environment=environment,
                workdir=workdir,
                stdin=stdin,
                stdout=stdout,
                stderr=stderr,
                job_name=job_name,
                capture_output=capture_output,
                encode=encode,
                interactive=interactive,
                stream=stream)

    async def get_available_resources(self, service: str) -> MutableMapping[str, Resource]:
        return {self.hostname: Resource(
            name=self.hostname,
            hostname=self.hostname,
            slots=self.maxConcurrentJobs)}

    async def undeploy(self, external: bool) -> None:
        await self._remove_jobs(self.hostname)
        self.scheduledJobs = {}
        await super().undeploy(external)


class SlurmConnector(QueueManagerConnector):

    async def _get_output(self,
                          job_id: str,
                          resource: str) -> str:
        async with self._get_ssh_client(resource) as ssh_client:
            output_path = (await ssh_client.run(
                "scontrol show -o job {job_id} | sed -n 's/^.*StdOut=\\([^[:space:]]*\\).*/\\1/p'".format(
                    job_id=job_id
                ))).stdout.strip()
            return ((await ssh_client.run('cat {output}'.format(output=output_path))).stdout.strip()
                    if output_path else "")

    async def _get_returncode(self,
                              job_id: str,
                              resource: str) -> int:
        async with self._get_ssh_client(resource) as ssh_client:
            return int((await ssh_client.run(
                "scontrol show -o job {job_id} | sed -n 's/^.*ExitCode=\\([0-9]\\+\\):.*/\\1/p'".format(
                    job_id=job_id
                ))).stdout.strip())

    @cachedmethod(lambda self: self.jobsCache, key=partial(cachetools.keys.hashkey, 'running_jobs'))
    async def _get_running_jobs(self,
                                resource: str) -> MutableSequence[str]:
        async with self._get_ssh_client(resource) as ssh_client:
            return [j.strip() for j in (await ssh_client.run(
                "squeue -h -j {job_ids} -t {states} -O JOBID".format(
                    job_ids=",".join(self.scheduledJobs),
                    states=",".join(['PENDING', 'RUNNING', 'SUSPENDED', 'COMPLETING', 'CONFIGURING',
                                     'RESIZING', 'REVOKED', 'SPECIAL_EXIT'])
                ))).stdout.strip().splitlines()]

    async def _remove_jobs(self,
                           resource: str) -> None:
        async with self._get_ssh_client(resource) as ssh_client:
            await ssh_client.run(
                "scancel {job_ids}".format(job_ids=" ".join(self.scheduledJobs)))

    async def _run_batch_command(self,
                                 helper_file: str,
                                 job_name: str,
                                 resource: str,
                                 workdir: Optional[str] = None,
                                 stdin: Optional[Union[int, str]] = None,
                                 stdout: Union[int, str] = asyncio.subprocess.STDOUT,
                                 stderr: Union[int, str] = asyncio.subprocess.STDOUT) -> str:
        batch_command = "sbatch --parsable {workdir} {stdin} {stdout} {stderr} {helper_file}".format(
            workdir="-D {workdir}".format(workdir=workdir) if workdir is not None else "",
            stdin="-i \"{stdin}\"".format(stdin=stdin) if stdin is not None else "",
            stdout="-o \"{stdout}\"".format(stdout=stdout) if stdout != STDOUT else "",
            stderr="-e \"{stderr}\"".format(stderr=stderr) if stderr != STDOUT and stderr != stdout else "",
            helper_file=helper_file)
        async with self._get_ssh_client(resource) as ssh_client:
            result = await ssh_client.run(batch_command)
        return result.stdout.strip()


class PBSConnector(QueueManagerConnector):

    async def _get_output(self,
                          job_id: str,
                          resource: str) -> str:
        async with self._get_ssh_client(resource) as ssh_client:
            output_path = (await ssh_client.run(
                "qstat {job_id} -xf | sed -n 's/^\\s*Output_Path\\s=\\s.*:\\(.*\\)\\s*$/\\1/p'".format(
                    job_id=job_id
                ))).stdout.strip()
            return ((await ssh_client.run('cat {output}'.format(output=output_path))).stdout.strip()
                    if output_path else "")

    async def _get_returncode(self,
                              job_id: str,
                              resource: str) -> int:
        async with self._get_ssh_client(resource) as ssh_client:
            return int((await ssh_client.run(
                "qstat {job_id} -xf | sed -n 's/^\\s*Exit_status\\s=\\s\\([0-9]\\+\\)\\s*$/\\1/p'".format(
                    job_id=job_id
                ))).stdout.strip())

    @cachedmethod(lambda self: self.jobsCache, key=partial(cachetools.keys.hashkey, 'running_jobs'))
    async def _get_running_jobs(self,
                                resource: str) -> MutableSequence[str]:
        async with self._get_ssh_client(resource) as ssh_client:
            return (await ssh_client.run(
                "qstat -awx {job_ids} | grep '{grep_ids}' | awk '{{if($10 != \"E\" && $10 != \"F\") {{print $1}}}}'".format(
                    job_ids=" ".join(self.scheduledJobs),
                    grep_ids="\\|".join(self.scheduledJobs)
                ))).stdout.strip().splitlines()

    async def _remove_jobs(self,
                           resource: str) -> None:
        async with self._get_ssh_client(resource) as ssh_client:
            await ssh_client.run(
                "qdel {job_ids}".format(job_ids=" ".join(self.scheduledJobs)))

    async def _run_batch_command(self,
                                 helper_file: str,
                                 job_name: str,
                                 resource: str,
                                 workdir: Optional[str] = None,
                                 stdin: Optional[Union[int, str]] = None,
                                 stdout: Union[int, str] = asyncio.subprocess.STDOUT,
                                 stderr: Union[int, str] = asyncio.subprocess.STDOUT) -> str:
        batch_command = "{workdir} qsub {stdin} {stdout} {stderr} {helper_file}".format(
            workdir="cd {workdir} &&".format(workdir=workdir) if workdir is not None else "",
            stdin="-i \"{stdin}\"".format(stdin=stdin) if stdin is not None else "",
            stdout=("-o \"{stdout}\"".format(stdout=stdout if stdout != STDOUT else utils.random_name())),
            stderr="-e \"{stderr}\"".format(stderr=stderr) if stderr != STDOUT and stderr != stdout else "",
            helper_file=helper_file)
        async with self._get_ssh_client(resource) as ssh_client:
            result = await ssh_client.run(batch_command)
        return result.stdout.strip()
