from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import shlex
from abc import ABC, abstractmethod
from functools import partial
from typing import Any, MutableMapping, MutableSequence

import cachetools
import pkg_resources

from streamflow.core import utils
from streamflow.core.asyncache import cachedmethod
from streamflow.core.deployment import Location
from streamflow.core.exception import (
    WorkflowDefinitionException,
    WorkflowExecutionException,
)
from streamflow.core.scheduling import AvailableLocation
from streamflow.deployment.connector.ssh import SSHConnector
from streamflow.log_handler import logger


class QueueManagerConnector(SSHConnector, ABC):
    def __init__(
        self,
        deployment_name: str,
        config_dir: str,
        hostname: str,
        username: str,
        checkHostKey: bool = True,
        dataTransferConnection: str | MutableMapping[str, Any] | None = None,
        file: str | None = None,
        maxConcurrentJobs: int | None = 1,
        maxConcurrentSessions: int | None = 10,
        maxConnections: int | None = 1,
        passwordFile: str | None = None,
        pollingInterval: int = 5,
        services: MutableMapping[str, str] | None = None,
        sshKey: str | None = None,
        sshKeyPassphraseFile: str | None = None,
        transferBufferSize: int = 2**16,
    ) -> None:
        super().__init__(
            deployment_name=deployment_name,
            config_dir=config_dir,
            checkHostKey=checkHostKey,
            dataTransferConnection=dataTransferConnection,
            file=file,
            maxConcurrentSessions=maxConcurrentSessions,
            maxConnections=maxConnections,
            nodes=[hostname],
            passwordFile=passwordFile,
            services=services,
            sshKey=sshKey,
            sshKeyPassphraseFile=sshKeyPassphraseFile,
            transferBufferSize=transferBufferSize,
            username=username,
        )
        self.hostname: str = hostname
        self.maxConcurrentJobs: int = maxConcurrentJobs
        self.pollingInterval: int = pollingInterval
        self.scheduledJobs: MutableSequence[str] = []
        self.jobsCache: cachetools.Cache = cachetools.TTLCache(
            maxsize=1, ttl=self.pollingInterval
        )
        self.jobsCacheLock: asyncio.Lock = asyncio.Lock()

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
    async def _remove_jobs(self, location: str) -> None:
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

    async def get_available_locations(
        self,
        service: str | None = None,
        input_directory: str | None = None,
        output_directory: str | None = None,
        tmp_directory: str | None = None,
    ) -> MutableMapping[str, AvailableLocation]:
        if service is not None and service not in self.templates:
            raise WorkflowDefinitionException(
                f"Invalid service {service} for deployment {self.deployment_name}."
            )
        return {
            self.hostname: AvailableLocation(
                name=self.hostname,
                deployment=self.deployment_name,
                service=service,
                hostname=self.hostname,
                slots=self.maxConcurrentJobs,
            )
        }

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "queue_manager.json")
        )

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
                command=command, environment=environment, workdir=workdir
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
            command = self._get_command_from_template(
                command=command,
                environment=environment,
                template=location.service,
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
        await self._remove_jobs(self.hostname)
        self.scheduledJobs = {}
        await super().undeploy(external)


class SlurmConnector(QueueManagerConnector):
    async def _get_output(self, job_id: str, location: Location) -> str:
        async with self._get_ssh_client(location.name) as ssh_client:
            command = (
                f"scontrol show -o job {job_id} | "
                "sed -n 's/^.*StdOut=\\([^[:space:]]*\\).*/\\1/p'"
            )
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Running command {command}")
            output_path = (await ssh_client.run(command)).stdout.strip()
            return (
                (await ssh_client.run(f"cat {output_path}")).stdout.strip()
                if output_path
                else ""
            )

    async def _get_returncode(self, job_id: str, location: Location) -> int:
        async with self._get_ssh_client(location.name) as ssh_client:
            command = (
                f"scontrol show -o job {job_id} | "
                "sed -n 's/^.*ExitCode=\\([0-9]\\+\\):.*/\\1/p'"
            )
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Running command {command}")
            return int((await ssh_client.run(command)).stdout.strip())

    @cachedmethod(
        lambda self: self.jobsCache,
        key=partial(cachetools.keys.hashkey, "running_jobs"),
    )
    async def _get_running_jobs(self, location: Location) -> MutableSequence[str]:
        async with self._get_ssh_client(location.name) as ssh_client:
            command = "squeue -h -j {job_ids} -t {states} -O JOBID".format(
                job_ids=",".join(self.scheduledJobs),
                states=",".join(
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
            )
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Running command {command}")
            result = (await ssh_client.run(command)).stdout.strip()
            return [j.strip() for j in result.splitlines()]

    async def _remove_jobs(self, location: str) -> None:
        async with self._get_ssh_client(location) as ssh_client:
            await ssh_client.run(f"scancel {' '.join(self.scheduledJobs)}")

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
        batch_command = (
            "sh -c 'echo {command} | "
            "base64 -d | "
            "sbatch --parsable {workdir} {stdin} {stdout} {stderr} {timeout}'"
        ).format(
            workdir=(f"-D {workdir}" if workdir is not None else ""),
            stdin=(f'-i "{shlex.quote(stdin)}"' if stdin is not None else ""),
            stdout=(
                f'-o "{shlex.quote(stdout)}"'
                if stdout != asyncio.subprocess.STDOUT
                else ""
            ),
            stderr=(
                f'-e "{shlex.quote(stderr)}"'
                if stderr != asyncio.subprocess.STDOUT and stderr != stdout
                else ""
            ),
            timeout=(
                f"-t {utils.format_seconds_to_hhmmss(timeout)}" if timeout else ""
            ),
            command=base64.b64encode(command.encode("utf-8")).decode("utf-8"),
        )
        async with self._get_ssh_client(location.name) as ssh_client:
            result = await ssh_client.run(batch_command)
            if result.returncode == 0:
                return result.stdout.strip()
            else:
                raise WorkflowExecutionException(
                    f"Error submitting job {job_name} to Slurm: {result.stderr.strip()}"
                )


class PBSConnector(QueueManagerConnector):
    async def _get_output(self, job_id: str, location: Location) -> str:
        async with self._get_ssh_client(location.name) as ssh_client:
            command = f"qstat {job_id} -xf -Fjson"
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Running command {command}")
            result = json.loads((await ssh_client.run(command)).stdout.strip())
            output_path = result["Jobs"][job_id]["Output_Path"]
            if ":" in output_path:
                output_path = "".join(output_path.split(":")[1:])
            return (
                (await ssh_client.run(f"cat {output_path}")).stdout.strip()
                if output_path
                else ""
            )

    async def _get_returncode(self, job_id: str, location: Location) -> int:
        async with self._get_ssh_client(location.name) as ssh_client:
            command = f"qstat {job_id} -xf -Fjson"
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Running command {command}")
            result = json.loads((await ssh_client.run(command)).stdout.strip())
            return int(result["Jobs"][job_id]["Exit_status"])

    @cachedmethod(
        lambda self: self.jobsCache,
        key=partial(cachetools.keys.hashkey, "running_jobs"),
    )
    async def _get_running_jobs(self, location: Location) -> MutableSequence[str]:
        async with self._get_ssh_client(location.name) as ssh_client:
            command = f"qstat {' '.join(self.scheduledJobs)} -xf -Fjson"
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Running command {command}")
            result = json.loads((await ssh_client.run(command)).stdout.strip())
            return [
                j
                for j in self.scheduledJobs
                if j not in result["Jobs"]  # Job id has not been processed yet
                or result["Jobs"][j]["job_state"] not in ["E", "F"]  # Job finished
            ]

    async def _remove_jobs(self, location: str) -> None:
        async with self._get_ssh_client(location) as ssh_client:
            await ssh_client.run(f"qdel {' '.join(self.scheduledJobs)}")

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
        batch_command = "sh -c '{workdir} echo {command} | base64 -d | qsub {stdin} {stdout} {stderr} {timeout} -'".format(
            workdir=(f"cd {workdir} &&" if workdir is not None else ""),
            stdin=(f'-i "{stdin}"' if stdin is not None else ""),
            stdout='-o "{stdout}"'.format(
                stdout=stdout
                if stdout != asyncio.subprocess.STDOUT
                else utils.random_name()
            ),
            stderr=(
                f'-e "{stderr}"'
                if stderr != asyncio.subprocess.STDOUT and stderr != stdout
                else ""
            ),
            timeout=(
                f"-l walltime={utils.format_seconds_to_hhmmss(timeout)}"
                if timeout
                else ""
            ),
            command=base64.b64encode(command.encode("utf-8")).decode("utf-8"),
        )
        async with self._get_ssh_client(location.name) as ssh_client:
            result = await ssh_client.run(batch_command)
            if result.returncode == 0:
                return result.stdout.strip()
            else:
                raise WorkflowExecutionException(
                    f"Error submitting job {job_name} "
                    f"to PBS: {result.stderr.strip()}"
                )
