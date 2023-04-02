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
from streamflow.core.deployment import Connector, Location
from streamflow.core.exception import (
    WorkflowDefinitionException,
    WorkflowExecutionException,
)
from streamflow.core.scheduling import AvailableLocation
from streamflow.deployment.connector.ssh import SSHConnector
from streamflow.deployment.template import CommandTemplateMap
from streamflow.deployment.wrapper import ConnectorWrapper
from streamflow.log_handler import logger


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
        services: MutableMapping[str, str] | None = None,
        sshKey: str | None = None,
        sshKeyPassphraseFile: str | None = None,
        transferBufferSize: int = 2**16,
    ) -> None:
        self._inner_ssh_connector: bool = False
        if hostname is not None:
            if logger.isEnabledFor(logging.WARN):
                logger.warn(
                    "Inline SSH options are deprecated and will be removed in StreamFlow 0.3.0. "
                    "Define a standalone SSH connector and link to it using the `connector` property."
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
        services_map: MutableMapping[str, Any] = {}
        if services:
            for name, service in services.items():
                with open(os.path.join(self.config_dir, service)) as f:
                    services_map[name] = f.read()
        if file is not None:
            if logger.isEnabledFor(logging.WARN):
                logger.warn(
                    "The `file` keyword is deprecated and will be removed in StreamFlow 0.3.0. "
                    "Use `services` instead."
                )
            with open(os.path.join(self.config_dir, file)) as f:
                self.template_map: CommandTemplateMap = CommandTemplateMap(
                    default=f.read(), template_map=services_map
                )
        else:
            self.template_map: CommandTemplateMap = CommandTemplateMap(
                default="#!/bin/sh\n\n{{streamflow_command}}",
                template_map=services_map,
            )
        self.maxConcurrentJobs: int = maxConcurrentJobs
        self.pollingInterval: int = pollingInterval
        self.scheduledJobs: MutableSequence[str] = []
        self.jobsCache: cachetools.Cache = cachetools.TTLCache(
            maxsize=1, ttl=self.pollingInterval
        )
        self.jobsCacheLock: asyncio.Lock = asyncio.Lock()

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

    async def get_available_locations(
        self,
        service: str | None = None,
        input_directory: str | None = None,
        output_directory: str | None = None,
        tmp_directory: str | None = None,
    ) -> MutableMapping[str, AvailableLocation]:
        if service is not None and service not in self.template_map.templates:
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
        if workdir is not None:
            batch_command.extend(["-D", workdir])
        if stdin is not None:
            batch_command.extend(["-i", shlex.quote(stdin)])
        if stdout != asyncio.subprocess.STDOUT:
            batch_command.extend(["-o", shlex.quote(stdout)])
        if stderr != asyncio.subprocess.STDOUT and stderr != stdout:
            batch_command.extend(["-e", shlex.quote(stderr)])
        if timeout:
            batch_command.extend(["-t", utils.format_seconds_to_hhmmss(timeout)])
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
        batch_command.extend(
            [
                "echo",
                base64.b64encode(command.encode("utf-8")).decode("utf-8"),
                "|",
                "base64",
                "-d",
                "|",
                "qsub",
            ]
        )
        if stdin is not None:
            batch_command.extend(["-i", stdin])
        batch_command.extend(
            [
                "-i",
                stdout if stdout != asyncio.subprocess.STDOUT else utils.random_name(),
            ]
        )
        if stderr != asyncio.subprocess.STDOUT and stderr != stdout:
            batch_command.extend(["-e", stderr])
        if timeout:
            batch_command.extend(
                ["-l", f"walltime={utils.format_seconds_to_hhmmss(timeout)}"]
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


class FluxConnector(QueueManagerConnector):
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
            "-N",
            "1",
        ]
        if workdir is not None:
            batch_command.extend(["--cwd", workdir])
        if stdin is not None:
            batch_command.extend(["--input", shlex.quote(stdin)])
        if stdout != asyncio.subprocess.STDOUT:
            batch_command.extend(["--output", shlex.quote(stdout)])
        if stderr != asyncio.subprocess.STDOUT and stderr != stdout:
            batch_command.extend(["--error", shlex.quote(stderr)])
        if timeout:
            batch_command.extend(["-t", utils.format_seconds_to_hhmmss(timeout)])
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
