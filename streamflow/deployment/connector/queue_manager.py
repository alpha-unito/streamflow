import asyncio
import os
from abc import ABC, abstractmethod
from asyncio.subprocess import STDOUT
from typing import Optional, MutableSequence, MutableMapping, Tuple, Any, Union

from ruamel.yaml import YAML
from typing_extensions import Text

from streamflow.core import utils
from streamflow.deployment.connector.ssh import SSHConnector
from streamflow.log_handler import logger


class QueueManagerConnector(SSHConnector, ABC):

    def __init__(self,
                 streamflow_config_dir: Text,
                 file: Text,
                 hostname: Text,
                 sshKey: Text,
                 username: Text,
                 pollingInterval: int = 5,
                 sshKeyPassphrase: Optional[Text] = None) -> None:
        super().__init__(
            streamflow_config_dir=streamflow_config_dir,
            file=file,
            hostname=hostname,
            sshKey=sshKey,
            sshKeyPassphrase=sshKeyPassphrase,
            username=username)
        self.pollingInterval: int = pollingInterval
        with open(os.path.join(streamflow_config_dir, file)) as f:
            yaml = YAML(typ='safe')
            self.env_description = yaml.load(f)

    @abstractmethod
    async def get_output(self,
                         job_id: Text) -> Text:
        ...

    @abstractmethod
    async def get_returncode(self,
                             job_id: Text) -> int:
        ...

    @abstractmethod
    async def is_terminated(self,
                            job_id: Text) -> bool:
        ...

    @abstractmethod
    async def run_batch_command(self,
                                helper_file: Text,
                                job_name: Text,
                                workdir: Optional[Text] = None,
                                stdin: Optional[Union[int, Text]] = None,
                                stdout: Union[int, Text] = asyncio.subprocess.STDOUT,
                                stderr: Union[int, Text] = asyncio.subprocess.STDOUT) -> Text:
        ...

    async def _run(self,
                   resource: Text,
                   command: MutableSequence[Text],
                   environment: MutableMapping[Text, Text] = None,
                   workdir: Optional[Text] = None,
                   stdin: Optional[Union[int, Text]] = None,
                   stdout: Union[int, Text] = asyncio.subprocess.STDOUT,
                   stderr: Union[int, Text] = asyncio.subprocess.STDOUT,
                   job_name: Optional[Text] = None,
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
            job_id = await self.run_batch_command(
                helper_file=helper_file,
                job_name=job_name,
                workdir=workdir,
                stdin=stdin,
                stdout=stdout,
                stderr=stderr)
            while not await self.is_terminated(job_id):
                await asyncio.sleep(self.pollingInterval)
            return await self.get_output(job_id) if stdout == STDOUT else None, await self.get_returncode(job_id)
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


class SlurmConnector(QueueManagerConnector):

    async def get_output(self,
                         job_id: Text) -> Text:
        return (await self.ssh_client.run(
            "scontrol show -o job {job_id} | sed -n 's/^.*StdOut=\\([^[:space:]]*\\).*\\/\\1/p'".format(
                job_id=job_id
            ))).stdout.strip()

    async def get_returncode(self,
                             job_id: Text) -> int:
        return int((await self.ssh_client.run(
            "scontrol show -o job {job_id} | sed -n 's/^.*ExitCode=\\([0-9]\\+\\):.*/\\1/p'".format(
                job_id=job_id
            ))).stdout.strip())

    async def is_terminated(self,
                            job_id: Text) -> bool:
        status = (await self.ssh_client.run(
            "scontrol show -o job {job_id} | sed -n 's/^.*JobState=\\([A-Z_]\\+\\).*$/\\1/p'".format(
                job_id=job_id
            ))).stdout.strip()
        return status in ['BOOT_FAIL', 'CANCELLED', 'COMPLETED', 'DEADLINE', 'FAILED',
                          'NODE_FAIL', 'OUT_OF_MEMORY', 'PREEMPTED', 'TIMEOUT']

    async def run_batch_command(self,
                                helper_file: Text,
                                job_name: Text,
                                workdir: Optional[Text] = None,
                                stdin: Optional[Union[int, Text]] = None,
                                stdout: Union[int, Text] = asyncio.subprocess.STDOUT,
                                stderr: Union[int, Text] = asyncio.subprocess.STDOUT) -> Text:
        batch_command = "sbatch --parsable {workdir} {job_name} {stdin} {stdout} {stderr} {helper_file}".format(
            workdir="-D {workdir}".format(workdir=workdir) if workdir is not None else "",
            job_name="-J \"{job_name}\"".format(job_name=job_name),
            stdin="-i \"{stdin}\"".format(stdin=stdin) if stdin is not None else "",
            stdout="-o \"{stdout}\"".format(stdout=stdout) if stdout != STDOUT else "",
            stderr="-e \"{stderr}\"".format(stderr=stderr) if stderr != STDOUT and stderr != stdout else "",
            helper_file=helper_file)
        result = await self.ssh_client.run(batch_command)
        return result.stdout.strip()


class PBSConnector(QueueManagerConnector):

    async def get_output(self,
                         job_id: Text) -> Text:
        output_path = (await self.ssh_client.run(
            "qstat {job_id} -xf | sed -n 's/^\\s*Output_Path\\s=\\s.*:\\(.*\\)\\s*$/\\1/p'".format(
                job_id=job_id
            ))).stdout.strip()
        return await self.ssh_client.run('cat {output}'.format(output=output_path)).stdout.strip()

    async def get_returncode(self,
                             job_id: Text) -> int:
        return int((await self.ssh_client.run(
            "qstat {job_id} -xf | sed -n 's/^\\s*Exit_status\\s=\\s\\([0-9]\\+\\)\\s*$/\\1/p'".format(
                job_id=job_id
            ))).stdout.strip())

    async def is_terminated(self,
                            job_id: Text) -> bool:
        status = (await self.ssh_client.run(
            "qstat {job_id} -xf | sed -n 's/^\\s*job_state\\s=\\s\\([A-Z]\\)\\s*$/\\1/p'".format(
                job_id=job_id
            ))).stdout.strip()
        return status in ['E', 'F']

    async def run_batch_command(self,
                                helper_file: Text,
                                job_name: Text,
                                workdir: Optional[Text] = None,
                                stdin: Optional[Union[int, Text]] = None,
                                stdout: Union[int, Text] = asyncio.subprocess.STDOUT,
                                stderr: Union[int, Text] = asyncio.subprocess.STDOUT) -> Text:
        batch_command = "{workdir} qsub {job_name} {workdir} {stdin} {stdout} {stderr} {helper_file}".format(
            workdir="cd {workdir} &&".format(workdir=workdir) if workdir is not None else "",
            job_name="-N \"{job_name}\"".format(job_name=job_name),
            stdin="-i \"{stdin}\"".format(stdin=stdin) if stdin is not None else "",
            stdout=("-o \"{stdout}\"".format(stdout=stdout if stdout != STDOUT else
                    "{job_name}.out".format(job_name=job_name))),
            stderr="-e \"{stderr}\"".format(stderr=stderr) if stderr != STDOUT and stderr != stdout else "",
            helper_file=helper_file)
        result = await self.ssh_client.run(batch_command)
        return result.stdout.strip()
