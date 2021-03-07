import os
from asyncio.subprocess import STDOUT
from typing import Optional, MutableSequence, MutableMapping, Tuple, Any, Union

from ruamel.yaml import YAML
from typing_extensions import Text

from streamflow.deployment.connector.ssh import SSHConnector


class SlurmConnector(SSHConnector):

    def __init__(self,
                 streamflow_config_dir: Text,
                 file: Text,
                 hostname: Text,
                 sshKey: Text,
                 username: Text,
                 sshKeyPassphrase: Optional[Text] = None
                 ) -> None:
        super().__init__(
            streamflow_config_dir=streamflow_config_dir,
            file=file,
            hostname=hostname,
            sshKey=sshKey,
            sshKeyPassphrase=sshKeyPassphrase,
            username=username)
        with open(os.path.join(streamflow_config_dir, file)) as f:
            yaml = YAML(typ='safe')
            self.env_description = yaml.load(f)

    async def _run_sbatch(self,
                          resource: Text,
                          command: MutableSequence[Text],
                          environment: MutableMapping[Text, Text] = None,
                          workdir: Optional[Text] = None,
                          stdin: Optional[Union[int, Text]] = None,
                          stdout: Union[int, Text] = STDOUT,
                          stderr: Union[int, Text] = STDOUT,
                          capture_output: bool = False) -> Optional[Tuple[Optional[Any], int]]:
        encoded_command = self.create_encoded_command(
            command=command,
            resource=resource,
            environment=environment,
            workdir=workdir)
        helper_file = await self._build_helper_file(encoded_command, resource, environment, workdir)
        sbatch_command = "sbatch --wait {workdir} {stdin} {stdout} {stderr} {helper_file}".format(
            workdir="-D {workdir}".format(workdir=workdir) if workdir is not None else "",
            stdin="-i \"{stdin}\"".format(stdin=stdin) if stdin is not None else "",
            stdout="-o \"{stdout}\"".format(stdout=stdout) if stdout != STDOUT else "",
            stderr="-e \"{stderr}\"".format(stderr=stderr) if stderr != STDOUT and stderr != stdout else "",
            helper_file=helper_file
        )
        result = await self.ssh_client.run(sbatch_command, stderr=STDOUT)
        if capture_output:
            status = result.returncode
            lines = (line for line in result.stdout.split('\n'))
            for line in lines:
                if line.startswith("Submitted batch job"):
                    job_id = line.split()[-1]
                    status = (await self.ssh_client.run(
                        "scontrol show -o job {job_id} | sed -n 's/^.*ExitCode=\\([0-9:]\\+\\).*/\\1/p'".format(
                            job_id=job_id
                        ))).stdout.strip()
            return result.stdout.strip(), status

    async def run(self,
                  resource: Text,
                  command: MutableSequence[Text],
                  environment: MutableMapping[Text, Text] = None,
                  workdir: Optional[Text] = None,
                  stdin: Optional[Union[int, Text]] = None,
                  stdout: Union[int, Text] = STDOUT,
                  stderr: Union[int, Text] = STDOUT,
                  capture_output: bool = False,
                  job_name: Optional[Text] = None) -> Optional[Tuple[Optional[Any], int]]:
        if job_name is None:
            return await super().run(
                resource,
                command,
                environment,
                workdir,
                stdin,
                stdout,
                stderr,
                capture_output,
                job_name)
        else:
            return await self._run_sbatch(
                resource,
                command,
                environment,
                workdir,
                stdin,
                stdout,
                stderr,
                capture_output)
