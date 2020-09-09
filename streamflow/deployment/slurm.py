import os
from asyncio.subprocess import STDOUT
from typing import Optional, List, MutableMapping, Tuple, Any

from ruamel.yaml import YAML
from typing_extensions import Text

from streamflow.deployment.ssh import SSHConnector


class SlurmConnector(SSHConnector):

    def __init__(self,
                 streamflow_config_dir: Text,
                 file: Text,
                 hostname: Text,
                 sharedPaths: List[Text],
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
        self.sharedPaths: List[Text] = sharedPaths

    def _is_shared(self, path: Text) -> bool:
        for shared_path in self.sharedPaths:
            if path.startswith(shared_path):
                return True
        return False

    async def _run_sbatch(self,
                          resource: Text,
                          command: List[Text],
                          environment: MutableMapping[Text, Text] = None,
                          workdir: Optional[Text] = None,
                          capture_output: bool = False) -> Optional[Tuple[Optional[Any], int]]:
        encoded_command = self.create_encoded_command(command, resource, environment, workdir)
        helper_file = await self._build_helper_file(encoded_command, resource, environment, workdir)
        sbatch_command = "sbatch --wait {workdir} {helper_file}".format(
            workdir="-D {workdir}".format(workdir=workdir) if workdir is not None else "",
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
                  command: List[Text],
                  environment: MutableMapping[Text, Text] = None,
                  workdir: Optional[Text] = None,
                  capture_output: bool = False,
                  task_command: bool = False) -> Optional[Tuple[Optional[Any], int]]:
        if not task_command:
            return await super().run(resource, command, environment, workdir, capture_output, task_command)
        else:
            return await self._run_sbatch(resource, command, environment, workdir, capture_output)
