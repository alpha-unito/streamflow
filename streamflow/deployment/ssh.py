import asyncio
import os
import posixpath
import stat
import tempfile
from asyncio.subprocess import STDOUT
from typing import List, Optional, MutableMapping, Tuple, Any

import asyncssh
from jinja2 import Template
from typing_extensions import Text

from streamflow.core.scheduling import Resource
from streamflow.deployment.base import BaseConnector


def _parse_hostname(hostname):
    if ':' in hostname:
        hostname, port = hostname.split(':')
        port = int(port)
    else:
        port = 22
    return hostname, port


class SSHConnector(BaseConnector):

    def __init__(self,
                 streamflow_config_dir: Text,
                 hostname: Text,
                 username: Text,
                 sshKey: Text,
                 file: Optional[Text] = None,
                 sshKeyPassphrase: Optional[Text] = None) -> None:
        super().__init__(streamflow_config_dir)
        if file is not None:
            with open(os.path.join(streamflow_config_dir, file)) as f:
                self.template: Optional[Template] = Template(f.read())
        else:
            self.template: Optional[Template] = None
        self.hostname: Text = hostname
        self.sshKey: Text = sshKey
        self.sshKeyPassphrase: Optional[Text] = sshKeyPassphrase
        self.username: Text = username
        self.jobs_table: MutableMapping[Text, List[Text]] = {}
        self.ssh_client = None

    async def _build_helper_file(self,
                                 command: List[Text],
                                 resource: Text,
                                 environment: MutableMapping[Text, Text] = None,
                                 workdir: Text = None
                                 ) -> Text:
        helper_file = tempfile.mktemp()
        with open(helper_file, mode='w') as f:
            f.write(self.template.render(
                streamflow_command=command,
                streamflow_environment=environment,
                streamflow_workdir=workdir
            ))
        os.chmod(helper_file, os.stat(helper_file).st_mode | stat.S_IEXEC)
        remote_path = posixpath.join(workdir if workdir is not None else '/tmp', os.path.basename(helper_file))
        await self._copy_local_to_remote(helper_file, remote_path, [resource])
        return remote_path

    async def _copy_local_to_remote(self, src: Text, dst: Text, resources: List[Text]):
        copy_tasks = []
        for resource in resources:
            copy_tasks.append(asyncio.create_task(
                self._copy_local_to_remote_single(src, dst, resource, None)))
        await asyncio.gather(*copy_tasks)

    async def _copy_local_to_remote_single(self, src: Text, dst: Text, resource: Text, temp_dir: Optional[Text]):
        await asyncssh.scp(src, (self.ssh_client, dst), preserve=True, recurse=True)

    async def _copy_remote_to_local(self, src: Text, dst: Text, resource: Text) -> None:
        await asyncssh.scp((self.ssh_client, src), dst, preserve=True, recurse=True)

    async def deploy(self) -> None:
        (hostname, port) = _parse_hostname(self.hostname)
        self.ssh_client = await asyncssh.connect(
            host=hostname,
            port=port,
            username=self.username,
            client_keys=[self.sshKey],
            passphrase=self.sshKeyPassphrase
        )

    async def get_available_resources(self, service: Text) -> MutableMapping[Text, Resource]:
        return {self.hostname: Resource(self.hostname, self.hostname)}

    async def run(self,
                  resource: Text,
                  command: List[Text],
                  environment: MutableMapping[Text, Text] = None,
                  workdir: Optional[Text] = None,
                  capture_output: bool = False,
                  job_name: Optional[Text] = None) -> Optional[Tuple[Optional[Any], int]]:
        encoded_command = self.create_encoded_command(command, resource, environment, workdir)
        if job_name is not None and self.template is not None:
            helper_file = await self._build_helper_file(encoded_command, resource, environment, workdir)
            result = await self.ssh_client.run(helper_file, stderr=STDOUT)
        else:
            result = await self.ssh_client.run(encoded_command, stderr=STDOUT)
        if capture_output:
            return result.stdout.strip(), result.returncode

    async def undeploy(self) -> None:
        if self.ssh_client is not None:
            self.ssh_client.close()
            self.ssh_client = None
