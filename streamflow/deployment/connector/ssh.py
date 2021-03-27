import asyncio
import os
import posixpath
import stat
import tempfile
from asyncio.subprocess import STDOUT
from typing import MutableSequence, Optional, MutableMapping, Tuple, Any, Union

import asyncssh
from jinja2 import Template
from typing_extensions import Text

from streamflow.core import utils
from streamflow.core.scheduling import Resource
from streamflow.deployment.connector.base import BaseConnector
from streamflow.log_handler import logger


def _parse_hostname(hostname):
    if ':' in hostname:
        hostname, port = hostname.split(':')
        port = int(port)
    else:
        port = 22
    return hostname, port


class SSHConnector(BaseConnector):

    def _get_run_command(self,
                         command: Text,
                         resource: Text,
                         interactive: bool = False):
        return "".join([
            "ssh ",
            "{resource} ",
            "{command}"
        ]).format(
            resource=resource,
            command=command)

    def __init__(self,
                 streamflow_config_dir: Text,
                 hostname: Text,
                 username: Text,
                 sshKey: Text,
                 file: Optional[Text] = None,
                 sshKeyPassphrase: Optional[Text] = None,
                 transferBufferSize: int = 2 ** 16) -> None:
        super().__init__(streamflow_config_dir, transferBufferSize)
        if file is not None:
            with open(os.path.join(streamflow_config_dir, file)) as f:
                self.template: Optional[Template] = Template(f.read())
        else:
            self.template: Optional[Template] = None
        self.hostname: Text = hostname
        self.sshKey: Text = sshKey
        self.sshKeyPassphrase: Optional[Text] = sshKeyPassphrase
        self.username: Text = username
        self.jobs_table: MutableMapping[Text, MutableSequence[Text]] = {}
        self.ssh_client = None

    async def _build_helper_file(self,
                                 command: Text,
                                 resource: Text,
                                 environment: MutableMapping[Text, Text] = None,
                                 workdir: Text = None) -> Text:
        helper_file = tempfile.mktemp()
        with open(helper_file, mode='w') as f:
            f.write(self.template.render(
                streamflow_command="sh -c '{command}'".format(command=command),
                streamflow_environment=environment,
                streamflow_workdir=workdir))
        os.chmod(helper_file, os.stat(helper_file).st_mode | stat.S_IEXEC)
        remote_path = posixpath.join(workdir or '/tmp', os.path.basename(helper_file))
        await self._copy_local_to_remote(helper_file, remote_path, [resource])
        return remote_path

    async def _copy_local_to_remote(self, src: Text, dst: Text, resources: MutableSequence[Text]):
        await asyncio.gather(*[asyncio.create_task(
            asyncssh.scp(src, (self.ssh_client, dst), preserve=True, recurse=True)
        ) for _ in resources])

    async def _copy_remote_to_local(self, src: Text, dst: Text, resource: Text) -> None:
        await asyncssh.scp((self.ssh_client, src), dst, preserve=True, recurse=True)

    async def deploy(self, external: bool) -> None:
        (hostname, port) = _parse_hostname(self.hostname)
        self.ssh_client = await asyncssh.connect(
            host=hostname,
            port=port,
            username=self.username,
            client_keys=[self.sshKey],
            passphrase=self.sshKeyPassphrase)

    async def get_available_resources(self, service: Text) -> MutableMapping[Text, Resource]:
        return {self.hostname: Resource(self.hostname, self.hostname)}

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
        command = utils.create_command(
            command=command,
            environment=environment,
            workdir=workdir,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr)
        logger.debug("Executing command {command} on {resource} {job}".format(
            command=command,
            resource=resource,
            job="for job {job}".format(job=job_name) if job_name else ""))
        command = utils.encode_command(command)
        if self.template is not None:
            helper_file = await self._build_helper_file(command, resource, environment, workdir)
            result = await self.ssh_client.run(helper_file, stderr=STDOUT)
        else:
            result = await self.ssh_client.run("sh -c '{command}'".format(command=command), stderr=STDOUT)
        if capture_output:
            return result.stdout.strip(), result.returncode

    async def undeploy(self, external: bool) -> None:
        if self.ssh_client is not None:
            self.ssh_client.close()
            self.ssh_client = None
