import asyncio
import os
import posixpath
import stat
import tempfile
from asyncio import Semaphore, Lock
from asyncio.subprocess import STDOUT
from typing import MutableSequence, Optional, MutableMapping, Tuple, Any, Union

import asyncssh
from asyncssh import SSHClientConnection
from jinja2 import Template
from typing_extensions import Text

from streamflow.core import utils
from streamflow.core.scheduling import Resource
from streamflow.deployment.connector.base import BaseConnector
from streamflow.log_handler import logger


class SSHContext(object):

    def __init__(self,
                 host: Text,
                 port: int,
                 username: Text,
                 client_keys: MutableSequence[Text],
                 passphrase: Text,
                 max_concurrent_sessions: int):
        self._host: Text = host
        self._port: int = port
        self._username: Text = username
        self._client_keys: MutableSequence[Text] = client_keys
        self._passphrase: Text = passphrase
        self._ssh_connection: Optional[SSHClientConnection] = None
        self._connection_lock: Lock = Lock()
        self._sem: Semaphore = Semaphore(max_concurrent_sessions)

    async def __aenter__(self):
        with await self._connection_lock:
            if self._ssh_connection is None:
                self._ssh_connection = await asyncssh.connect(
                    host=self._host,
                    port=self._port,
                    username=self._username,
                    client_keys=self._client_keys,
                    passphrase=self._passphrase)
        await self._sem.acquire()
        return self._ssh_connection

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._sem.release()


def _parse_hostname(hostname):
    if ':' in hostname:
        hostname, port = hostname.split(':')
        port = int(port)
    else:
        port = 22
    return hostname, port


class SSHConnector(BaseConnector):

    @staticmethod
    def _get_command(resource: Text,
                     command: MutableSequence[Text],
                     environment: MutableMapping[Text, Text] = None,
                     workdir: Optional[Text] = None,
                     stdin: Optional[Union[int, Text]] = None,
                     stdout: Union[int, Text] = asyncio.subprocess.STDOUT,
                     stderr: Union[int, Text] = asyncio.subprocess.STDOUT,
                     job_name: Optional[Text] = None,
                     encode: bool = True):
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
        return utils.encode_command(command) if encode else command

    def __init__(self,
                 streamflow_config_dir: Text,
                 hostname: Text,
                 username: Text,
                 sshKey: Text,
                 file: Optional[Text] = None,
                 maxConcurrentSessions: int = 10,
                 sshKeyPassphrase: Optional[Text] = None,
                 transferBufferSize: int = 2 ** 16) -> None:
        super().__init__(
            streamflow_config_dir=streamflow_config_dir,
            transferBufferSize=transferBufferSize)
        if file is not None:
            with open(os.path.join(streamflow_config_dir, file)) as f:
                self.template: Optional[Template] = Template(f.read())
        else:
            self.template: Optional[Template] = None
        self.hostname: Text = hostname
        self.sshKey: Text = sshKey
        self.sshKeyPassphrase: Optional[Text] = sshKeyPassphrase
        self.username: Text = username
        self.maxConcurrentSessions: int = maxConcurrentSessions
        self.jobs_table: MutableMapping[Text, MutableSequence[Text]] = {}
        self.ssh_contexts: MutableMapping[Text, SSHContext] = {}

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
        await self._copy_local_to_remote(
            src=helper_file,
            dst=remote_path,
            resources=[resource])
        return remote_path

    async def _copy_local_to_remote(self,
                                    src: Text,
                                    dst: Text,
                                    resources: MutableSequence[Text],
                                    read_only: bool = False):
        for resource in resources:
            async with self._get_ssh_client(resource) as ssh_client:
                await asyncssh.scp(src, (ssh_client, dst), preserve=True, recurse=True)

    async def _copy_remote_to_local(self,
                                    src: Text,
                                    dst: Text,
                                    resource: Text,
                                    read_only: bool = False) -> None:
        async with self._get_ssh_client(resource) as ssh_client:
            await asyncssh.scp((ssh_client, src), dst, preserve=True, recurse=True)

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

    def _get_ssh_client(self, resource: Text):
        if resource not in self.ssh_contexts:
            (hostname, port) = _parse_hostname(self.hostname)
            self.ssh_contexts[resource] = SSHContext(
                host=hostname,
                port=port,
                username=self.username,
                client_keys=[self.sshKey],
                passphrase=self.sshKeyPassphrase,
                max_concurrent_sessions=self.maxConcurrentSessions)
        return self.ssh_contexts[resource]

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
        command = self._get_command(
            resource=resource,
            command=command,
            environment=environment,
            workdir=workdir,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            encode=encode,
            job_name=job_name)
        if job_name is not None and self.template is not None:
            helper_file = await self._build_helper_file(command, resource, environment, workdir)
            async with self._get_ssh_client(resource) as ssh_client:
                result = await ssh_client.run(helper_file, stderr=STDOUT)
        else:
            async with self._get_ssh_client(resource) as ssh_client:
                result = await ssh_client.run("sh -c '{command}'".format(command=command), stderr=STDOUT)
        if capture_output:
            return result.stdout.strip(), result.returncode

    async def deploy(self, external: bool) -> None:
        pass

    async def get_available_resources(self, service: Text) -> MutableMapping[Text, Resource]:
        return {self.hostname: Resource(self.hostname, self.hostname)}

    async def undeploy(self, external: bool) -> None:
        for ssh_context in self.ssh_contexts.values():
            async with ssh_context as ssh_client:
                ssh_client.close()
        self.ssh_contexts = {}
