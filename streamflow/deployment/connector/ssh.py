from __future__ import annotations

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
from cachetools import Cache, LRUCache
from jinja2 import Template

from streamflow.core import utils
from streamflow.core.asyncache import cachedmethod
from streamflow.core.deployment import ConnectorCopyKind
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.scheduling import Resource, Hardware
from streamflow.deployment.connector.base import BaseConnector
from streamflow.log_handler import logger


def _parse_hostname(hostname):
    if ':' in hostname:
        hostname, port = hostname.split(':')
        port = int(port)
    else:
        port = 22
    return hostname, port


class SSHContext(object):

    def __init__(self,
                 streamflow_config_dir: str,
                 config: SSHConfig,
                 max_concurrent_sessions: int):
        self._streamflow_config_dir: str = streamflow_config_dir
        self._config: SSHConfig = config
        self._ssh_connection: Optional[SSHClientConnection] = None
        self._connection_lock: Lock = Lock()
        self._sem: Semaphore = Semaphore(max_concurrent_sessions)

    async def __aenter__(self):
        with await self._connection_lock:
            if self._ssh_connection is None:
                self._ssh_connection = await self._get_connection(self._config)
        await self._sem.acquire()
        return self._ssh_connection

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._sem.release()

    async def _get_connection(self,
                              config: SSHConfig):
        if config is None:
            return None
        (hostname, port) = _parse_hostname(config.hostname)
        passphrase = (self._get_param_from_file(config.ssh_key_passphrase_file)
                      if config.ssh_key_passphrase_file else None)
        password = self._get_param_from_file(config.password_file) if config.password_file else None
        return await asyncssh.connect(
            client_keys=config.client_keys,
            known_hosts=() if config.check_host_key else None,
            host=hostname,
            passphrase=passphrase,
            password=password,
            port=port,
            tunnel=await self._get_connection(config.tunnel),
            username=config.username)

    def _get_param_from_file(self,
                             file_path: str):
        if not os.path.isabs(file_path):
            file_path = os.path.join(self._streamflow_config_dir, file_path)
        with open(file_path) as f:
            return f.read().strip()


class SSHConfig(object):

    def __init__(self,
                 check_host_key: bool,
                 client_keys: MutableSequence[str],
                 hostname: str,
                 password_file: Optional[str],
                 ssh_key_passphrase_file: Optional[str],
                 tunnel: Optional[SSHConfig],
                 username: str):
        self.check_host_key: bool = check_host_key
        self.client_keys: MutableSequence[str] = client_keys
        self.hostname: str = hostname
        self.password_file: Optional[str] = password_file
        self.ssh_key_passphrase_file: Optional[str] = ssh_key_passphrase_file
        self.tunnel: Optional[SSHConfig] = tunnel
        self.username: str = username


class SSHConnector(BaseConnector):

    @staticmethod
    def _get_command(resource: str,
                     command: MutableSequence[str],
                     environment: MutableMapping[str, str] = None,
                     workdir: Optional[str] = None,
                     stdin: Optional[Union[int, str]] = None,
                     stdout: Union[int, str] = asyncio.subprocess.STDOUT,
                     stderr: Union[int, str] = asyncio.subprocess.STDOUT,
                     job_name: Optional[str] = None,
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
                 streamflow_config_dir: str,
                 nodes: MutableSequence[Any],
                 username: str,
                 checkHostKey: bool = True,
                 dataTransferConnection: Optional[Union[str, MutableMapping[str, Any]]] = None,
                 file: Optional[str] = None,
                 maxConcurrentSessions: int = 10,
                 passwordFile: Optional[str] = None,
                 sharedPaths: Optional[MutableSequence[str]] = None,
                 sshKey: Optional[str] = None,
                 sshKeyPassphraseFile: Optional[str] = None,
                 tunnel: Optional[MutableMapping[str, Any]] = None,
                 transferBufferSize: int = 2 ** 16) -> None:
        super().__init__(
            streamflow_config_dir=streamflow_config_dir,
            transferBufferSize=transferBufferSize)
        if file is not None:
            with open(os.path.join(streamflow_config_dir, file)) as f:
                self.template: Optional[Template] = Template(f.read())
        else:
            self.template: Optional[Template] = None
        self.checkHostKey: bool = checkHostKey
        self.passwordFile: Optional[str] = passwordFile
        self.maxConcurrentSessions: int = maxConcurrentSessions
        self.sharedPaths: MutableSequence[str] = sharedPaths or []
        self.sshKey: Optional[str] = sshKey
        self.sshKeyPassphraseFile: Optional[str] = sshKeyPassphraseFile
        self.ssh_contexts: MutableMapping[str, SSHContext] = {}
        self.data_transfer_contexts: MutableMapping[str, SSHContext] = {}
        self.username: str = username
        self.tunnel: Optional[SSHConfig] = self._get_config(tunnel)
        self.dataTransferConfig: Optional[SSHConfig] = self._get_config(
            dataTransferConnection)
        self.nodes: MutableMapping[str, SSHConfig] = {n.hostname: n for n in [self._get_config(n) for n in nodes]}
        self.hardwareCache: Cache = LRUCache(maxsize=len(self.nodes))

    async def _build_helper_file(self,
                                 command: str,
                                 resource: str,
                                 environment: MutableMapping[str, str] = None,
                                 workdir: str = None) -> str:
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
                                    src: str,
                                    dst: str,
                                    resources: MutableSequence[str],
                                    read_only: bool = False):
        await asyncio.gather(*(asyncio.create_task(
            self._scp(src=src, dst=dst, resource=resource, kind=ConnectorCopyKind.LOCAL_TO_REMOTE)
        ) for resource in resources))

    async def _copy_remote_to_local(self,
                                    src: str,
                                    dst: str,
                                    resource: str,
                                    read_only: bool = False) -> None:
        await self._scp(src=src, dst=dst, resource=resource, kind=ConnectorCopyKind.REMOTE_TO_LOCAL)

    def _get_config(self,
                    node: Union[str, MutableMapping[str, Any]]):
        if node is None:
            return None
        elif isinstance(node, str):
            node = {'hostname': node}
        ssh_key = node['sshKey'] if 'sshKey' in node else self.sshKey
        return SSHConfig(
            hostname=node['hostname'],
            username=node['username'] if 'username' in node else self.username,
            check_host_key=node['checkHostKey'] if 'checkHostKey' in node else self.checkHostKey,
            client_keys=[ssh_key] if ssh_key is not None else [],
            password_file=node['passwordFile'] if 'passwordFile' in node else self.passwordFile,
            ssh_key_passphrase_file=(node['sshKeyPassphraseFile'] if 'sshKeyPassphraseFile' in node else
                                     self.sshKeyPassphraseFile),
            tunnel=(self._get_config(node['tunnel']) if 'tunnel' in node else
                    self.tunnel if hasattr(self, 'tunnel') else
                    None))

    def _get_data_transfer_client(self, resource: str):
        if self.dataTransferConfig:
            if resource not in self.data_transfer_contexts:
                self.data_transfer_contexts[resource] = SSHContext(
                    streamflow_config_dir=self.streamflow_config_dir,
                    config=self.dataTransferConfig,
                    max_concurrent_sessions=self.maxConcurrentSessions)
            return self.data_transfer_contexts[resource]
        else:
            return self._get_ssh_client(resource)

    def _get_run_command(self,
                         command: str,
                         resource: str,
                         interactive: bool = False):
        return "".join([
            "ssh ",
            "{resource} ",
            "{command}"
        ]).format(
            resource=resource,
            command=command)

    def _get_ssh_client(self, resource: str):
        if resource not in self.ssh_contexts:
            self.ssh_contexts[resource] = SSHContext(
                streamflow_config_dir=self.streamflow_config_dir,
                config=self.nodes[resource],
                max_concurrent_sessions=self.maxConcurrentSessions)
        return self.ssh_contexts[resource]

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

    async def _scp(self,
                   src: str,
                   dst: str,
                   resource: str,
                   kind: ConnectorCopyKind):
        async with self._get_data_transfer_client(resource) as ssh_client:
            if kind == ConnectorCopyKind.REMOTE_TO_LOCAL:
                await asyncssh.scp((ssh_client, src), dst, preserve=True, recurse=True)
            elif kind == ConnectorCopyKind.LOCAL_TO_REMOTE:
                await asyncssh.scp(src, (ssh_client, dst), preserve=True, recurse=True)

    @cachedmethod(lambda self: self.hardwareCache)
    async def _get_resource_hardware(self, resource: str) -> Hardware:
        async with self._get_ssh_client(resource) as ssh_client:
            cores, memory, disk = await asyncio.gather(
                ssh_client.run("nproc", stderr=STDOUT),
                ssh_client.run("free | grep Mem | awk '{print $2}'", stderr=STDOUT),
                ssh_client.run("df / | tail -n 1 | awk '{print $2}'", stderr=STDOUT))
            if cores.returncode == 0 and memory.returncode == 0 and disk.returncode == 0:
                return Hardware(
                    cores=float(cores.stdout.strip()),
                    memory=float(memory.stdout.strip()) / 2 ** 10,
                    disk=float(disk.stdout.strip()) / 2 ** 10)
            else:
                raise WorkflowExecutionException(
                    "Impossible to retrieve resources for {resource}".format(resource=resource))

    async def deploy(self, external: bool) -> None:
        pass

    async def get_available_resources(self, service: str) -> MutableMapping[str, Resource]:
        resources = {}
        for resource_obj in self.nodes.values():
            hardware = await self._get_resource_hardware(resource_obj.hostname)
            resources[resource_obj.hostname] = Resource(
                name=resource_obj.hostname,
                hostname=resource_obj.hostname,
                hardware=hardware)
            logger.debug(
                "Resource {resource} available with {cores} cores, {mem}MiB memory and {disk}MiB disk space".format(
                    resource=resource_obj.hostname,
                    cores=hardware.cores,
                    mem=hardware.memory,
                    disk=hardware.disk))
        return resources

    async def undeploy(self, external: bool) -> None:
        for ssh_context in self.ssh_contexts.values():
            async with ssh_context as ssh_client:
                ssh_client.close()
        self.ssh_contexts = {}
        for ssh_context in self.data_transfer_contexts.values():
            async with ssh_context as ssh_client:
                ssh_client.close()
        self.data_transfer_contexts = {}
