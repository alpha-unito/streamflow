import asyncio
import base64
import os
import posixpath
import re
from typing import List, MutableMapping, Optional, Any, Tuple

import asyncssh
from ruamel.yaml import YAML
from typing_extensions import Text

from streamflow.deployment.base import BaseConnector
from streamflow.core import utils
from streamflow.core.scheduling import Resource
from streamflow.log_handler import logger


def _parse_hostname(hostname):
    if ':' in hostname:
        hostname, port = hostname.split(':')
        port = int(port)
    else:
        port = 22
    return hostname, port


class OccamConnector(BaseConnector):

    def __init__(self,
                 streamflow_config_dir: Text,
                 file: Text,
                 sshKey: Text,
                 username: Text,
                 sshKeyPassphrase: Optional[Text] = None,
                 hostname: Optional[Text] = "occam.c3s.unito.it",
                 socketTimeout: Optional[float] = 60.0
                 ) -> None:
        super().__init__(streamflow_config_dir)
        yaml = YAML(typ='safe')
        with open(os.path.join(streamflow_config_dir, file)) as f:
            self.env_description = yaml.load(f)
        self.hostname = hostname
        self.sshKey = sshKey
        self.sshKeyPassphrase = sshKeyPassphrase
        self.username = username
        self.socketTimeout = socketTimeout
        self.archive_home = '/archive/home/{username}'.format(username=username)
        self.scratch_home = '/scratch/home/{username}'.format(username=username)
        self.jobs_table = {}
        self.ssh_client = None

    def _get_effective_resources(self,
                                 resources: List[Text],
                                 dest_path: Text,
                                 source_remote: Optional[Text] = None) -> List[Text]:
        # If destination path starts with /archive or /scrath, transfer only on the first resource
        if dest_path.startswith(self.archive_home) or dest_path.startswith(self.scratch_home):
            if source_remote in resources:
                return [source_remote]
            else:
                return [resources[0]]
        # Otherwise, check if some resources share bind mounts to the same persistent local path
        else:
            common_paths = {}
            effective_resources = []
            for resource in resources:
                persistent_path = self._get_persistent_path(resource, dest_path)
                if persistent_path is not None:
                    if persistent_path not in common_paths:
                        common_paths[persistent_path] = resource
                        effective_resources.append(resource)
                    elif resource == source_remote:
                        effective_resources.remove(common_paths[persistent_path])
                        common_paths[persistent_path] = resource
                        effective_resources.append(resource)
                else:
                    effective_resources.append(resource)

            return effective_resources

    async def _get_ssh_client(self):
        if self.ssh_client is None:
            (hostname, port) = _parse_hostname(self.hostname)
            self.ssh_client = await asyncssh.connect(
                host=hostname,
                port=port,
                username=self.username,
                client_keys=[self.sshKey],
                passphrase=self.sshKeyPassphrase
            )
        return self.ssh_client

    async def _get_tmpdir(self):
        temp_dir = posixpath.join(self.scratch_home, 'streamflow', "".join(utils.random_name()))
        ssh_client = await self._get_ssh_client()
        await ssh_client.run('mkdir -p {dir}'.format(dir=temp_dir))
        return temp_dir

    def _get_volumes(self, resource: Text) -> List[Text]:
        for name in self.jobs_table:
            if resource in self.jobs_table[name]:
                service = name
                return self.env_description[service].get('volumes', [])

    def _get_persistent_path(self, resource: Text, path: Text) -> Optional[Text]:
        if path.startswith(self.archive_home) or path.startswith(self.scratch_home):
            return path
        volumes = self._get_volumes(resource)
        for volume in volumes:
            local, remote = volume.split(':')
            if path.startswith(remote):
                return posixpath.normpath(posixpath.join(local, posixpath.relpath(path, remote)))
        return None

    async def _copy_remote_to_remote(self, src: Text, dst: Text, resources: List[Text], source_remote: Text) -> None:
        ssh_client = await self._get_ssh_client()
        effective_resources = self._get_effective_resources(resources, dst, source_remote)
        # Check for the need of a temporary copy
        temp_dir = None
        for resource in effective_resources:
            if source_remote != resource:
                temp_dir = await self._get_tmpdir()
                copy1_command = ['/bin/cp', '-rf', src, temp_dir]
                await self.run(source_remote, copy1_command)
                break
        # Perform the actual copies
        copy_tasks = []
        for resource in effective_resources:
            copy_tasks.append(asyncio.create_task(
                self._copy_remote_to_remote_single(src, dst, resource, source_remote, temp_dir)))
        await asyncio.gather(*copy_tasks)
        # If a temporary location was created, delete it
        if temp_dir is not None:
            await ssh_client.run('rm -rf {dir}'.format(dir=temp_dir))

    async def _copy_remote_to_remote_single(self,
                                            src: Text,
                                            dst: Text,
                                            resource: Text,
                                            source_remote: Text,
                                            temp_dir: Optional[Text]) -> None:
        if source_remote == resource:
            command = ['/bin/cp', "-rf", src, dst]
            await self.run(resource, command)
        else:
            copy2_command = ['/bin/cp', '-rf', temp_dir + "/*", dst]
            await self.run(resource, copy2_command)

    async def _copy_local_to_remote(self, src: Text, dst: Text, resources: List[Text]):
        ssh_client = await self._get_ssh_client()
        effective_resources = self._get_effective_resources(resources, dst)
        # Check for the need of a temporary copy
        temp_dir = None
        for resource in effective_resources:
            persistent_path = self._get_persistent_path(resource, dst)
            if persistent_path is None:
                temp_dir = await self._get_tmpdir()
                await asyncssh.scp(src, (ssh_client, temp_dir), preserve=True, recurse=True)
                break
        # Perform the actual copies
        copy_tasks = []
        for resource in effective_resources:
            copy_tasks.append(asyncio.create_task(
                self._copy_local_to_remote_single(src, dst, resource, temp_dir)))
        await asyncio.gather(*copy_tasks)
        # If a temporary location was created, delete it
        if temp_dir is not None:
            await ssh_client.run('rm -rf {dir}'.format(dir=temp_dir))

    async def _copy_local_to_remote_single(self, src: Text, dst: Text, resource: Text, temp_dir: Optional[Text]):
        ssh_client = await self._get_ssh_client()
        persistent_path = self._get_persistent_path(resource, dst)
        if persistent_path is not None:
            await asyncssh.scp(src, (ssh_client, persistent_path), preserve=True, recurse=True)
        else:
            copy_command = ['/bin/cp', "-rf", temp_dir + "/*", dst]
            await self.run(resource, copy_command)

    async def _copy_remote_to_local(self, src: Text, dst: Text, resource: Text):
        persistent_path = self._get_persistent_path(resource, src)
        ssh_client = await self._get_ssh_client()
        if persistent_path is not None:
            await asyncssh.scp((ssh_client, persistent_path), dst, preserve=True, recurse=True)
        else:
            temp_dir = await self._get_tmpdir()
            copy_command = ['/bin/cp', "-rf", src, temp_dir, '&&',
                            'find', temp_dir, '-maxdepth', '1', '-mindepth', '1', '-exec', 'basename', '{}', '\\;']
            contents, _ = await self.run(resource, copy_command, capture_output=True)
            contents = contents.split()
            scp_tasks = []
            for content in contents:
                scp_tasks.append(asyncio.create_task(asyncssh.scp(
                    (ssh_client, posixpath.join(temp_dir, content)),
                    dst,
                    preserve=True,
                    recurse=True
                )))
            await asyncio.gather(*scp_tasks)
            delete_command = ['rm', '-rf', temp_dir]
            await self.run(resource, delete_command)

    async def _deploy_node(self, name: Text, service: MutableMapping[Text, Any], node: Text):
        deploy_command = "".join([
            "{workdir}"
            "occam-run ",
            "{x11}",
            "{node}",
            "{stdin}"
            "{jobidFile}"
            "{shmSize}"
            "{volumes}"
            "{image} "
            "{command}"
        ]).format(
            workdir="cd {workdir} && ".format(workdir=service.get('workdir')) if 'workdir' in service else "",
            x11=self.get_option("x", service.get('x11')),
            node=self.get_option("n", node),
            stdin=self.get_option("i", service.get('stdin')),
            jobidFile=self.get_option("c", service.get('jobidFile')),
            shmSize=self.get_option("s", service.get('shmSize')),
            volumes=self.get_option("v", service.get('volumes')),
            image=service['image'],
            command=" ".join(service.get('command')) if 'command' in service else ""
        )
        logger.debug("Executing {command}".format(command=deploy_command))
        ssh_client = await self._get_ssh_client()
        result = await ssh_client.run(deploy_command)
        output = result.stdout
        search_result = re.findall('({node}-[0-9]+).*'.format(node=node), output, re.MULTILINE)
        if search_result:
            if name not in self.jobs_table:
                self.jobs_table[name] = []
            self.jobs_table[name].append(search_result[0])
            logger.info("Deployed {name} on {resource}".format(name=name, resource=search_result[0]))
        else:
            raise Exception

    async def _undeploy_node(self, job_id: Text):
        undeploy_command = "".join([
            "occam-kill ",
            "{job_id}"
        ]).format(
            job_id=job_id
        )
        logger.debug("Executing {command}".format(command=undeploy_command))
        ssh_client = await self._get_ssh_client()
        await ssh_client.run(undeploy_command)
        logger.info("Killed {resource}".format(resource=job_id))

    async def deploy(self) -> None:
        deploy_tasks = []
        for (name, service) in self.env_description.items():
            nodes = service.get('nodes', ['node22'])
            for node in nodes:
                deploy_tasks.append(asyncio.create_task(self._deploy_node(name, service, node)))
        await asyncio.gather(*deploy_tasks)

    async def get_available_resources(self, service: str) -> MutableMapping[Text, Resource]:
        resources = {}
        for node in self.jobs_table.get(service, []):
            resources[node] = Resource(name=node, hostname=node.split('-')[0])
        return resources

    async def undeploy(self) -> None:
        # Undeploy models
        undeploy_tasks = []
        for name in self.jobs_table:
            for job_id in self.jobs_table[name]:
                undeploy_tasks.append(asyncio.create_task(self._undeploy_node(job_id)))
        await asyncio.gather(*undeploy_tasks)
        # Close connection
        if self.ssh_client is not None:
            self.ssh_client.close()
            self.ssh_client = None

    async def run(self,
                  resource: str, command: List[str],
                  environment: MutableMapping[str, str] = None,
                  workdir: str = None,
                  capture_output: bool = False) -> Optional[Tuple[Optional[Any], int]]:
        exec_command = "".join(
            "{workdir}"
            "{environment}"
            "{command}"
        ).format(
            resource=resource,
            workdir="cd {workdir} && ".format(workdir=workdir) if workdir is not None else "",
            environment="".join(["export %s=%s && " % (key, value) for (key, value) in
                                 environment.items()]) if environment is not None else "",
            command=" ".join(command)
        )
        logger.debug("Executing {command} on {resource}".format(command=exec_command, resource=resource))
        occam_command = "".join(
            "occam-exec "
            "{resource} "
            "sh -c "
            "\"$(echo {command} | base64 -d)\""
        ).format(
            resource=resource,
            command=base64.b64encode(exec_command.encode('utf-8')).decode('utf-8')
        )
        ssh_client = await self._get_ssh_client()
        result = await ssh_client.run(occam_command)
        if capture_output:
            lines = (line for line in result.stdout.split('\n'))
            out = ""
            for line in lines:
                if line.startswith("Trying to exec commands into container"):
                    break
            try:
                line = next(lines)
                out = line.strip(' \r\t')
            except StopIteration:
                return out
            for line in lines:
                out = "\n".join([out, line.strip(' \r\t')])
            return out, result.returncode
