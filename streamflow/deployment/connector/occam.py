import asyncio
import os
import posixpath
import re
from typing import MutableSequence, MutableMapping, Optional, Any, Tuple, Union

import asyncssh
from ruamel.yaml import YAML
from typing_extensions import Text

from streamflow.core import utils
from streamflow.core.scheduling import Resource
from streamflow.deployment.connector.ssh import SSHConnector
from streamflow.log_handler import logger


class OccamConnector(SSHConnector):

    def __init__(self,
                 streamflow_config_dir: Text,
                 file: Text,
                 sshKey: Text,
                 username: Text,
                 sshKeyPassphrase: Optional[Text] = None,
                 hostname: Optional[Text] = "occam.c3s.unito.it",
                 transferBufferSize: int = 2**16) -> None:
        super().__init__(
            streamflow_config_dir=streamflow_config_dir,
            hostname=hostname,
            sshKey=sshKey,
            sshKeyPassphrase=sshKeyPassphrase,
            transferBufferSize=transferBufferSize,
            username=username)
        with open(os.path.join(streamflow_config_dir, file)) as f:
            yaml = YAML(typ='safe')
            self.env_description = yaml.load(f)
        self.sharedPaths = [
            '/archive/home/{username}'.format(username=username),
            '/scratch/home/{username}'.format(username=username)
        ]

    def _get_effective_resources(self,
                                 resources: MutableSequence[Text],
                                 dest_path: Text,
                                 source_remote: Optional[Text] = None) -> MutableSequence[Text]:
        # If destination path is in a shared location, transfer only on the first resource
        for shared_path in self.sharedPaths:
            if dest_path.startswith(shared_path):
                if source_remote in resources:
                    return [source_remote]
                else:
                    return [resources[0]]
        # Otherwise, check if some resources share bind mounts to the same shared local path
        common_paths = {}
        effective_resources = []
        for resource in resources:
            shared_path = self._get_shared_path(resource, dest_path)
            if shared_path is not None:
                if shared_path not in common_paths:
                    common_paths[shared_path] = resource
                    effective_resources.append(resource)
                elif resource == source_remote:
                    effective_resources.remove(common_paths[shared_path])
                    common_paths[shared_path] = resource
                    effective_resources.append(resource)
            else:
                effective_resources.append(resource)

        return effective_resources

    def _get_shared_path(self, resource: Text, path: Text) -> Optional[Text]:
        for shared_path in self.sharedPaths:
            if path.startswith(shared_path):
                return path
        volumes = self._get_volumes(resource)
        for volume in volumes:
            local, remote = volume.split(':')
            if path.startswith(remote):
                return posixpath.normpath(posixpath.join(local, posixpath.relpath(path, remote)))
        return None

    async def _get_tmpdir(self, resource: Text):
        scratch_home = '/scratch/home/{username}'.format(username=self.username)
        temp_dir = posixpath.join(scratch_home, 'streamflow', "".join(utils.random_name()))
        async with self._get_ssh_client(resource) as ssh_client:
            await ssh_client.run('mkdir -p {dir}'.format(dir=temp_dir))
        return temp_dir

    def _get_volumes(self, resource: Text) -> MutableSequence[Text]:
        for name in self.jobs_table:
            if resource in self.jobs_table[name]:
                service = name
                return self.env_description[service].get('volumes', [])

    async def _copy_remote_to_remote(self,
                                     src: Text,
                                     dst: Text,
                                     resources: MutableSequence[Text],
                                     source_remote: Text,
                                     read_only: bool = False) -> None:
        effective_resources = self._get_effective_resources(resources, dst, source_remote)
        # Check for the need of a temporary copy
        temp_dir = None
        for resource in effective_resources:
            if source_remote != resource:
                temp_dir = await self._get_tmpdir(resource)
                copy1_command = ['/bin/cp', '-rf', src, temp_dir]
                await self.run(source_remote, copy1_command)
                break
        # Perform the actual copies
        await asyncio.gather(*[asyncio.create_task(
            self._copy_remote_to_remote_single(
                src=src,
                dst=dst,
                resource=resource,
                source_remote=source_remote,
                temp_dir=temp_dir,
                read_only=read_only)
        ) for resource in effective_resources])
        # If a temporary location was created, delete it
        if temp_dir is not None:
            for resource in effective_resources:
                async with self._get_ssh_client(resource) as ssh_client:
                    await ssh_client.run('rm -rf {dir}'.format(dir=temp_dir))

    async def _copy_remote_to_remote_single(self,
                                            src: Text,
                                            dst: Text,
                                            resource: Text,
                                            source_remote: Text,
                                            temp_dir: Optional[Text],
                                            read_only: bool = False) -> None:
        if source_remote == resource:
            command = ['/bin/cp', "-rf", src, dst]
            await self.run(resource, command)
        else:
            copy2_command = ['/bin/cp', '-rf', temp_dir + "/*", dst]
            await self.run(resource, copy2_command)

    async def _copy_local_to_remote(self,
                                    src: Text,
                                    dst: Text,
                                    resources: MutableSequence[Text],
                                    read_only: bool = False) -> None:
        effective_resources = self._get_effective_resources(resources, dst)
        # Check for the need of a temporary copy
        temp_dir = None
        for resource in effective_resources:
            shared_path = self._get_shared_path(resource, dst)
            if shared_path is None:
                temp_dir = await self._get_tmpdir(resource)
                async with self._get_ssh_client(resource) as ssh_client:
                    await asyncssh.scp(src, (ssh_client, temp_dir), preserve=True, recurse=True)
                break
        # Perform the actual copies
        copy_tasks = []
        for resource in effective_resources:
            copy_tasks.append(asyncio.create_task(
                self._copy_local_to_remote_single(
                    src=src,
                    dst=dst,
                    resource=resource,
                    temp_dir=temp_dir,
                    read_only=read_only)))
        await asyncio.gather(*copy_tasks)
        # If a temporary location was created, delete it
        if temp_dir is not None:
            for resource in effective_resources:
                async with self._get_ssh_client(resource) as ssh_client:
                    await ssh_client.run('rm -rf {dir}'.format(dir=temp_dir))

    async def _copy_local_to_remote_single(self,
                                           src: Text,
                                           dst: Text,
                                           resource: Text,
                                           temp_dir: Optional[Text],
                                           read_only: bool = False) -> None:
        shared_path = self._get_shared_path(resource, dst)
        if shared_path is not None:
            async with self._get_ssh_client(resource) as ssh_client:
                await asyncssh.scp(src, (ssh_client, shared_path), preserve=True, recurse=True)
        else:
            copy_command = ['/bin/cp', "-rf", temp_dir + "/*", dst]
            await self.run(resource, copy_command)

    async def _copy_remote_to_local(self,
                                    src: Text,
                                    dst: Text,
                                    resource: Text,
                                    read_only: bool = False) -> None:
        shared_path = self._get_shared_path(resource, src)
        if shared_path is not None:
            async with self._get_ssh_client(resource) as ssh_client:
                await asyncssh.scp((ssh_client, shared_path), dst, preserve=True, recurse=True)
        else:
            temp_dir = await self._get_tmpdir(resource)
            copy_command = ['/bin/cp', "-rf", src, temp_dir, '&&',
                            'find', temp_dir, '-maxdepth', '1', '-mindepth', '1', '-exec', 'basename', '{}', '\\;']
            contents, _ = await self.run(resource, copy_command, capture_output=True)
            contents = contents.split()
            scp_tasks = []
            async with self._get_ssh_client(resource) as ssh_client:
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
            command=" ".join(service.get('command', ""))
        )
        logger.debug("Executing {command}".format(command=deploy_command))
        async with self._get_ssh_client(name) as ssh_client:
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

    async def _undeploy_node(self, name: Text, job_id: Text):
        undeploy_command = "".join([
            "occam-kill ",
            "{job_id}"
        ]).format(
            job_id=job_id
        )
        logger.debug("Executing {command}".format(command=undeploy_command))
        async with self._get_ssh_client(name) as ssh_client:
            await ssh_client.run(undeploy_command)
        logger.info("Killed {resource}".format(resource=job_id))

    async def deploy(self, external: bool) -> None:
        await super().deploy(external)
        if not external:
            deploy_tasks = []
            for (name, service) in self.env_description.items():
                nodes = service.get('nodes', ['node22'])
                for node in nodes:
                    deploy_tasks.append(asyncio.create_task(self._deploy_node(name, service, node)))
            await asyncio.gather(*deploy_tasks)

    async def get_available_resources(self, service: Text) -> MutableMapping[Text, Resource]:
        nodes = self.jobs_table.get(service, []) if service else utils.flatten_list(self.jobs_table.values())
        return {n: Resource(name=n, hostname=n.split('-')[0]) for n in nodes}

    async def undeploy(self, external: bool) -> None:
        if not external:
            # Undeploy models
            undeploy_tasks = []
            for name in self.jobs_table:
                for job_id in self.jobs_table[name]:
                    undeploy_tasks.append(asyncio.create_task(self._undeploy_node(name, job_id)))
            await asyncio.gather(*undeploy_tasks)
            self.jobs_table = {}
        # Close connection
        await super().undeploy(external)

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
        occam_command = "".join(
            "occam-exec "
            "{resource} "
            "sh -c '{command}'"
        ).format(
            resource=resource,
            command=command)
        async with self._get_ssh_client(resource) as ssh_client:
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
