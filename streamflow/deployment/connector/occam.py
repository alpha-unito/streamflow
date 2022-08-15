import asyncio
import os
import posixpath
import re
from typing import Any, MutableMapping, MutableSequence, Optional, Tuple, Union

import asyncssh
import pkg_resources
from ruamel.yaml import YAML

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.scheduling import Location
from streamflow.deployment.connector.ssh import SSHConnector
from streamflow.log_handler import logger


class OccamConnector(SSHConnector):

    def __init__(self,
                 deployment_name: str,
                 context: StreamFlowContext,
                 file: str,
                 sshKey: str,
                 username: str,
                 sshKeyPassphraseFile: Optional[str] = None,
                 hostname: Optional[str] = "occam.c3s.unito.it",
                 transferBufferSize: int = 2 ** 16) -> None:
        super().__init__(
            deployment_name=deployment_name,
            context=context,
            nodes=[hostname],
            sshKey=sshKey,
            sshKeyPassphraseFile=sshKeyPassphraseFile,
            sharedPaths=[
                '/archive/home/{username}'.format(username=username),
                '/scratch/home/{username}'.format(username=username)],
            transferBufferSize=transferBufferSize,
            username=username)
        with open(os.path.join(context.config_dir, file)) as f:
            yaml = YAML(typ='safe')
            self.env_description = yaml.load(f)
        self.jobs_table: MutableMapping[str, MutableSequence[str]] = {}

    def _get_effective_locations(self,
                                 locations: MutableSequence[str],
                                 dest_path: str,
                                 source_location: Optional[str] = None) -> MutableSequence[str]:
        # If destination path is in a shared location, transfer only on the first location
        for shared_path in self.sharedPaths:
            if dest_path.startswith(shared_path):
                if source_location in locations:
                    return [source_location]
                else:
                    return [locations[0]]
        # Otherwise, check if some locations share bind mounts to the same shared local path
        common_paths = {}
        effective_locations = []
        for location in locations:
            shared_path = self._get_shared_path(location, dest_path)
            if shared_path is not None:
                if shared_path not in common_paths:
                    common_paths[shared_path] = location
                    effective_locations.append(location)
                elif location == source_location:
                    effective_locations.remove(common_paths[shared_path])
                    common_paths[shared_path] = location
                    effective_locations.append(location)
            else:
                effective_locations.append(location)

        return effective_locations

    def _get_shared_path(self, location: str, path: str) -> Optional[str]:
        for shared_path in self.sharedPaths:
            if path.startswith(shared_path):
                return path
        volumes = self._get_volumes(location)
        for volume in volumes:
            local, remote = volume.split(':')
            if path.startswith(remote):
                return posixpath.normpath(posixpath.join(local, posixpath.relpath(path, remote)))
        return None

    async def _get_tmpdir(self, location: str):
        scratch_home = '/scratch/home/{username}'.format(username=self.username)
        temp_dir = posixpath.join(scratch_home, 'streamflow', "".join(utils.random_name()))
        async with self._get_ssh_client(location) as ssh_client:
            await ssh_client.run('mkdir -p {dir}'.format(dir=temp_dir))
        return temp_dir

    def _get_volumes(self, location: str) -> MutableSequence[str]:
        for name in self.jobs_table:
            if location in self.jobs_table[name]:
                service = name
                return self.env_description[service].get('volumes', [])

    async def _copy_remote_to_remote(self,
                                     src: str,
                                     dst: str,
                                     locations: MutableSequence[str],
                                     source_location: str,
                                     read_only: bool = False) -> None:
        effective_locations = self._get_effective_locations(locations, dst, source_location)
        # Check for the need of a temporary copy
        temp_dir = None
        for location in effective_locations:
            if source_location != location:
                temp_dir = await self._get_tmpdir(location)
                copy1_command = ['/bin/cp', '-rf', src, temp_dir]
                await self.run(source_location, copy1_command)
                break
        # Perform the actual copies
        await asyncio.gather(*(asyncio.create_task(
            self._copy_remote_to_remote_single(
                src=src,
                dst=dst,
                location=location,
                source_location=source_location,
                temp_dir=temp_dir,
                read_only=read_only)
        ) for location in effective_locations))
        # If a temporary location was created, delete it
        if temp_dir is not None:
            for location in effective_locations:
                async with self._get_ssh_client(location) as ssh_client:
                    await ssh_client.run('rm -rf {dir}'.format(dir=temp_dir))

    async def _copy_remote_to_remote_single(self,
                                            src: str,
                                            dst: str,
                                            location: str,
                                            source_location: str,
                                            temp_dir: Optional[str],
                                            read_only: bool = False) -> None:
        if source_location == location:
            command = ['/bin/cp', "-rf", src, dst]
            await self.run(location, command)
        else:
            copy2_command = ['/bin/cp', '-rf', temp_dir + "/*", dst]
            await self.run(location, copy2_command)

    async def _copy_local_to_remote(self,
                                    src: str,
                                    dst: str,
                                    locations: MutableSequence[str],
                                    read_only: bool = False) -> None:
        effective_locations = self._get_effective_locations(locations, dst)
        # Check for the need of a temporary copy
        temp_dir = None
        for location in effective_locations:
            shared_path = self._get_shared_path(location, dst)
            if shared_path is None:
                temp_dir = await self._get_tmpdir(location)
                async with self._get_ssh_client(location) as ssh_client:
                    await asyncssh.scp(src, (ssh_client, temp_dir), preserve=True, recurse=True)
                break
        # Perform the actual copies
        copy_tasks = []
        for location in effective_locations:
            copy_tasks.append(asyncio.create_task(
                self._copy_local_to_remote_single(
                    src=src,
                    dst=dst,
                    location=location,
                    temp_dir=temp_dir,
                    read_only=read_only)))
        await asyncio.gather(*copy_tasks)
        # If a temporary location was created, delete it
        if temp_dir is not None:
            for location in effective_locations:
                async with self._get_ssh_client(location) as ssh_client:
                    await ssh_client.run('rm -rf {dir}'.format(dir=temp_dir))

    async def _copy_local_to_remote_single(self,
                                           src: str,
                                           dst: str,
                                           location: str,
                                           temp_dir: Optional[str],
                                           read_only: bool = False) -> None:
        shared_path = self._get_shared_path(location, dst)
        if shared_path is not None:
            async with self._get_ssh_client(location) as ssh_client:
                await asyncssh.scp(src, (ssh_client, shared_path), preserve=True, recurse=True)
        else:
            copy_command = ['/bin/cp', "-rf", temp_dir + "/*", dst]
            await self.run(location, copy_command)

    async def _copy_remote_to_local(self,
                                    src: str,
                                    dst: str,
                                    location: str,
                                    read_only: bool = False) -> None:
        shared_path = self._get_shared_path(location, src)
        if shared_path is not None:
            async with self._get_ssh_client(location) as ssh_client:
                await asyncssh.scp((ssh_client, shared_path), dst, preserve=True, recurse=True)
        else:
            temp_dir = await self._get_tmpdir(location)
            copy_command = ['/bin/cp', "-rf", src, temp_dir, '&&',
                            'find', temp_dir, '-maxdepth', '1', '-mindepth', '1', '-exec', 'basename', '{}', '\\;']
            contents, _ = await self.run(location, copy_command, capture_output=True)
            contents = contents.split()
            scp_tasks = []
            async with self._get_ssh_client(location) as ssh_client:
                for content in contents:
                    scp_tasks.append(asyncio.create_task(asyncssh.scp(
                        (ssh_client, posixpath.join(temp_dir, content)),
                        dst,
                        preserve=True,
                        recurse=True
                    )))
                await asyncio.gather(*scp_tasks)
            delete_command = ['rm', '-rf', temp_dir]
            await self.run(location, delete_command)

    async def _deploy_node(self, name: str, service: MutableMapping[str, Any], node: str):
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
            logger.info("Deployed {name} on {location}".format(name=name, location=search_result[0]))
        else:
            raise Exception

    async def _undeploy_node(self, name: str, job_id: str):
        undeploy_command = "".join([
            "occam-kill ",
            "{job_id}"
        ]).format(
            job_id=job_id
        )
        logger.debug("Executing {command}".format(command=undeploy_command))
        async with self._get_ssh_client(name) as ssh_client:
            await ssh_client.run(undeploy_command)
        logger.info("Killed {location}".format(location=job_id))

    async def deploy(self, external: bool) -> None:
        await super().deploy(external)
        if not external:
            deploy_tasks = []
            for (name, service) in self.env_description.items():
                nodes = service.get('nodes', ['node22'])
                for node in nodes:
                    deploy_tasks.append(asyncio.create_task(self._deploy_node(name, service, node)))
            await asyncio.gather(*deploy_tasks)

    async def get_available_locations(self,
                                      service: str,
                                      input_directory: Optional[str] = None,
                                      output_directory: Optional[str] = None,
                                      tmp_directory: Optional[str] = None) -> MutableMapping[str, Location]:
        nodes = self.jobs_table.get(service, []) if service else utils.flatten_list(self.jobs_table.values())
        return {n: Location(name=n, hostname=n.split('-')[0]) for n in nodes}

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join('schemas', 'occam.json'))

    async def undeploy(self, external: bool) -> None:
        if not external:
            # Undeploy
            undeploy_tasks = []
            for name in self.jobs_table:
                for job_id in self.jobs_table[name]:
                    undeploy_tasks.append(asyncio.create_task(self._undeploy_node(name, job_id)))
            await asyncio.gather(*undeploy_tasks)
            self.jobs_table = {}
        # Close connection
        await super().undeploy(external)

    async def run(self,
                  location: str,
                  command: MutableSequence[str],
                  environment: MutableMapping[str, str] = None,
                  workdir: Optional[str] = None,
                  stdin: Optional[Union[int, str]] = None,
                  stdout: Union[int, str] = asyncio.subprocess.STDOUT,
                  stderr: Union[int, str] = asyncio.subprocess.STDOUT,
                  capture_output: bool = False,
                  job_name: Optional[str] = None) -> Optional[Tuple[Optional[Any], int]]:
        command = self._get_command(
            location=location,
            command=command,
            environment=environment,
            workdir=workdir,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            job_name=job_name)
        occam_command = "".join(
            "occam-exec "
            "{location} "
            "sh -c '{command}'"
        ).format(
            location=location,
            command=command)
        async with self._get_ssh_client(location) as ssh_client:
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
