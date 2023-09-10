from __future__ import annotations

import asyncio
import logging
import os
import posixpath
import re
from typing import Any, MutableMapping, MutableSequence

import asyncssh
import pkg_resources
from ruamel.yaml import YAML

from streamflow.core import utils
from streamflow.core.deployment import Location
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.scheduling import AvailableLocation
from streamflow.core.utils import get_option
from streamflow.deployment.connector.ssh import SSHConnector
from streamflow.log_handler import logger


class OccamConnector(SSHConnector):
    def __init__(
        self,
        deployment_name: str,
        config_dir: str,
        file: str,
        sshKey: str,
        username: str,
        sshKeyPassphraseFile: str | None = None,
        hostname: str | None = "occam.c3s.unito.it",
        transferBufferSize: int = 2**16,
    ) -> None:
        super().__init__(
            deployment_name=deployment_name,
            config_dir=config_dir,
            nodes=[hostname],
            sshKey=sshKey,
            sshKeyPassphraseFile=sshKeyPassphraseFile,
            sharedPaths=[
                f"/archive/home/{username}",
                f"/scratch/home/{username}",
            ],
            transferBufferSize=transferBufferSize,
            username=username,
        )
        with open(os.path.join(config_dir, file)) as f:
            yaml = YAML(typ="safe")
            self.env_description = yaml.load(f)
        self.jobs_table: MutableMapping[str, MutableSequence[str]] = {}

    def _get_effective_locations(
        self,
        locations: MutableSequence[Location],
        dest_path: str,
        source_location: Location,
    ) -> MutableSequence[Location]:
        # If destination path is in a shared location, transfer only on the first location
        for shared_path in self.sharedPaths:
            if dest_path.startswith(shared_path):
                if source_location.name in [loc.name for loc in locations]:
                    return [source_location]
                else:
                    return [locations[0]]
        # Otherwise, check if some locations share bind mounts to the same shared local path
        common_paths = {}
        effective_locations = []
        for location in locations:
            shared_path = self._get_shared_path(location.name, dest_path)
            if shared_path is not None:
                if shared_path not in common_paths:
                    common_paths[shared_path] = location
                    effective_locations.append(location)
                elif location.name == source_location.name:
                    effective_locations.remove(common_paths[shared_path])
                    common_paths[shared_path] = location
                    effective_locations.append(location)
            else:
                effective_locations.append(location)

        return effective_locations

    def _get_shared_path(self, location: str, path: str) -> str | None:
        for shared_path in self.sharedPaths:
            if path.startswith(shared_path):
                return path
        volumes = self._get_volumes(location)
        for volume in volumes:
            local, remote = volume.split(":")
            if path.startswith(remote):
                return posixpath.normpath(
                    posixpath.join(local, posixpath.relpath(path, remote))
                )
        return None

    async def _get_tmpdir(self, location: str):
        scratch_home = f"/scratch/home/{self.username}"
        temp_dir = posixpath.join(
            scratch_home, "streamflow", "".join(utils.random_name())
        )
        async with self._get_ssh_client_process(
            location=location,
            command=f"mkdir -p {temp_dir}",
            stdout=asyncio.subprocess.STDOUT,
            stderr=asyncio.subprocess.STDOUT,
        ) as proc:
            result = await proc.wait()
            if result.returncode == 0:
                return temp_dir
            else:
                raise WorkflowExecutionException(
                    f"Error while creating directory {temp_dir}: {result.stdout.strip()}"
                )
        return temp_dir

    def _get_volumes(self, location: str) -> MutableSequence[str]:
        for name in self.jobs_table:
            if location in self.jobs_table[name]:
                service = name
                return self.env_description[service].get("volumes", [])
        return []

    async def _copy_remote_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[Location],
        source_location: Location,
        source_connector: str | None = None,
        read_only: bool = False,
    ) -> None:
        effective_locations = self._get_effective_locations(
            locations, dst, source_location
        )
        # Check for the need of a temporary copy
        temp_dir = None
        for location in effective_locations:
            if source_location != location:
                temp_dir = await self._get_tmpdir(location.name)
                copy1_command = ["/bin/cp", "-rf", src, temp_dir]
                await self.run(source_location, copy1_command)
                break
        # Perform the actual copies
        await asyncio.gather(
            *(
                asyncio.create_task(
                    self._copy_remote_to_remote_single(
                        src=src,
                        dst=dst,
                        location=location,
                        source_location=source_location,
                        temp_dir=temp_dir,
                        read_only=read_only,
                    )
                )
                for location in effective_locations
            )
        )
        # If a temporary location was created, delete it
        if temp_dir is not None:
            for location in effective_locations:
                async with self._get_ssh_client_process(
                    location=location.name,
                    command=f"rm -rf {temp_dir}",
                    stdout=asyncio.subprocess.STDOUT,
                    stderr=asyncio.subprocess.STDOUT,
                ) as proc:
                    result = await proc.wait()
                    if result.returncode != 0:
                        raise WorkflowExecutionException(
                            f"Error while removing directory {temp_dir}: {result.stdout.strip()}"
                        )

    async def _copy_remote_to_remote_single(
        self,
        src: str,
        dst: str,
        location: Location,
        source_location: Location,
        temp_dir: str | None,
        read_only: bool = False,
    ) -> None:
        if source_location.name == location.name:
            command = ["/bin/cp", "-rf", src, dst]
            await self.run(location, command)
        else:
            copy2_command = ["/bin/cp", "-rf", temp_dir + "/*", dst]
            await self.run(location, copy2_command)

    async def _copy_local_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[Location],
        read_only: bool = False,
    ) -> None:
        effective_locations = self._get_effective_locations(locations, dst)
        # Check for the need of a temporary copy
        temp_dir = None
        for location in effective_locations:
            shared_path = self._get_shared_path(location.name, dst)
            if shared_path is None:
                temp_dir = await self._get_tmpdir(location.name)
                async with self._get_ssh_client_process(location.name) as ssh_client:
                    await asyncssh.scp(
                        src, (ssh_client, temp_dir), preserve=True, recurse=True
                    )
                break
        # Perform the actual copies
        copy_tasks = []
        for location in effective_locations:
            copy_tasks.append(
                asyncio.create_task(
                    self._copy_local_to_remote_single(
                        src=src,
                        dst=dst,
                        location=location,
                        temp_dir=temp_dir,
                        read_only=read_only,
                    )
                )
            )
        await asyncio.gather(*copy_tasks)
        # If a temporary location was created, delete it
        if temp_dir is not None:
            delete_command = ["rm", "-rf", temp_dir]
            await asyncio.gather(
                *(
                    asyncio.create_task(self.run(location, delete_command))
                    for location in effective_locations
                )
            )

    async def _copy_local_to_remote_single(
        self,
        src: str,
        dst: str,
        location: Location,
        temp_dir: str | None,
        read_only: bool = False,
    ) -> None:
        shared_path = self._get_shared_path(location.name, dst)
        if shared_path is not None:
            async with self._get_ssh_client_process(location.name) as ssh_client:
                await asyncssh.scp(
                    src, (ssh_client, shared_path), preserve=True, recurse=True
                )
        else:
            copy_command = ["/bin/cp", "-rf", temp_dir + "/*", dst]
            await self.run(location, copy_command)

    async def _copy_remote_to_local(
        self, src: str, dst: str, location: Location, read_only: bool = False
    ) -> None:
        shared_path = self._get_shared_path(location.name, src)
        if shared_path is not None:
            async with self._get_ssh_client_process(location.name) as ssh_client:
                await asyncssh.scp(
                    (ssh_client, shared_path), dst, preserve=True, recurse=True
                )
        else:
            temp_dir = await self._get_tmpdir(location.name)
            copy_command = [
                "/bin/cp",
                "-rf",
                src,
                temp_dir,
                "&&",
                "find",
                temp_dir,
                "-maxdepth",
                "1",
                "-mindepth",
                "1",
                "-exec",
                "basename",
                "{}",
                "\\;",
            ]
            contents, _ = await self.run(location, copy_command, capture_output=True)
            contents = contents.split()
            scp_tasks = []
            async with self._get_ssh_client_process(location.name) as ssh_client:
                for content in contents:
                    scp_tasks.append(
                        asyncio.create_task(
                            asyncssh.scp(
                                (ssh_client, posixpath.join(temp_dir, content)),
                                dst,
                                preserve=True,
                                recurse=True,
                            )
                        )
                    )
                await asyncio.gather(*scp_tasks)
            delete_command = ["rm", "-rf", temp_dir]
            await self.run(location, delete_command)

    async def _deploy_node(
        self, name: str, service: MutableMapping[str, Any], node: str
    ):
        deploy_command = (
            f"cd {service.get('workdir')} && "
            if "workdir" in service
            else ""
            f"occam-run "
            f"{get_option('x', service.get('x11'))}"
            f"{get_option('n', node)}"
            f"{get_option('i', service.get('stdin'))}"
            f"{get_option('c', service.get('jobidFile'))}"
            f"{get_option('s', service.get('shmSize'))}"
            f"{get_option('v', service.get('volumes'))}"
            f"{service['image']} "
            f"{' '.join(service.get('command', ''))}"
        )
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"EXECUTING {deploy_command}")
        async with self._get_ssh_client_process(
            location=name,
            command=deploy_command,
            stdout=asyncio.subprocess.STDOUT,
            stderr=asyncio.subprocess.STDOUT,
        ) as proc:
            result = await proc.wait()
            output = result.stdout.strip()
            if result.returncode == 0:
                search_result = re.findall(f"({node}-[0-9]+).*", output, re.MULTILINE)
                if search_result:
                    if name not in self.jobs_table:
                        self.jobs_table[name] = []
                    self.jobs_table[name].append(search_result[0])
                    if logger.isEnabledFor(logging.INFO):
                        logger.info(f"Deployed {name} on {search_result[0]}")
                else:
                    raise WorkflowExecutionException(
                        f"Failed to deploy {name}: {output}"
                    )
            else:
                raise WorkflowExecutionException(f"Failed to deploy {name}: {output}")

    async def _undeploy_node(self, name: str, job_id: str):
        undeploy_command = f"occam-kill {job_id}"
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"EXECUTING {undeploy_command}")
        async with self._get_ssh_client_process(
            location=name,
            command=undeploy_command,
            stdout=asyncio.subprocess.STDOUT,
            stderr=asyncio.subprocess.STDOUT,
        ) as proc:
            result = await proc.wait()
            if result.returncode == 0:
                if logger.isEnabledFor(logging.INFO):
                    logger.info(f"Killed {job_id}")
            else:
                raise WorkflowExecutionException(
                    f"Failed to undeploy {name}: {result.stdout.strip()}"
                )

    async def deploy(self, external: bool) -> None:
        await super().deploy(external)
        if not external:
            deploy_tasks = []
            for name, service in self.env_description.items():
                nodes = service.get("nodes", ["node22"])
                for node in nodes:
                    deploy_tasks.append(
                        asyncio.create_task(self._deploy_node(name, service, node))
                    )
            await asyncio.gather(*deploy_tasks)

    async def get_available_locations(
        self,
        service: str | None = None,
        input_directory: str | None = None,
        output_directory: str | None = None,
        tmp_directory: str | None = None,
    ) -> MutableMapping[str, AvailableLocation]:
        nodes = (
            self.jobs_table.get(service, [])
            if service
            else utils.flatten_list(self.jobs_table.values())
        )
        return {
            n: AvailableLocation(
                name=n,
                deployment=self.deployment_name,
                service=service,
                hostname=n.split("-")[0],
            )
            for n in nodes
        }

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "occam.json")
        )

    async def undeploy(self, external: bool) -> None:
        if not external:
            # Undeploy
            undeploy_tasks = []
            for name in self.jobs_table:
                for job_id in self.jobs_table[name]:
                    undeploy_tasks.append(
                        asyncio.create_task(self._undeploy_node(name, job_id))
                    )
            await asyncio.gather(*undeploy_tasks)
            self.jobs_table = {}
        # Close connection
        await super().undeploy(external)

    async def run(
        self,
        location: Location,
        command: MutableSequence[str],
        environment: MutableMapping[str, str] = None,
        workdir: str | None = None,
        stdin: int | str | None = None,
        stdout: int | str = asyncio.subprocess.STDOUT,
        stderr: int | str = asyncio.subprocess.STDOUT,
        capture_output: bool = False,
        timeout: int | None = None,
        job_name: str | None = None,
    ) -> tuple[Any | None, int] | None:
        command = self._get_command(
            location=location,
            command=command,
            environment=environment,
            workdir=workdir,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            job_name=job_name,
        )
        occam_command = f"occam-exec {location} sh -c '{command}'"
        async with self._get_ssh_client_process(
            location=location.name,
            command=occam_command,
            stdout=asyncio.subprocess.STDOUT,
            stderr=asyncio.subprocess.STDOUT,
        ) as proc:
            result = await proc.wait(timeout=timeout)
            if capture_output:
                lines = (line for line in result.stdout.split("\n"))
                out = ""
                for line in lines:
                    if line.startswith("Trying to exec commands into container"):
                        break
                try:
                    line = next(lines)
                    out = line.strip(" \r\t")
                except StopIteration:
                    return out, result.returncode
                for line in lines:
                    out = "\n".join([out, line.strip(" \r\t")])
                return out, result.returncode
            else:
                return None
