from __future__ import annotations

import asyncio
import json
import os
import shlex
from asyncio.subprocess import STDOUT
from typing import TYPE_CHECKING, Union

from streamflow.core.scheduling import Resource
from streamflow.deployment.base import BaseConnector
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from typing import List, Optional, MutableMapping, Any, Tuple
    from typing_extensions import Text


async def _check_effective_resource(common_paths: MutableMapping[Text, Any],
                                    effective_resources: List[Text],
                                    resource: Text,
                                    path: Text,
                                    source_remote: Optional[Text] = None
                                    ) -> Tuple[MutableMapping[Text, Any], List[Text]]:
    # Get all container mounts
    volumes = await _get_volumes(resource)
    for volume in volumes:
        # If path is in a persistent volume
        if path.startswith(volume['Destination']):
            if path not in common_paths:
                common_paths[path] = []
            # Check if path is shared with another resource that has been already processed
            for i, common_path in enumerate(common_paths[path]):
                if common_path['source'] == volume['Source']:
                    # If path has already been processed, but the current resource is the source, substitute it
                    if resource == source_remote:
                        effective_resources.remove(common_path['resource'])
                        common_path['resource'] = resource
                        common_paths[path][i] = common_path
                        effective_resources.append(resource)
                        return common_paths, effective_resources
                    # Otherwise simply skip current resource
                    else:
                        return common_paths, effective_resources
            # If this is the first resource encountered with the persistent path, add it to the list
            effective_resources.append(resource)
            common_paths[path].append({'source': volume['Source'], 'resource': resource})
            return common_paths, effective_resources
    # If path is not in a persistent volume, add the current resource to the list
    effective_resources.append(resource)
    return common_paths, effective_resources


async def _copy_local_to_remote_single(src: Text,
                                       dst: Text,
                                       resource: Text) -> None:
    dst = resource + ":" + dst
    proc = await asyncio.create_subprocess_exec(*shlex.split(_get_copy_command(src, dst)))
    await proc.wait()


def _get_copy_command(src: Text, dst: Text):
    return "".join([
        "docker ",
        "cp ",
        "{src} ",
        "{dst}"
    ]).format(
        src=src,
        dst=dst
    )


async def _get_effective_resources(resources: List[Text],
                                   dest_path: Text,
                                   source_remote: Optional[Text] = None) -> List[Text]:
    common_paths = {}
    effective_resources = []
    for resource in resources:
        common_paths, effective_resources = await _check_effective_resource(
            common_paths,
            effective_resources,
            resource,
            dest_path,
            source_remote)
    return effective_resources


async def _get_volumes(resource: Text) -> List[MutableMapping[Text, Text]]:
    inspect_command = "".join([
        "docker ",
        "inspect ",
        "--format ",
        "'{{json .Mounts }}' ",
        resource])
    proc = await asyncio.create_subprocess_exec(
        *shlex.split(inspect_command),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)
    stdout, _ = await proc.communicate()
    return json.loads(stdout.decode().strip())


class DockerComposeConnector(BaseConnector):

    def __init__(self,
                 streamflow_config_dir: Text,
                 files: List[Text],
                 projectName: Optional[Text] = None,
                 verbose: Optional[bool] = False,
                 logLevel: Optional[Text] = None,
                 noAnsi: Optional[bool] = False,
                 host: Optional[Text] = None,
                 tls: Optional[bool] = False,
                 tlscacert: Optional[Text] = None,
                 tlscert: Optional[Text] = None,
                 tlskey: Optional[Text] = None,
                 tlsverify: Optional[bool] = False,
                 skipHostnameCheck: Optional[bool] = False,
                 projectDirectory: Optional[Text] = None,
                 compatibility: Optional[bool] = False,
                 noDeps: Optional[bool] = False,
                 forceRecreate: Optional[bool] = False,
                 alwaysRecreateDeps: Optional[bool] = False,
                 noRecreate: Optional[bool] = False,
                 noBuild: Optional[bool] = False,
                 noStart: Optional[bool] = False,
                 build: Optional[bool] = False,
                 timeout: Optional[int] = None,
                 renewAnonVolumes: Optional[bool] = False,
                 removeOrphans: Optional[bool] = False,
                 removeVolumes: Optional[bool] = False
                 ) -> None:
        super().__init__(streamflow_config_dir)
        self.files = [os.path.join(streamflow_config_dir, file) for file in files]
        self.projectName = projectName
        self.verbose = verbose
        self.logLevel = logLevel
        self.noAnsi = noAnsi
        self.host = host
        self.noDeps = noDeps
        self.forceRecreate = forceRecreate
        self.alwaysRecreateDeps = alwaysRecreateDeps
        self.noRecreate = noRecreate
        self.noBuild = noBuild
        self.noStart = noStart
        self.build = build
        self.renewAnonVolumes = renewAnonVolumes
        self.removeOrphans = removeOrphans
        self.removeVolumes = removeVolumes
        self.skipHostnameCheck = skipHostnameCheck
        self.projectDirectory = projectDirectory
        self.compatibility = compatibility
        self.timeout = timeout
        self.tls = tls
        self.tlscacert = tlscacert
        self.tlscert = tlscert
        self.tlskey = tlskey
        self.tlsverify = tlsverify

    async def _copy_local_to_remote(self,
                                    src: Text,
                                    dst: Text,
                                    resources: List[Text]) -> None:
        effective_resources = await _get_effective_resources(resources, dst)
        copy_tasks = []
        for resource in effective_resources:
            copy_tasks.append(asyncio.create_task(_copy_local_to_remote_single(src, dst, resource)))
        await asyncio.gather(*copy_tasks)

    async def _copy_remote_to_local(self,
                                    src: Text,
                                    dst: Text,
                                    resource: Text) -> None:
        src = resource + ":" + src
        proc = await asyncio.create_subprocess_exec(*shlex.split(_get_copy_command(src, dst)))
        await proc.wait()

    async def _copy_remote_to_remote(self,
                                     src: Text,
                                     dst: Text,
                                     resources: List[Text],
                                     source_remote: Text) -> None:
        effective_resources = await _get_effective_resources(resources, dst, source_remote)
        await super()._copy_remote_to_remote(src, dst, effective_resources, source_remote)

    def base_command(self) -> Text:
        return "".join([
            "docker-compose ",
            "{files}",
            "{projectName}",
            "{verbose}",
            "{logLevel}",
            "{noAnsi}",
            "{host}",
            "{tls}",
            "{tlscacert}",
            "{tlscert}",
            "{tlskey}",
            "{tlsverify}",
            "{skipHostnameCheck}",
            "{projectDirectory}",
            "{compatibility} "
        ]).format(
            files=self.get_option("file", self.files),
            projectName=self.get_option("project-name", self.projectName),
            verbose=self.get_option("verbose", self.verbose),
            logLevel=self.get_option("log-level", self.logLevel),
            noAnsi=self.get_option("no-ansi", self.noAnsi),
            host=self.get_option("host", self.host),
            tls=self.get_option("tls", self.tls),
            tlscacert=self.get_option("tlscacert", self.tlscacert),
            tlscert=self.get_option("tlscert", self.tlscert),
            tlskey=self.get_option("tlskey", self.tlskey),
            tlsverify=self.get_option("tlsverify", self.tlsverify),
            skipHostnameCheck=self.get_option("skip-hostname-check", self.skipHostnameCheck),
            projectDirectory=self.get_option("project-directory", self.projectDirectory),
            compatibility=self.get_option("compatibility", self.compatibility)
        )

    async def deploy(self, external: bool) -> None:
        if not external:
            deploy_command = self.base_command() + "".join([
                "up ",
                "--detach ",
                "{noDeps}",
                "{forceRecreate}",
                "{alwaysRecreateDeps}",
                "{noRecreate}",
                "{noBuild}",
                "{noStart}"
            ]).format(
                noDeps=self.get_option("no-deps ", self.noDeps),
                forceRecreate=self.get_option("force-recreate", self.forceRecreate),
                alwaysRecreateDeps=self.get_option("always-recreate-deps", self.alwaysRecreateDeps),
                noRecreate=self.get_option("no-recreate", self.noRecreate),
                noBuild=self.get_option("no-build", self.noBuild),
                noStart=self.get_option("no-start", self.noStart)
            )
            logger.debug("Executing command {command}".format(command=deploy_command))
            proc = await asyncio.create_subprocess_exec(*shlex.split(deploy_command))
            await proc.wait()

    async def get_available_resources(self, service: Text) -> MutableMapping[Text, Resource]:
        ps_command = self.base_command() + "".join([
            "ps ",
            "--filter \"com.docker.compose.service\"=\"{service}\"".format(service=service)
        ])
        logger.debug("Executing command {command}".format(command=ps_command))
        proc = await asyncio.create_subprocess_exec(
            *shlex.split(ps_command),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        stdout, _ = await proc.communicate()
        lines = (line for line in stdout.decode().strip().split('\n'))
        resources = {}
        for line in lines:
            if line.startswith("---------"):
                break
        for line in lines:
            resource_name = line.split()[0].strip()
            inspect_command = "".join([
                "docker ",
                "inspect ",
                "--format ",
                "'{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ",
                resource_name])
            proc = await asyncio.create_subprocess_exec(
                *shlex.split(inspect_command),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE)
            stdout, _ = await proc.communicate()
            resources[resource_name] = Resource(name=resource_name, hostname=stdout.decode().strip())
        return resources

    async def undeploy(self, external: bool) -> None:
        if not external:
            undeploy_command = self.base_command() + "".join([
                "down ",
                "{removeVolumes}"
            ]).format(
                removeVolumes=self.get_option("volumes", self.removeVolumes)
            )
            logger.debug("Executing command {command}".format(command=undeploy_command))
            proc = await asyncio.create_subprocess_exec(*shlex.split(undeploy_command))
            await proc.wait()

    async def run(self,
                  resource: Text,
                  command: List[Text],
                  environment: MutableMapping[Text, Text] = None,
                  workdir: Optional[Text] = None,
                  stdin: Optional[Union[int, Text]] = None,
                  stdout: Union[int, Text] = STDOUT,
                  stderr: Union[int, Text] = STDOUT,
                  capture_output: bool = False,
                  job_name: Optional[Text] = None) -> Optional[Tuple[Optional[Any], int]]:
        helper_file = await self._build_helper_file(
            command, resource, environment, workdir, stdin, stdout, stderr)
        try:
            run_command = "".join([
                "docker "
                "exec ",
                "-t ",
                "{service} "
                "{command}"
            ]).format(
                environment="".join(["--env-command %s=%s " % (key, value) for (key, value) in
                                     environment.items()]) if environment is not None else "",
                workdir=self.get_option("workdir", workdir) if workdir is not None else "",
                service=resource,
                command=helper_file
            )
            proc = await asyncio.create_subprocess_exec(
                *shlex.split(run_command),
                stdout=asyncio.subprocess.PIPE if capture_output else None,
                stderr=asyncio.subprocess.PIPE if capture_output else None)
            if capture_output:
                stdout, _ = await proc.communicate()
                return stdout.decode().strip(), proc.returncode
            else:
                await proc.wait()
        finally:
            rm_command = "".join([
                "docker "
                "exec ",
                "-t ",
                "{service} ",
                "{command}",
            ]).format(
                service=resource,
                command="rm -rf {file}".format(file=helper_file)
            )
            proc = await asyncio.create_subprocess_exec(*shlex.split(rm_command))
            await proc.wait()
