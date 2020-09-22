from __future__ import annotations

import asyncio
import os
import shlex
import subprocess
from typing import TYPE_CHECKING

from streamflow.core.scheduling import Resource
from streamflow.deployment.base import BaseConnector
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from typing import List, Optional, MutableMapping, Any, Tuple
    from typing_extensions import Text


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

    async def _copy_local_to_remote(self, src: Text, dst: Text, resource: Text) -> None:
        resource_id = await self._get_resource_id(resource)
        dst = resource_id + ":" + dst
        proc = await asyncio.create_subprocess_exec(shlex.split(_get_copy_command(src, dst)))
        await proc.wait()

    async def _copy_remote_to_local(self, src: Text, dst: Text, resource: Text) -> None:
        resource_id = await self._get_resource_id(resource)
        src = resource_id + ":" + src
        proc = await asyncio.create_subprocess_exec(shlex.split(_get_copy_command(src, dst)))
        await proc.wait()

    async def _copy_remote_to_remote(self, src: Text, dst: Text, resource: Text, source_remote: Text) -> None:
        source_remote = source_remote or resource
        if source_remote == resource:
            await self.run(resource, ["/bin/cp", "-rf", src, dst])
        else:
            subprocess.run(shlex.split(_get_copy_command(src, dst)), check=True)
            proc = await asyncio.create_subprocess_exec(shlex.split(_get_copy_command(src, dst)))
            await proc.wait()

    async def _get_resource_id(self, resource: Text):
        resource_command = self.base_command() + "".join([
            "ps ",
            "-q ",
            resource
        ])
        proc = await asyncio.create_subprocess_exec(shlex.split(resource_command), capture_output=True, text=True)
        stdout, _ = await proc.communicate()
        return stdout.decode().strip()

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

    async def deploy(self) -> None:
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
        proc = await asyncio.create_subprocess_exec(shlex.split(deploy_command))
        await proc.wait()

    async def get_available_resources(self, service: Text) -> MutableMapping[Text, Resource]:
        return {service: Resource(name=service, hostname=service)}

    async def undeploy(self) -> None:
        undeploy_command = self.base_command() + "".join([
            "down ",
            "{removeVolumes}"
        ]).format(
            removeVolumes=self.get_option("volumes", self.removeVolumes)
        )
        proc = await asyncio.create_subprocess_exec(shlex.split(undeploy_command))
        await proc.wait()

    async def run(self,
                  resource: Text,
                  command: List[Text],
                  environment: MutableMapping[Text, Text] = None,
                  workdir: Optional[Text] = None,
                  capture_output: bool = False,
                  job_name: Optional[Text] = None) -> Optional[Tuple[Optional[Any], int]]:
        run_command = self.base_command() + "".join([
            "exec ",
            "-T ",
            "{environment}",
            "{workdir}",
            "{service}"
            "{command}"
        ]).format(
            environment="".join(["--env-command %s=%s " % (key, value) for (key, value) in
                                 environment.items()]) if environment is not None else "",
            workdir=self.get_option("workdir", workdir) if workdir is not None else "",
            service=resource,
            command=command
        )
        run_command = shlex.split(run_command)
        logger.debug("Executing {command}".format(command=run_command, resource=resource))
        proc = await asyncio.create_subprocess_exec(
            shlex.split(run_command),
            capture_output=capture_output,
            text=capture_output)
        if capture_output:
            stdout, _ = await proc.communicate()
            return stdout.decode().strip(), proc.returncode
        else:
            await proc.wait()
