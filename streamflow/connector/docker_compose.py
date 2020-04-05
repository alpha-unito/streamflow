import os
import shlex
import subprocess
from typing import List, Optional, MutableMapping, Any

from streamflow.connector.connector import ConnectorCopyKind, Connector
from streamflow.log_handler import _logger


class DockerComposeConnector(Connector):

    def __init__(self,
                 config_file: MutableMapping[str, str],
                 files: List[str],
                 projectName: Optional[str] = None,
                 verbose: Optional[bool] = False,
                 logLevel: Optional[str] = None,
                 noAnsi: Optional[bool] = False,
                 host: Optional[str] = None,
                 tls: Optional[bool] = False,
                 tlscacert: Optional[str] = None,
                 tlscert: Optional[str] = None,
                 tlskey: Optional[str] = None,
                 tlsverify: Optional[bool] = False,
                 skipHostnameCheck: Optional[bool] = False,
                 projectDirectory: Optional[str] = None,
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
        super().__init__()
        config_dir = config_file['dirname']
        self.files = [os.path.join(config_dir, file) for file in files]
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

    def base_command(self) -> str:
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

    def copy(self,
             src: str,
             dst: str,
             resource: str,
             kind: ConnectorCopyKind,
             source_remote: str = None
             ) -> None:
        resource_id = \
            subprocess.run(shlex.split(self.base_command()) + ["ps", "-q", resource],
                           check=True,
                           capture_output=True,
                           text=True
                           ).stdout.strip()
        if kind == ConnectorCopyKind.remoteToRemote:
            source_remote = source_remote or resource
            if source_remote == resource:
                self.run(resource, ["/bin/cp", "-rf", src, dst])
        if kind == ConnectorCopyKind.localToRemote:
            dst = resource_id + ":" + dst
        elif kind == ConnectorCopyKind.remoteToLocal:
            src = resource_id + ":" + src
        copy_command = "".join([
            "docker ",
            "cp ",
            "{src} ",
            "{dst}"
        ]).format(
            src=src,
            dst=dst
        )
        subprocess.run(shlex.split(copy_command), check=True)

    def deploy(self) -> None:
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
        subprocess.run(shlex.split(deploy_command), check=True)

    def get_available_resources(self, service: str) -> List[str]:
        return [service]

    def get_runtime(self,
                    resource: str,
                    environment: MutableMapping[str, str] = None,
                    workdir: str = None
                    ) -> str:
        return self.base_command() + "".join([
            "exec ",
            "-T ",
            "{environment}",
            "{workdir}",
            "{service}"
        ]).format(
            environment="".join(["--env-command %s=%s " % (key, value) for (key, value) in
                                 environment.items()]) if environment is not None else "",
            workdir=self.get_option("workdir", workdir) if workdir is not None else "",
            service=resource
        )

    def undeploy(self) -> None:
        undeploy_command = self.base_command() + "".join([
            "down ",
            "{removeVolumes}"
        ]).format(
            removeVolumes=self.get_option("volumes", self.removeVolumes)
        )
        subprocess.run(shlex.split(undeploy_command), check=True)

    def run(self,
            resource: str,
            command: List[str],
            environment: MutableMapping[str, str] = None,
            workdir: str = None,
            capture_output: bool = False) -> Optional[Any]:
        run_command = shlex.split(self.get_runtime(resource=resource,
                                                   environment=environment,
                                                   workdir=workdir
                                                   )) + command
        _logger.debug("Executing {command}".format(command=run_command, resource=resource))
        process = subprocess.run(
            run_command,
            check=True,
            capture_output=capture_output,
            text=capture_output)
        if capture_output:
            return process.stdout
