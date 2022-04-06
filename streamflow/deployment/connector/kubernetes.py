from __future__ import annotations

import asyncio
import io
import os
import posixpath
import shlex
import tarfile
import tempfile
import uuid
from abc import ABC
from pathlib import Path
from shutil import which
from typing import Any, MutableMapping, MutableSequence, Optional, Tuple, Union

import yaml
from cachetools import Cache, TTLCache
from kubernetes_asyncio import client
from kubernetes_asyncio.client import ApiClient, Configuration, V1Container
from kubernetes_asyncio.config import ConfigException, load_incluster_config, load_kube_config
from kubernetes_asyncio.stream import WsApiClient, ws_client

from streamflow.core import utils
from streamflow.core.asyncache import cachedmethod
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.scheduling import Location
from streamflow.deployment.connector.base import BaseConnector
from streamflow.log_handler import logger

SERVICE_NAMESPACE_FILENAME = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"


def _check_helm_installed():
    if which("helm") is None:
        raise WorkflowExecutionException("Helm must be installed on the system to use the Helm connector.")


async def _get_helm_version():
    proc = await asyncio.create_subprocess_exec(
        *shlex.split("helm version --template '{{.Version}}'"),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL)
    stdout, _ = await proc.communicate()
    return stdout.decode().strip()


class BaseKubernetesConnector(BaseConnector, ABC):

    def __init__(self,
                 deployment_name: str,
                 streamflow_config_dir: str,
                 inCluster: Optional[bool] = False,
                 kubeconfig: Optional[str] = os.path.join(str(Path.home()), ".kube", "config"),
                 namespace: Optional[str] = None,
                 locationsCacheTTL: int = None,
                 resourcesCacheTTL: int = None,
                 transferBufferSize: int = (2 ** 25) - 1,
                 maxConcurrentConnections: int = 4096):
        super().__init__(
            deployment_name=deployment_name,
            streamflow_config_dir=streamflow_config_dir,
            transferBufferSize=transferBufferSize)
        self.inCluster = inCluster
        self.kubeconfig = kubeconfig
        self.namespace = namespace
        cacheTTL = locationsCacheTTL
        if cacheTTL is None:
            cacheTTL = resourcesCacheTTL
            if cacheTTL is not None:
                logger.warn("The `resourcesCacheTTL` keyword is deprecated and will be removed in StreamFlow 0.3.0. "
                            "Use `locationsCacheTTL` instead.")
            else:
                cacheTTL = 10
        self.locationsCache: Cache = TTLCache(maxsize=10, ttl=cacheTTL)
        self.configuration: Optional[Configuration] = None
        self.client: Optional[client.CoreV1Api] = None
        self.client_ws: Optional[client.CoreV1Api] = None
        self.maxConcurrentConnections: int = maxConcurrentConnections

    def _configure_incluster_namespace(self):
        if self.namespace is None:
            if not os.path.isfile(SERVICE_NAMESPACE_FILENAME):
                raise ConfigException(
                    "Service namespace file does not exists.")

            with open(SERVICE_NAMESPACE_FILENAME) as f:
                self.namespace = f.read()
                if not self.namespace:
                    raise ConfigException("Namespace file exists but empty.")

    async def _copy_remote_to_remote(self,
                                     src: str,
                                     dst: str,
                                     locations: MutableSequence[str],
                                     source_location: str,
                                     read_only: bool = False) -> None:
        effective_locations = await self._get_effective_locations(locations, dst)
        await super()._copy_remote_to_remote(
            src=src,
            dst=dst,
            locations=effective_locations,
            source_location=source_location,
            read_only=read_only)

    async def _copy_local_to_remote(self,
                                    src: str,
                                    dst: str,
                                    locations: MutableSequence[str],
                                    read_only: bool = False):
        effective_locations = await self._get_effective_locations(locations, dst)
        await super()._copy_local_to_remote(
            src=src,
            dst=dst,
            locations=effective_locations,
            read_only=read_only)

    async def _copy_local_to_remote_single(self,
                                           location: str,
                                           tar_buffer: io.BufferedRandom,
                                           read_only: bool = False) -> None:
        location_buffer = io.BufferedReader(tar_buffer.raw)
        pod, container = location.split(':')
        command = ["tar", "xf", "-", "-C", "/"]
        # noinspection PyUnresolvedReferences
        response = await self.client_ws.connect_get_namespaced_pod_exec(
            name=pod,
            namespace=self.namespace or 'default',
            container=container,
            command=command,
            stderr=False,
            stdin=True,
            stdout=False,
            tty=False,
            _preload_content=False)
        while content := location_buffer.read(self.transferBufferSize):
            channel_prefix = bytes(chr(ws_client.STDIN_CHANNEL), "ascii")
            payload = channel_prefix + content
            await response.send_bytes(payload)
        await response.close()

    async def _copy_remote_to_local(self,
                                    src: str,
                                    dst: str,
                                    location: str,
                                    read_only: bool = False):
        pod, container = location.split(':')
        command = ["tar", "chf", "-", "-C", "/", posixpath.relpath(src, '/')]
        # noinspection PyUnresolvedReferences
        response = await self.client_ws.connect_get_namespaced_pod_exec(
            name=pod,
            namespace=self.namespace or 'default',
            container=container,
            command=command,
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
            _preload_content=False)
        with tempfile.TemporaryFile() as tar_buffer:
            while not response.closed:
                async for msg in response:
                    channel = msg.data[0]
                    data = msg.data[1:]
                    if data and channel == ws_client.STDOUT_CHANNEL:
                        tar_buffer.write(data)
            await response.close()
            tar_buffer.seek(0)
            with tarfile.open(
                    fileobj=tar_buffer,
                    mode='r|') as tar:
                utils.extract_tar_stream(tar, src, dst)

    async def _get_container(self, location: str) -> Tuple[str, V1Container]:
        pod_name, container_name = location.split(':')
        pod = await self.client.read_namespaced_pod(name=pod_name, namespace=self.namespace or 'default')
        for container in pod.spec.containers:
            if container.name == container_name:
                return container.name, container

    async def _get_configuration(self) -> Configuration:
        if self.configuration is None:
            self.configuration = Configuration()
            if self.inCluster:
                load_incluster_config(client_configuration=self.configuration)
                self._configure_incluster_namespace()
            else:
                await load_kube_config(config_file=self.kubeconfig, client_configuration=self.configuration)
        return self.configuration

    async def _get_effective_locations(self,
                                       locations: MutableSequence[str],
                                       dest_path: str,
                                       source_location: Optional[str] = None) -> MutableSequence[str]:
        # Get containers
        container_tasks = []
        for location in locations:
            container_tasks.append(asyncio.create_task(self._get_container(location)))
        containers = {k: v for (k, v) in await asyncio.gather(*container_tasks)}
        # Check if some locations share volume mounts to the same path
        common_paths = {}
        effective_locations = []
        for location in locations:
            container = containers[location.split(':')[1]]
            add_location = True
            for volume in container.volume_mounts:
                if dest_path.startswith(volume.mount_path):
                    path = ':'.join([volume.name, dest_path])
                    if path not in common_paths:
                        common_paths[path] = location
                    elif location == source_location:
                        effective_locations.remove(common_paths[path])
                        common_paths[path] = location
                    else:
                        add_location = False
                    break
            if add_location:
                effective_locations.append(location)
        return effective_locations

    def _get_run_command(self,
                         command: str,
                         location: str,
                         interactive: bool = False):
        pod, container = location.split(':')
        return (
            "kubectl "
            "{namespace}"
            "{kubeconfig}"
            "exec "
            "{pod} "
            "{interactive}"
            "{container}"
            "-- "
            "{command}"
        ).format(
            namespace=self.get_option("namespace", self.namespace),
            kubeconfig=self.get_option("kubeconfig", self.kubeconfig),
            pod=pod,
            interactive=self.get_option("i", interactive),
            container=self.get_option("container", container),
            command=command)

    async def deploy(self, external: bool):
        # Init standard client
        configuration = await self._get_configuration()
        configuration.connection_pool_maxsize = self.maxConcurrentConnections
        self.client = client.CoreV1Api(api_client=ApiClient(configuration=configuration))
        # Init WebSocket client
        configuration = await self._get_configuration()
        configuration.connection_pool_maxsize = self.maxConcurrentConnections
        ws_api_client = WsApiClient(configuration=configuration, heartbeat=30)
        ws_api_client.set_default_header('Connection', 'upgrade,keep-alive')
        self.client_ws = client.CoreV1Api(api_client=ws_api_client)

    async def _run(self,
                   location: str,
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
        command = utils.create_command(
            command, environment, workdir, stdin, stdout, stderr)
        logger.debug("Executing command {command} on {location} {job}".format(
            command=command,
            location=location,
            job="for job {job}".format(job=job_name) if job_name else ""))
        if encode:
            command = utils.encode_command(command)
        pod, container = location.split(':')
        # noinspection PyUnresolvedReferences
        response = await self.client_ws.connect_get_namespaced_pod_exec(
            name=pod,
            namespace=self.namespace or 'default',
            container=container,
            command=["sh", "-c", "{command}".format(command=command)],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
            _preload_content=not capture_output)
        if capture_output:
            with io.StringIO() as out_buffer, io.StringIO() as err_buffer:
                while not response.closed:
                    async for msg in response:
                        data = msg.data.decode('utf-8', 'replace')
                        channel = ord(data[0])
                        data = data[1:]
                        if data and channel in [ws_client.STDOUT_CHANNEL, ws_client.STDERR_CHANNEL]:
                            out_buffer.write(data)
                        elif data and channel == ws_client.ERROR_CHANNEL:
                            err_buffer.write(data)
                await response.close()
                err = yaml.safe_load(err_buffer.getvalue())
                if err['status'] == "Success":
                    return out_buffer.getvalue(), 0
                else:
                    if 'code' in err:
                        return err['message'], int(err['code'])
                    else:
                        return err['message'], int(err['details']['causes'][0]['message'])

    async def undeploy(self, external: bool):
        if self.client is not None:
            await self.client.api_client.close()
            self.client = None
        if self.client_ws is not None:
            await self.client_ws.api_client.close()
            self.client_ws = None
        self.configuration = None


class Helm3Connector(BaseKubernetesConnector):
    def __init__(self,
                 deployment_name: str,
                 streamflow_config_dir: str,
                 chart: str,
                 debug: Optional[bool] = False,
                 kubeContext: Optional[str] = None,
                 kubeconfig: Optional[str] = None,
                 atomic: Optional[bool] = False,
                 caFile: Optional[str] = None,
                 certFile: Optional[str] = None,
                 depUp: Optional[bool] = False,
                 devel: Optional[bool] = False,
                 inCluster: Optional[bool] = False,
                 keepHistory: Optional[bool] = False,
                 keyFile: Optional[str] = None,
                 keyring: Optional[str] = None,
                 locationsCacheTTL: int = None,
                 nameTemplate: Optional[str] = None,
                 namespace: Optional[str] = None,
                 noHooks: Optional[bool] = False,
                 password: Optional[str] = None,
                 renderSubchartNotes: Optional[bool] = False,
                 repo: Optional[str] = None,
                 commandLineValues: Optional[MutableSequence[str]] = None,
                 fileValues: Optional[MutableSequence[str]] = None,
                 stringValues: Optional[MutableSequence[str]] = None,
                 registryConfig: Optional[str] = os.path.join(str(Path.home()), ".config/helm/registry.json"),
                 releaseName: Optional[str] = "release-%s" % str(uuid.uuid1()),
                 repositoryCache: Optional[str] = os.path.join(str(Path.home()), ".cache/helm/repository"),
                 repositoryConfig: Optional[str] = os.path.join(str(Path.home()), ".config/helm/repositories.yaml"),
                 resourcesCacheTTL: int = None,
                 skipCrds: Optional[bool] = False,
                 timeout: Optional[str] = "1000m",
                 transferBufferSize: int = (32 << 20) - 1,
                 username: Optional[str] = None,
                 yamlValues: Optional[MutableSequence[str]] = None,
                 verify: Optional[bool] = False,
                 chartVersion: Optional[str] = None,
                 wait: Optional[bool] = True):
        super().__init__(
            deployment_name=deployment_name,
            streamflow_config_dir=streamflow_config_dir,
            inCluster=inCluster,
            kubeconfig=kubeconfig,
            namespace=namespace,
            locationsCacheTTL=locationsCacheTTL,
            resourcesCacheTTL=resourcesCacheTTL,
            transferBufferSize=transferBufferSize)
        self.chart: str = os.path.join(streamflow_config_dir, chart)
        self.debug: bool = debug
        self.kubeContext: Optional[str] = kubeContext
        self.atomic: bool = atomic
        self.caFile: Optional[str] = caFile
        self.certFile: Optional[str] = certFile
        self.depUp: bool = depUp
        self.devel: bool = devel
        self.keepHistory: bool = keepHistory
        self.keyFile: Optional[str] = keyFile
        self.keyring: Optional[str] = keyring
        self.nameTemplate: Optional[str] = nameTemplate
        self.noHooks: bool = noHooks
        self.password: Optional[str] = password
        self.renderSubchartNotes: bool = renderSubchartNotes
        self.repo: Optional[str] = repo
        self.commandLineValues: Optional[MutableSequence[str]] = commandLineValues
        self.fileValues: Optional[MutableSequence[str]] = fileValues
        self.stringValues: Optional[MutableSequence[str]] = stringValues
        self.skipCrds: bool = skipCrds
        self.registryConfig = registryConfig
        self.releaseName: str = releaseName
        self.repositoryCache = repositoryCache
        self.repositoryConfig = repositoryConfig
        self.timeout: Optional[str] = timeout
        self.username: Optional[str] = username
        self.yamlValues: Optional[MutableSequence[str]] = yamlValues
        self.verify: bool = verify
        self.chartVersion: Optional[str] = chartVersion
        self.wait: bool = wait

    def base_command(self):
        return (
            "helm "
            "{debug}"
            "{kubeContext}"
            "{kubeconfig}"
            "{namespace}"
            "{registryConfig}"
            "{repositoryCache}"
            "{repositoryConfig}"
        ).format(
            debug=self.get_option("debug", self.debug),
            kubeContext=self.get_option("kube-context", self.kubeContext),
            kubeconfig=self.get_option("kubeconfig", self.kubeconfig),
            namespace=self.get_option("namespace", self.namespace),
            registryConfig=self.get_option("registry-config", self.registryConfig),
            repositoryCache=self.get_option("repository-cache", self.repositoryCache),
            repositoryConfig=self.get_option("repository-config", self.repositoryConfig),
        )

    async def deploy(self, external: bool) -> None:
        # Create clients
        await super().deploy(external)
        if not external:
            # Check if Helm is installed
            _check_helm_installed()
            # Check correct version of Helm
            version = await _get_helm_version()
            if not version.startswith("v3"):
                raise WorkflowExecutionException(
                    "Helm {version} is not compatible with Helm3Connector".format(version=version))
            # Deploy Helm charts
            deploy_command = self.base_command() + "".join([
                "install "
                "{atomic}"
                "{caFile}"
                "{certFile}"
                "{depUp}"
                "{devel}"
                "{keyFile}"
                "{keyring}"
                "{nameTemplate}"
                "{noHooks}"
                "{password}"
                "{renderSubchartNotes}"
                "{repo}"
                "{commandLineValues}"
                "{fileValues}"
                "{stringValues}"
                "{skipCrds}"
                "{timeout}"
                "{username}"
                "{yamlValues}"
                "{verify}"
                "{chartVersion}"
                "{wait}"
                "{releaseName}"
                "{chart}"
            ]).format(
                atomic=self.get_option("atomic", self.atomic),
                caFile=self.get_option("ca-file", self.caFile),
                certFile=self.get_option("cert-file", self.certFile),
                depUp=self.get_option("dep-up", self.depUp),
                devel=self.get_option("devel", self.devel),
                keyFile=self.get_option("key-file", self.keyFile),
                keyring=self.get_option("keyring", self.keyring),
                nameTemplate=self.get_option("name-template", self.nameTemplate),
                namespace=self.get_option("namespace", self.namespace),
                noHooks=self.get_option("no-hooks", self.noHooks),
                password=self.get_option("password", self.password),
                renderSubchartNotes=self.get_option("render-subchart-notes", self.renderSubchartNotes),
                repo=self.get_option("repo", self.repo),
                commandLineValues=self.get_option("set", self.commandLineValues),
                fileValues=self.get_option("set-file", self.fileValues),
                stringValues=self.get_option("set-string", self.stringValues),
                skipCrds=self.get_option("skip-crds", self.skipCrds),
                timeout=self.get_option("timeout", self.timeout),
                username=self.get_option("username", self.username),
                yamlValues=self.get_option("values", self.yamlValues),
                verify=self.get_option("verify", self.verify),
                chartVersion=self.get_option("version", self.chartVersion),
                wait=self.get_option("wait", self.wait),
                releaseName="{releaseName} ".format(releaseName=self.releaseName),
                chart="\"{chart}\"".format(chart=self.chart)
            )
            logger.debug("Executing {command}".format(command=deploy_command))
            proc = await asyncio.create_subprocess_exec(*shlex.split(deploy_command))
            await proc.wait()

    @cachedmethod(lambda self: self.locationsCache)
    async def get_available_locations(self,
                                      service: str,
                                      input_directory: str,
                                      output_directory: str,
                                      tmp_directory: str) -> MutableMapping[str, Location]:
        pods = await self.client.list_namespaced_pod(
            namespace=self.namespace or 'default',
            label_selector="app.kubernetes.io/instance={}".format(self.releaseName),
            field_selector="status.phase=Running"
        )
        valid_targets = {}
        for pod in pods.items:
            # Check if pod is ready
            is_ready = True
            for condition in pod.status.conditions:
                if condition.status != 'True':
                    is_ready = False
                    break
            # Filter out not ready and Terminating locations
            if is_ready and pod.metadata.deletion_timestamp is None:
                for container in pod.spec.containers:
                    if not service or service == container.name:
                        location_name = pod.metadata.name + ':' + service
                        valid_targets[location_name] = Location(name=location_name, hostname=pod.status.pod_ip)
                        break
        return valid_targets

    async def undeploy(self, external: bool) -> None:
        if not external:
            # Undeploy
            undeploy_command = self.base_command() + (
                "uninstall "
                "{keepHistory}"
                "{noHooks}"
                "{timeout}"
                "{releaseName}"
            ).format(
                keepHistory=self.get_option("keep-history", self.keepHistory),
                noHooks=self.get_option("no-hooks", self.noHooks),
                timeout=self.get_option("timeout", self.timeout),
                releaseName=self.releaseName
            )
            logger.debug("Executing {command}".format(command=undeploy_command))
            proc = await asyncio.create_subprocess_exec(*shlex.split(undeploy_command))
            await proc.wait()
        # Close connections
        await super().undeploy(external)
