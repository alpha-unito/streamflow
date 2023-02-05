from __future__ import annotations

import asyncio
import io
import logging
import os
import posixpath
import shlex
import tarfile
import uuid
from abc import ABC
from pathlib import Path
from shutil import which
from typing import (
    Any,
    Awaitable,
    Coroutine,
    MutableMapping,
    MutableSequence,
    cast,
)

import pkg_resources
import yaml
from cachetools import Cache, TTLCache
from kubernetes_asyncio import client
from kubernetes_asyncio.client import ApiClient, Configuration, V1Container
from kubernetes_asyncio.config import (
    ConfigException,
    load_incluster_config,
    load_kube_config,
)
from kubernetes_asyncio.stream import WsApiClient, ws_client

from streamflow.core import utils
from streamflow.core.asyncache import cachedmethod
from streamflow.core.data import StreamWrapperContext
from streamflow.core.deployment import Connector, Location
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.scheduling import AvailableLocation
from streamflow.deployment import aiotarstream
from streamflow.deployment.aiotarstream import BaseStreamWrapper
from streamflow.deployment.connector.base import BaseConnector, extract_tar_stream
from streamflow.log_handler import logger

SERVICE_NAMESPACE_FILENAME = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"


def _check_helm_installed():
    if which("helm") is None:
        raise WorkflowExecutionException(
            "Helm must be installed on the system to use the Helm connector."
        )


async def _get_helm_version():
    proc = await asyncio.create_subprocess_exec(
        *shlex.split("helm version --template '{{.Version}}'"),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await proc.communicate()
    return stdout.decode().strip()


class KubernetesResponseWrapper(BaseStreamWrapper):
    async def read(self, size: int | None = None):
        while not self.stream.closed:
            async for msg in self.stream:
                channel = msg.data[0]
                data = msg.data[1:]
                if data and channel == ws_client.STDOUT_CHANNEL:
                    return data
        return None

    async def write(self, data: Any):
        channel_prefix = bytes(chr(ws_client.STDIN_CHANNEL), "ascii")
        payload = channel_prefix + data
        await self.stream.send_bytes(payload)


class KubernetesResponseWrapperContext(StreamWrapperContext):
    def __init__(self, coro: Coroutine):
        self.coro: Coroutine = coro
        self.response: KubernetesResponseWrapper | None = None

    async def __aenter__(self):
        response = await self.coro
        self.response = KubernetesResponseWrapper(response)
        return self.response

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.response:
            await self.response.close()


class BaseKubernetesConnector(BaseConnector, ABC):
    def __init__(
        self,
        deployment_name: str,
        config_dir: str,
        inCluster: bool | None = False,
        kubeconfig: str | None = None,
        namespace: str | None = None,
        locationsCacheSize: int = None,
        locationsCacheTTL: int = None,
        resourcesCacheSize: int = None,
        resourcesCacheTTL: int = None,
        transferBufferSize: int = (2**25) - 1,
        maxConcurrentConnections: int = 4096,
    ):
        super().__init__(
            deployment_name=deployment_name,
            config_dir=config_dir,
            transferBufferSize=transferBufferSize,
        )
        self.inCluster = inCluster
        self.kubeconfig = (
            kubeconfig
            if kubeconfig is not None
            else os.path.join(str(Path.home()), ".kube", "config")
        )
        self.namespace = namespace
        cacheSize = locationsCacheSize
        if cacheSize is None:
            cacheSize = resourcesCacheSize
            if cacheSize is not None:
                if logger.isEnabledFor(logging.WARN):
                    logger.warn(
                        "The `resourcesCacheSize` keyword is deprecated and will be removed in StreamFlow 0.3.0. "
                        "Use `locationsCacheSize` instead."
                    )
            else:
                cacheSize = 10
        cacheTTL = locationsCacheTTL
        if cacheTTL is None:
            cacheTTL = resourcesCacheTTL
            if cacheTTL is not None:
                if logger.isEnabledFor(logging.WARN):
                    logger.warn(
                        "The `resourcesCacheTTL` keyword is deprecated and will be removed in StreamFlow 0.3.0. "
                        "Use `locationsCacheTTL` instead."
                    )
            else:
                cacheTTL = 10
        self.locationsCache: Cache = TTLCache(maxsize=cacheSize, ttl=cacheTTL)
        self.configuration: Configuration | None = None
        self.client: client.CoreV1Api | None = None
        self.client_ws: client.CoreV1Api | None = None
        self.maxConcurrentConnections: int = maxConcurrentConnections

    def _configure_incluster_namespace(self):
        if self.namespace is None:
            if not os.path.isfile(SERVICE_NAMESPACE_FILENAME):
                raise ConfigException("Service namespace file does not exists.")

            with open(SERVICE_NAMESPACE_FILENAME) as f:
                self.namespace = f.read()
                if not self.namespace:
                    raise ConfigException("Namespace file exists but empty.")

    async def _copy_local_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[Location],
        read_only: bool = False,
    ):
        effective_locations = await self._get_effective_locations(locations, dst)
        await super()._copy_local_to_remote(
            src=src, dst=dst, locations=effective_locations, read_only=read_only
        )

    async def _copy_local_to_remote_single(
        self, src: str, dst: str, location: Location, read_only: bool = False
    ) -> None:
        pod, container = location.name.split(":")
        command = ["tar", "xf", "-", "-C", "/"]
        # noinspection PyUnresolvedReferences
        response = await self.client_ws.connect_get_namespaced_pod_exec(
            name=pod,
            namespace=self.namespace or "default",
            container=container,
            command=command,
            stderr=False,
            stdin=True,
            stdout=False,
            tty=False,
            _preload_content=False,
        )
        try:
            async with aiotarstream.open(
                stream=KubernetesResponseWrapper(response),
                format=tarfile.GNU_FORMAT,
                mode="w",
                dereference=True,
                copybufsize=self.transferBufferSize,
            ) as tar:
                await tar.add(src, arcname=dst)
        except tarfile.TarError as e:
            raise WorkflowExecutionException(
                f"Error copying {src} to {dst} on location {location}: {e}"
            ) from e
        finally:
            await response.close()

    async def _copy_remote_to_local(
        self, src: str, dst: str, location: Location, read_only: bool = False
    ):
        pod, container = location.name.split(":")
        command = ["tar", "chf", "-", "-C", "/", posixpath.relpath(src, "/")]
        # noinspection PyUnresolvedReferences
        response = await self.client_ws.connect_get_namespaced_pod_exec(
            name=pod,
            namespace=self.namespace or "default",
            container=container,
            command=command,
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
            _preload_content=False,
        )
        try:
            async with aiotarstream.open(
                stream=KubernetesResponseWrapper(response),
                mode="r",
                copybufsize=self.transferBufferSize,
            ) as tar:
                await extract_tar_stream(tar, src, dst, self.transferBufferSize)
        except tarfile.TarError as e:
            raise WorkflowExecutionException(
                f"Error copying {src} from location {location} to {dst}: {e}"
            ) from e
        finally:
            await response.close()

    async def _copy_remote_to_remote(
        self,
        src: str,
        dst: str,
        locations: MutableSequence[Location],
        source_location: Location,
        source_connector: Connector | None = None,
        read_only: bool = False,
    ) -> None:
        source_connector = source_connector or self
        locations = await self._get_effective_locations(locations, dst)
        if source_connector == self and source_location in locations:
            if src != dst:
                command = ["/bin/cp", "-rf", src, dst]
                await self.run(source_location, command)
                locations.remove(source_location)
        if locations:
            # Get write command
            write_command = await utils.get_remote_to_remote_write_command(
                src_connector=source_connector,
                src_location=source_location,
                src=src,
                dst_connector=self,
                dst_locations=locations,
                dst=dst,
            )
            async with source_connector._get_stream_reader(
                source_location, src
            ) as reader:
                # Open a target response for each location
                writers = [
                    KubernetesResponseWrapper(w)
                    for w in await asyncio.gather(
                        *(
                            asyncio.create_task(
                                cast(
                                    Coroutine,
                                    self.client_ws.connect_get_namespaced_pod_exec(
                                        name=location.name.split(":")[0],
                                        namespace=self.namespace or "default",
                                        container=location.name.split(":")[1],
                                        command=write_command,
                                        stderr=True,
                                        stdin=False,
                                        stdout=True,
                                        tty=False,
                                        _preload_content=False,
                                    ),
                                )
                            )
                            for location in locations
                        )
                    )
                ]
                # Multiplex the reader output to all the writers
                while content := await reader.read(source_connector.transferBufferSize):
                    await asyncio.gather(
                        *(
                            asyncio.create_task(writer.write(content))
                            for writer in writers
                        )
                    )

    async def _get_container(self, location: Location) -> tuple[str, V1Container]:
        pod_name, container_name = location.name.split(":")
        pod = await self.client.read_namespaced_pod(
            name=pod_name, namespace=self.namespace or "default"
        )
        for container in pod.spec.containers:
            if container.name == container_name:
                return container.name, container
        raise WorkflowExecutionException(
            f"No container with name {container_name} available on pod {pod_name}."
        )

    async def _get_configuration(self) -> Configuration:
        if self.configuration is None:
            self.configuration = Configuration()
            if self.inCluster:
                load_incluster_config(client_configuration=self.configuration)
                self._configure_incluster_namespace()
            else:
                await load_kube_config(
                    config_file=self.kubeconfig, client_configuration=self.configuration
                )
        return self.configuration

    async def _get_effective_locations(
        self,
        locations: MutableSequence[Location],
        dest_path: str,
        source_location: Location | None = None,
    ) -> MutableSequence[Location]:
        # Get containers
        container_tasks = []
        for location in locations:
            container_tasks.append(asyncio.create_task(self._get_container(location)))
        containers = {k: v for (k, v) in await asyncio.gather(*container_tasks)}
        # Check if some locations share volume mounts to the same path
        common_paths = {}
        effective_locations = []
        for location in locations:
            container = containers[location.name.split(":")[1]]
            add_location = True
            for volume in container.volume_mounts:
                if dest_path.startswith(volume.mount_path):
                    path = ":".join([volume.name, dest_path])
                    if path not in common_paths:
                        common_paths[path] = location
                    elif location.name == source_location.name:
                        effective_locations.remove(common_paths[path])
                        common_paths[path] = location
                    else:
                        add_location = False
                    break
            if add_location:
                effective_locations.append(location)
        return effective_locations

    def _get_run_command(
        self, command: str, location: Location, interactive: bool = False
    ):
        pod, container = location.name.split(":")
        return "".join(
            [
                "kubectl ",
                self.get_option("namespace", self.namespace),
                self.get_option("kubeconfig", self.kubeconfig),
                "exec ",
                f"{pod} ",
                self.get_option("i", interactive),
                self.get_option("container", container),
                "-- ",
                command,
            ]
        )

    def _get_stream_reader(self, location: Location, src: str) -> StreamWrapperContext:
        pod, container = location.name.split(":")
        dirname, basename = posixpath.split(src)
        return KubernetesResponseWrapperContext(
            coro=cast(
                Coroutine,
                self.client_ws.connect_get_namespaced_pod_exec(
                    name=pod,
                    namespace=self.namespace or "default",
                    container=container,
                    command=["tar", "chf", "-", "-C", dirname, basename],
                    stderr=False,
                    stdin=True,
                    stdout=False,
                    tty=False,
                    _preload_content=False,
                ),
            )
        )

    async def deploy(self, external: bool):
        # Init standard client
        configuration = await self._get_configuration()
        configuration.connection_pool_maxsize = self.maxConcurrentConnections
        self.client = client.CoreV1Api(
            api_client=ApiClient(configuration=configuration)
        )
        # Init WebSocket client
        configuration = await self._get_configuration()
        configuration.connection_pool_maxsize = self.maxConcurrentConnections
        ws_api_client = WsApiClient(configuration=configuration, heartbeat=30)
        ws_api_client.set_default_header("Connection", "upgrade,keep-alive")
        self.client_ws = client.CoreV1Api(api_client=ws_api_client)

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
        command = utils.create_command(
            command, environment, workdir, stdin, stdout, stderr
        )
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "EXECUTING command {command} on {location} {job}".format(
                    command=command,
                    location=location,
                    job=f"for job {job_name}" if job_name else "",
                )
            )
        command = utils.encode_command(command)
        pod, container = location.name.split(":")
        # noinspection PyUnresolvedReferences
        response = await asyncio.wait_for(
            cast(
                Awaitable,
                self.client_ws.connect_get_namespaced_pod_exec(
                    name=pod,
                    namespace=self.namespace or "default",
                    container=container,
                    command=["sh", "-c", f"{command}"],
                    stderr=True,
                    stdin=False,
                    stdout=True,
                    tty=False,
                    _preload_content=not capture_output,
                ),
            ),
            timeout=timeout,
        )
        if capture_output:
            with io.StringIO() as out_buffer, io.StringIO() as err_buffer:
                while not response.closed:
                    async for msg in response:
                        data = msg.data.decode("utf-8", "replace")
                        channel = ord(data[0])
                        data = data[1:]
                        if data and channel in [
                            ws_client.STDOUT_CHANNEL,
                            ws_client.STDERR_CHANNEL,
                        ]:
                            out_buffer.write(data)
                        elif data and channel == ws_client.ERROR_CHANNEL:
                            err_buffer.write(data)
                await response.close()
                err = yaml.safe_load(err_buffer.getvalue())
                if err["status"] == "Success":
                    return out_buffer.getvalue(), 0
                else:
                    if "code" in err:
                        return err["message"], int(err["code"])
                    else:
                        return err["message"], int(
                            err["details"]["causes"][0]["message"]
                        )
        else:
            return None

    async def undeploy(self, external: bool):
        if self.client is not None:
            await self.client.api_client.close()
            self.client = None
        if self.client_ws is not None:
            await self.client_ws.api_client.close()
            self.client_ws = None
        self.configuration = None


class Helm3Connector(BaseKubernetesConnector):
    def __init__(
        self,
        deployment_name: str,
        config_dir: str,
        chart: str,
        debug: bool | None = False,
        kubeContext: str | None = None,
        kubeconfig: str | None = None,
        atomic: bool | None = False,
        caFile: str | None = None,
        certFile: str | None = None,
        depUp: bool | None = False,
        devel: bool | None = False,
        inCluster: bool | None = False,
        keepHistory: bool | None = False,
        keyFile: str | None = None,
        keyring: str | None = None,
        locationsCacheSize: int = None,
        locationsCacheTTL: int = None,
        nameTemplate: str | None = None,
        namespace: str | None = None,
        noHooks: bool | None = False,
        password: str | None = None,
        renderSubchartNotes: bool | None = False,
        repo: str | None = None,
        commandLineValues: MutableSequence[str] | None = None,
        fileValues: MutableSequence[str] | None = None,
        stringValues: MutableSequence[str] | None = None,
        registryConfig: str | None = None,
        releaseName: str | None = None,
        repositoryCache: str | None = None,
        repositoryConfig: str | None = None,
        resourcesCacheSize: int = None,
        resourcesCacheTTL: int = None,
        skipCrds: bool | None = False,
        timeout: str | None = "1000m",
        transferBufferSize: int = (32 << 20) - 1,
        username: str | None = None,
        yamlValues: MutableSequence[str] | None = None,
        verify: bool | None = False,
        chartVersion: str | None = None,
        wait: bool | None = True,
    ):
        super().__init__(
            deployment_name=deployment_name,
            config_dir=config_dir,
            inCluster=inCluster,
            kubeconfig=kubeconfig,
            namespace=namespace,
            locationsCacheSize=locationsCacheSize,
            locationsCacheTTL=locationsCacheTTL,
            resourcesCacheSize=resourcesCacheSize,
            resourcesCacheTTL=resourcesCacheTTL,
            transferBufferSize=transferBufferSize,
        )
        self.chart: str = os.path.join(self.config_dir, chart)
        self.debug: bool = debug
        self.kubeContext: str | None = kubeContext
        self.atomic: bool = atomic
        self.caFile: str | None = caFile
        self.certFile: str | None = certFile
        self.depUp: bool = depUp
        self.devel: bool = devel
        self.keepHistory: bool = keepHistory
        self.keyFile: str | None = keyFile
        self.keyring: str | None = keyring
        self.nameTemplate: str | None = nameTemplate
        self.noHooks: bool = noHooks
        self.password: str | None = password
        self.renderSubchartNotes: bool = renderSubchartNotes
        self.repo: str | None = repo
        self.commandLineValues: MutableSequence[str] | None = commandLineValues
        self.fileValues: MutableSequence[str] | None = fileValues
        self.stringValues: MutableSequence[str] | None = stringValues
        self.skipCrds: bool = skipCrds
        self.registryConfig = (
            registryConfig
            if registryConfig is not None
            else os.path.join(str(Path.home()), ".config/helm/registry.json")
        )
        self.releaseName: str = (
            releaseName if releaseName is not None else f"release-{uuid.uuid1()}"
        )
        self.repositoryCache = (
            repositoryCache
            if repositoryCache is not None
            else os.path.join(str(Path.home()), ".cache/helm/repository")
        )
        self.repositoryConfig = (
            repositoryConfig
            if repositoryConfig is not None
            else os.path.join(str(Path.home()), ".config/helm/repositories.yaml")
        )
        self.timeout: str | None = timeout
        self.username: str | None = username
        self.yamlValues: MutableSequence[str] | None = yamlValues
        self.verify: bool = verify
        self.chartVersion: str | None = chartVersion
        self.wait: bool = wait

    def base_command(self) -> str:
        return "".join(
            [
                "helm ",
                self.get_option("debug", self.debug),
                self.get_option("kube-context", self.kubeContext),
                self.get_option("kubeconfig", self.kubeconfig),
                self.get_option("namespace", self.namespace),
                self.get_option("registry-config", self.registryConfig),
                self.get_option("repository-cache", self.repositoryCache),
                self.get_option("repository-config", self.repositoryConfig),
            ]
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
                    f"Helm {version} is not compatible with Helm3Connector"
                )
            # Deploy Helm charts
            deploy_command = self.base_command() + "".join(
                [
                    "install ",
                    self.get_option("atomic", self.atomic),
                    self.get_option("ca-file", self.caFile),
                    self.get_option("cert-file", self.certFile),
                    self.get_option("dep-up", self.depUp),
                    self.get_option("devel", self.devel),
                    self.get_option("key-file", self.keyFile),
                    self.get_option("keyring", self.keyring),
                    self.get_option("name-template", self.nameTemplate),
                    self.get_option("no-hooks", self.noHooks),
                    self.get_option("password", self.password),
                    self.get_option("render-subchart-notes", self.renderSubchartNotes),
                    self.get_option("repo", self.repo),
                    self.get_option("set", self.commandLineValues),
                    self.get_option("set-file", self.fileValues),
                    self.get_option("set-string", self.stringValues),
                    self.get_option("skip-crds", self.skipCrds),
                    self.get_option("timeout", self.timeout),
                    self.get_option("username", self.username),
                    self.get_option("values", self.yamlValues),
                    self.get_option("verify", self.verify),
                    self.get_option("version", self.chartVersion),
                    self.get_option("wait", self.wait),
                    f"{self.releaseName} ",
                    self.chart,
                ]
            )
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"EXECUTING {deploy_command}")
            proc = await asyncio.create_subprocess_exec(
                *shlex.split(deploy_command),
                stderr=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
            )
            await proc.wait()

    @cachedmethod(lambda self: self.locationsCache)
    async def get_available_locations(
        self,
        service: str | None = None,
        input_directory: str | None = None,
        output_directory: str | None = None,
        tmp_directory: str | None = None,
    ) -> MutableMapping[str, AvailableLocation]:
        pods = await self.client.list_namespaced_pod(
            namespace=self.namespace or "default",
            label_selector=f"app.kubernetes.io/instance={self.releaseName}",
            field_selector="status.phase=Running",
        )
        valid_targets = {}
        for pod in pods.items:
            # Check if pod is ready
            is_ready = True
            for condition in pod.status.conditions:
                if condition.status != "True":
                    is_ready = False
                    break
            # Filter out not ready and Terminating locations
            if is_ready and pod.metadata.deletion_timestamp is None:
                for container in pod.spec.containers:
                    if not service or service == container.name:
                        location_name = pod.metadata.name + ":" + service
                        valid_targets[location_name] = AvailableLocation(
                            name=location_name,
                            deployment=self.deployment_name,
                            service=service,
                            hostname=pod.status.pod_ip,
                        )
                        break
        return valid_targets

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "helm3.json")
        )

    async def undeploy(self, external: bool) -> None:
        if not external:
            # Undeploy
            undeploy_command = self.base_command() + "".join(
                [
                    "uninstall ",
                    self.get_option("keep-history", self.keepHistory),
                    self.get_option("no-hooks", self.noHooks),
                    self.get_option("timeout", self.timeout),
                    self.releaseName,
                ]
            )
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"EXECUTING {undeploy_command}")
            proc = await asyncio.create_subprocess_exec(*shlex.split(undeploy_command))
            await proc.wait()
        # Close connections
        await super().undeploy(external)
