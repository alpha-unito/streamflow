from __future__ import annotations

import asyncio
import io
import logging
import os
import posixpath
import re
import shlex
import tarfile
import uuid
from abc import ABC, abstractmethod
from math import ceil, floor
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
from kubernetes_asyncio.client import ApiClient, Configuration, V1Container, V1PodList
from kubernetes_asyncio.config import (
    ConfigException,
    load_incluster_config,
    load_kube_config,
)
from kubernetes_asyncio.stream import WsApiClient, ws_client
from kubernetes_asyncio.utils import create_from_yaml

from streamflow.core import utils
from streamflow.core.asyncache import cachedmethod
from streamflow.core.data import StreamWrapperContext
from streamflow.core.deployment import Connector, Location
from streamflow.core.exception import (
    WorkflowDefinitionException,
    WorkflowExecutionException,
)
from streamflow.core.scheduling import AvailableLocation
from streamflow.core.utils import get_option
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


def _label_selector_as_selector(selector: Any) -> str | None:
    if not (selector.match_labels or selector.match_expressions):
        return None
    else:
        requirements = []
        for label, value in (selector.match_labels or {}).items():
            requirements.append(f"{label}={value}")
        for expr in selector.match_expressions or []:
            if expr.operator == "In":
                requirements.append(f"{expr.key} in ({','.join(expr.values)})")
            elif expr.operator == "NotIn":
                requirements.append(f"{expr.key} notin ({','.join(expr.values)})")
            elif expr.operator == "Exists":
                requirements.append(f"{expr.key}")
            elif expr.operator == "DoesNotExist":
                requirements.append(f"!{expr.key}")
            else:
                raise WorkflowDefinitionException(
                    f"Invalid Kubernetes operator `{expr.operator}` for LabelSelector"
                )
        return ",".join(requirements)


def _selector_from_set(selector: MutableMapping[str, Any]) -> str:
    requirements = []
    for label, value in (selector or {}).items():
        requirements.append(f"{label}={value}")
    return ",".join(requirements)


class KubernetesResponseWrapper(BaseStreamWrapper):
    def __init__(self, stream):
        super().__init__(stream)
        self.msg: bytes = b""

    async def read(self, size: int | None = None):
        if len(self.msg) > 0:
            if len(self.msg) > size:
                data = self.msg[0:size]
                self.msg = self.msg[size:]
                return data
            else:
                data = self.msg
                size -= len(self.msg)
                self.msg = b""
        else:
            data = b""
        while size > 0 and not self.stream.closed:
            async for msg in self.stream:
                channel = msg.data[0]
                self.msg = msg.data[1:]
                if self.msg and channel == ws_client.STDOUT_CHANNEL:
                    if len(self.msg) > size:
                        data += self.msg[0:size]
                        self.msg = self.msg[size:]
                        return data
                    else:
                        data += self.msg
                        size -= len(self.msg)
                        self.msg = b""
        return data if len(data) > 0 else None

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
        kubeContext: str | None = None,
        namespace: str | None = None,
        locationsCacheSize: int | None = None,
        locationsCacheTTL: int | None = None,
        resourcesCacheSize: int | None = None,
        resourcesCacheTTL: int | None = None,
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
            str(Path(kubeconfig).expanduser())
            if kubeconfig is not None
            else os.environ.get(
                "KUBECONFIG", os.path.join(str(Path.home()), ".kube", "config")
            )
        )
        self.kubeContext: str | None = kubeContext
        self.namespace: str | None = namespace
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
        async with self._get_stream_reader(location, src) as reader:
            try:
                async with aiotarstream.open(
                    stream=reader,
                    mode="r",
                    copybufsize=self.transferBufferSize,
                ) as tar:
                    await extract_tar_stream(tar, src, dst, self.transferBufferSize)
            except tarfile.TarError as e:
                raise WorkflowExecutionException(
                    f"Error copying {src} from location {location} to {dst}: {e}"
                ) from e

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
            write_command = " ".join(
                await utils.get_remote_to_remote_write_command(
                    src_connector=source_connector,
                    src_location=source_location,
                    src=src,
                    dst_connector=self,
                    dst_locations=locations,
                    dst=dst,
                )
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
                                        command=["sh", "-c", write_command],
                                        stderr=False,
                                        stdin=True,
                                        stdout=False,
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
                # Close writers
                await asyncio.gather(
                    *(asyncio.create_task(writer.close()) for writer in writers)
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
                    config_file=self.kubeconfig,
                    context=self.kubeContext,
                    client_configuration=self.configuration,
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
        return (
            f"kubectl "
            f"{get_option('namespace', self.namespace)}"
            f"{get_option('kubeconfig', self.kubeconfig)}"
            f"exec {pod} "
            f"{get_option('i', interactive)}"
            f"{get_option('container', container)}"
            f"-- {command}"
        )

    @abstractmethod
    async def _get_running_pods(self) -> V1PodList:
        ...

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
                    stderr=True,
                    stdin=False,
                    stdout=True,
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

    @cachedmethod(lambda self: self.locationsCache)
    async def get_available_locations(
        self,
        service: str | None = None,
        input_directory: str | None = None,
        output_directory: str | None = None,
        tmp_directory: str | None = None,
    ) -> MutableMapping[str, AvailableLocation]:
        pods = await self._get_running_pods()
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
            self.__class__.__name__,
            command,
            environment,
            workdir,
            stdin,
            stdout,
            stderr,
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
                    command=["sh", "-c", command],
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


class KubernetesConnector(BaseKubernetesConnector):
    def __init__(
        self,
        deployment_name: str,
        config_dir: str,
        files: MutableSequence[str],
        debug: bool = False,
        inCluster: bool | None = False,
        kubeconfig: str | None = None,
        kubeContext: str | None = None,
        maxConcurrentConnections: int = 4096,
        namespace: str | None = None,
        locationsCacheSize: int | None = None,
        locationsCacheTTL: int | None = None,
        resourcesCacheSize: int | None = None,
        resourcesCacheTTL: int | None = None,
        transferBufferSize: int = (2**25) - 1,
        timeout: int | None = 60000,
        wait: bool = True,
    ):
        super().__init__(
            deployment_name=deployment_name,
            config_dir=config_dir,
            inCluster=inCluster,
            kubeconfig=kubeconfig,
            kubeContext=kubeContext,
            namespace=namespace,
            locationsCacheSize=locationsCacheSize,
            locationsCacheTTL=locationsCacheTTL,
            resourcesCacheSize=resourcesCacheSize,
            resourcesCacheTTL=resourcesCacheTTL,
            transferBufferSize=transferBufferSize,
            maxConcurrentConnections=maxConcurrentConnections,
        )
        self.files = [
            f if os.path.isabs(f) else os.path.join(self.config_dir, f) for f in files
        ]
        self.debug: bool = debug
        self.timeout: int | None = timeout
        self.wait: bool = wait
        self.k8s_objects: MutableMapping[str, MutableSequence[Any]] = {}

    def _get_api(self, k8s_object: Any) -> Any:
        group, _, version = k8s_object.api_version.partition("/")
        if version == "":
            version = group
            group = "core"
        group = "".join(group.rsplit(".k8s.io", 1))
        group = "".join(word.capitalize() for word in group.split("."))
        fcn_to_call = f"{group}{version.capitalize()}Api"
        return getattr(client, fcn_to_call)(self.client.api_client)

    async def _get_running_pods(self) -> V1PodList:
        return await self.client.list_namespaced_pod(
            namespace=self.namespace or "default",
            field_selector="status.phase=Running",
        )

    async def _is_ready(self, k8s_object: Any) -> bool:
        kind = k8s_object.kind
        if kind == "Pod":
            for condition in k8s_object.status.conditions or []:
                if condition.type == "Ready" and condition.status == "True":
                    return True
            return False
        elif kind == "Deployment":
            if k8s_object.spec.paused:
                return True
            else:
                k8s_api = self._get_api(k8s_object)
                replica_sets = await k8s_api.list_namespaced_replica_set(
                    namespace=k8s_object.metadata.namespace,
                    label_selector=_label_selector_as_selector(
                        k8s_object.spec.selector
                    ),
                )
                replica_sets = [
                    rs
                    for rs in replica_sets.items
                    if (
                        k8s_object.metadata.uid
                        in [ref.uid for ref in rs.metadata.owner_references]
                    )
                ]
                if len(replica_sets) == 0:
                    return False
                else:
                    replicas = int(k8s_object.spec.replicas)
                    ready_replicas = int(k8s_object.status.ready_replicas or 0)
                    if k8s_object.spec.strategy.type == "RollingUpdate":
                        max_unavailable = (
                            k8s_object.spec.strategy.rolling_update.max_unavailable
                        )
                        if str(max_unavailable).endswith("%"):
                            max_unavailable = floor(
                                replicas * (int(max_unavailable[:-1]) / 100)
                            )
                    else:
                        max_unavailable = 0
                    return ready_replicas >= replicas - max_unavailable
        elif kind == "PersistentVolumeClaim":
            return k8s_object.status.phase == "Bound"
        elif kind == "Service":
            if k8s_object.spec.type == "ExternalName":
                return True
            elif not k8s_object.spec.cluster_ip:
                return False
            elif k8s_object.spec.type == "LoadBalancer":
                if len(k8s_object.spec.external_ips or []) > 0:
                    return True
                elif k8s_object.status.load_balancer.ingress is None:
                    return False
                else:
                    return True
            else:
                return True
        elif kind == "DaemonSet":
            replicas = int(k8s_object.status.desired_number_scheduled)
            ready_replicas = int(k8s_object.status.updated_number_scheduled or 0)
            if k8s_object.spec.update_strategy.type != "RollingUpdate":
                return True
            elif ready_replicas != replicas:
                return False
            else:
                max_unavailable = (
                    k8s_object.spec.update_trategy.rolling_update.max_unavailable
                )
                if str(max_unavailable).endswith("%"):
                    max_unavailable = ceil(replicas * (int(max_unavailable[:-1]) / 100))
                return (
                    int(k8s_object.status.number_ready or 0)
                    >= replicas - max_unavailable
                )
        elif kind == "CustomResourceDefinition":
            for condition in k8s_object.status.conditions or []:
                if condition.type == "Established":
                    if condition.status == "True":
                        return True
                elif condition.type == "NamesAccepted":
                    if condition.status == "False":
                        return True
            return False
        elif kind == "StatefulSet":
            if k8s_object.spec.update_strategy.type != "RollingUpdate":
                return True
            elif int(k8s_object.status.observed_generation or 0) < int(
                k8s_object.generation
            ):
                return False
            else:
                replicas = int(k8s_object.spec.replicas or 1)
                partition = (
                    int(k8s_object.spec.update_trategy.partition)
                    if (
                        k8s_object.spec.update_trategy.rolling_update is not None
                        and k8s_object.spec.update_trategy.partition is not None
                    )
                    else 0
                )
                if k8s_object.status.updated_replicas < (replicas - partition):
                    return False
                elif k8s_object.status.ready_replicas != replicas:
                    return False
                elif partition == 0 and (
                    k8s_object.status.current_revision
                    != k8s_object.status.update_revision
                ):
                    return False
                else:
                    return True
        elif kind == "ReplicationController":
            pods = await self.client.list_namespaced_pod(
                namespace=k8s_object.metadata.namespace,
                label_selector=_selector_from_set(k8s_object.spec.selector),
            )
            return all(
                await asyncio.gather(
                    *(asyncio.create_task(self._is_ready(p)) for p in pods)
                )
            )
        elif kind == "ReplicaSet":
            pods = await self.client.list_namespaced_pod(
                namespace=k8s_object.metadata.namespace,
                label_selector=_label_selector_as_selector(k8s_object.spec.selector),
            )
            return all(
                await asyncio.gather(
                    *(asyncio.create_task(self._is_ready(p)) for p in pods)
                )
            )
        else:
            return True

    async def _undeploy(self, k8s_object: Any):
        k8s_api = self._get_api(k8s_object)
        kind = k8s_object.kind
        kind = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", kind)
        kind = re.sub("([a-z0-9])([A-Z])", r"\1_\2", kind).lower()
        if hasattr(k8s_api, f"delete_namespaced_{kind}"):
            resp = await getattr(k8s_api, f"delete_namespaced_{kind}")(
                name=k8s_object.metadata.name, namespace=k8s_object.metadata.namespace
            )
        else:
            resp = await getattr(k8s_api, f"delete_{kind}")(
                name=k8s_object.metadata.name
            )
        if self.debug:
            print(f"{kind} deleted. status='{str(resp.status)}'")
        return resp

    async def _wait(self, k8s_object: Any) -> None:
        k8s_api = self._get_api(k8s_object)
        kind = k8s_object.kind
        kind = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", kind)
        kind = re.sub("([a-z0-9])([A-Z])", r"\1_\2", kind).lower()
        if hasattr(k8s_api, f"read_namespaced_{kind}"):
            while not await self._is_ready(
                await getattr(k8s_api, f"read_namespaced_{kind}")(
                    name=k8s_object.metadata.name,
                    namespace=k8s_object.metadata.namespace,
                )
            ):
                await asyncio.sleep(2)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"{k8s_object.metadata.name} is Ready")
        else:
            while not await self._is_ready(
                await getattr(k8s_api, f"read_{kind}")(
                    name=k8s_object.metadata.name,
                )
            ):
                await asyncio.sleep(2)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"{k8s_object.metadata.name} is Ready")

    async def deploy(self, external: bool) -> None:
        # Create clients
        await super().deploy(external)
        if not external:
            try:
                for f in self.files:
                    self.k8s_objects[f] = utils.flatten_list(
                        await create_from_yaml(
                            k8s_client=self.client.api_client,
                            yaml_file=f,
                            namespace=self.namespace or "default",
                            verbose=self.debug,
                        )
                    )
                    if self.wait:
                        if stateful_objects := [
                            obj for obj in self.k8s_objects[f] if hasattr(obj, "status")
                        ]:
                            await asyncio.wait_for(
                                asyncio.gather(
                                    *(
                                        asyncio.create_task(self._wait(obj))
                                        for obj in stateful_objects
                                    )
                                ),
                                timeout=self.timeout,
                            )
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"COMPLETED deployment of {f}")
            except BaseException as e:
                raise WorkflowExecutionException(
                    f"FAILED Deployment of {self.deployment_name} environment."
                ) from e

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "kubernetes.json")
        )

    async def undeploy(self, external: bool) -> None:
        if not external:
            try:
                for f in self.k8s_objects:
                    await asyncio.gather(
                        *(
                            asyncio.create_task(self._undeploy(k8s_object))
                            for k8s_object in self.k8s_objects[f]
                        )
                    )
            except BaseException as e:
                raise WorkflowExecutionException(
                    f"FAILED Undeployment of {self.deployment_name} environment."
                ) from e

        # Close connections
        await super().undeploy(external)


class Helm3Connector(BaseKubernetesConnector):
    def __init__(
        self,
        deployment_name: str,
        config_dir: str,
        chart: str,
        debug: bool = False,
        kubeContext: str | None = None,
        kubeconfig: str | None = None,
        atomic: bool = False,
        caFile: str | None = None,
        certFile: str | None = None,
        depUp: bool = False,
        devel: bool = False,
        inCluster: bool = False,
        keepHistory: bool = False,
        keyFile: str | None = None,
        keyring: str | None = None,
        locationsCacheSize: int = None,
        locationsCacheTTL: int = None,
        maxConcurrentConnections: int = 4096,
        nameTemplate: str | None = None,
        namespace: str | None = None,
        noHooks: bool = False,
        password: str | None = None,
        renderSubchartNotes: bool = False,
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
        skipCrds: bool = False,
        timeout: str | None = "1000m",
        transferBufferSize: int = (32 << 20) - 1,
        username: str | None = None,
        yamlValues: MutableSequence[str] | None = None,
        verify: bool = False,
        chartVersion: str | None = None,
        wait: bool = True,
    ):
        super().__init__(
            deployment_name=deployment_name,
            config_dir=config_dir,
            inCluster=inCluster,
            kubeconfig=kubeconfig,
            kubeContext=kubeContext,
            namespace=namespace,
            locationsCacheSize=locationsCacheSize,
            locationsCacheTTL=locationsCacheTTL,
            resourcesCacheSize=resourcesCacheSize,
            resourcesCacheTTL=resourcesCacheTTL,
            transferBufferSize=transferBufferSize,
            maxConcurrentConnections=maxConcurrentConnections,
        )
        self.chart: str = (
            chart if os.path.isabs(chart) else os.path.join(self.config_dir, chart)
        )
        self.debug: bool = debug
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
            str(Path(registryConfig).expanduser())
            if registryConfig is not None
            else os.path.join(str(Path.home()), ".config/helm/registry.json")
        )
        self.releaseName: str = (
            releaseName if releaseName is not None else f"release-{uuid.uuid1()}"
        )
        self.repositoryCache = (
            str(Path(repositoryCache).expanduser())
            if repositoryCache is not None
            else os.path.join(str(Path.home()), ".cache/helm/repository")
        )
        self.repositoryConfig = (
            str(Path(repositoryConfig).expanduser())
            if repositoryConfig is not None
            else os.path.join(str(Path.home()), ".config/helm/repositories.yaml")
        )
        self.timeout: str | None = timeout
        self.username: str | None = username
        self.yamlValues: MutableSequence[str] | None = yamlValues
        self.verify: bool = verify
        self.chartVersion: str | None = chartVersion
        self.wait: bool = wait

    def _get_base_command(self) -> str:
        return (
            f"helm "
            f"{get_option('debug', self.debug)}"
            f"{get_option('kube-context', self.kubeContext)}"
            f"{get_option('kubeconfig', self.kubeconfig)}"
            f"{get_option('namespace', self.namespace)}"
            f"{get_option('registry-config', self.registryConfig)}"
            f"{get_option('repository-cache', self.repositoryCache)}"
            f"{get_option('repository-config', self.repositoryConfig)}"
        )

    async def _get_running_pods(self) -> V1PodList:
        return await self.client.list_namespaced_pod(
            namespace=self.namespace or "default",
            label_selector=f"app.kubernetes.io/instance={self.releaseName}",
            field_selector="status.phase=Running",
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
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Using Helm {version}.")
            # Deploy Helm charts
            deploy_command = (
                f"{self._get_base_command()} install "
                f"{get_option('atomic', self.atomic)}"
                f"{get_option('ca-file', self.caFile)}"
                f"{get_option('cert-file', self.certFile)}"
                f"{get_option('dep-up', self.depUp)}"
                f"{get_option('devel', self.devel)}"
                f"{get_option('key-file', self.keyFile)}"
                f"{get_option('keyring', self.keyring)}"
                f"{get_option('name-template', self.nameTemplate)}"
                f"{get_option('no-hooks', self.noHooks)}"
                f"{get_option('password', self.password)}"
                f"{get_option('render-subchart-notes', self.renderSubchartNotes)}"
                f"{get_option('repo', self.repo)}"
                f"{get_option('set', self.commandLineValues)}"
                f"{get_option('set-file', self.fileValues)}"
                f"{get_option('set-string', self.stringValues)}"
                f"{get_option('skip-crds', self.skipCrds)}"
                f"{get_option('timeout', self.timeout)}"
                f"{get_option('username', self.username)}"
                f"{get_option('values', self.yamlValues)}"
                f"{get_option('verify', self.verify)}"
                f"{get_option('version', self.chartVersion)}"
                f"{get_option('wait', self.wait)}"
                f"{self.releaseName} "
                f"{self.chart}"
            )
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"EXECUTING {deploy_command}")
            proc = await asyncio.create_subprocess_exec(
                *shlex.split(deploy_command),
                stderr=asyncio.subprocess.STDOUT,
                stdout=asyncio.subprocess.PIPE,
            )
            stdout, _ = await proc.communicate()
            if proc.returncode != 0:
                raise WorkflowExecutionException(
                    f"FAILED Deployment of {self.deployment_name} environment:\n\t{stdout.decode().strip()}"
                )

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "helm3.json")
        )

    async def undeploy(self, external: bool) -> None:
        if not external:
            # Undeploy
            undeploy_command = (
                f"{self._get_base_command()}  uninstall "
                f"{get_option('keep-history', self.keepHistory)}"
                f"{get_option('no-hooks', self.noHooks)}"
                f"{get_option('timeout', self.timeout)}"
                f"{self.releaseName}"
            )
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"EXECUTING {undeploy_command}")
            proc = await asyncio.create_subprocess_exec(
                *shlex.split(undeploy_command),
                stderr=asyncio.subprocess.STDOUT,
                stdout=asyncio.subprocess.PIPE,
            )
            stdout, _ = await proc.communicate()
            if proc.returncode != 0:
                raise WorkflowExecutionException(
                    f"FAILED Undeployment of {self.deployment_name} environment:\n\t{stdout.decode().strip()}"
                )
        # Close connections
        await super().undeploy(external)
