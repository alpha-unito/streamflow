from __future__ import annotations

import asyncio
import io
import os
import posixpath
import shlex
import subprocess
import tarfile
import tempfile
import uuid
from abc import ABC
from typing import MutableMapping, MutableSequence, Optional, Any, Tuple, Union
from urllib.parse import urlencode

import yaml
from cachetools import Cache, TTLCache
from kubernetes_asyncio import client
from kubernetes_asyncio.client import Configuration, ApiClient, V1Container
from kubernetes_asyncio.config import incluster_config, ConfigException, load_kube_config
from kubernetes_asyncio.stream import WsApiClient, ws_client
from typing_extensions import Text

from streamflow.core import utils
from streamflow.core.asyncache import cachedmethod
from streamflow.core.scheduling import Resource
from streamflow.deployment.connector.base import BaseConnector
from streamflow.log_handler import logger

SERVICE_NAMESPACE_FILENAME = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"


class PatchedInClusterConfigLoader(incluster_config.InClusterConfigLoader):

    def load_and_set(self, configuration: Optional[Configuration] = None):
        self._load_config()
        self._set_config(configuration)

    def _set_config(self, configuration: Optional[Configuration] = None):
        if configuration is None:
            super()._set_config()
        configuration.host = self.host
        configuration.ssl_ca_cert = self.ssl_ca_cert
        configuration.api_key['authorization'] = "bearer " + self.token


class PatchedWsApiClient(WsApiClient):

    async def request(self, method, url, query_params=None, headers=None,
                      post_params=None, body=None, _preload_content=True,
                      _request_timeout=None):
        if query_params:
            new_query_params = []
            for key, value in query_params:
                if key == 'command' and isinstance(value, list):
                    for command in value:
                        new_query_params.append((key, command))
                else:
                    new_query_params.append((key, value))
            query_params = new_query_params
        if headers is None:
            headers = {}
        if 'sec-websocket-protocol' not in headers:
            headers['sec-websocket-protocol'] = 'v4.channel.k8s.io'

        if query_params:
            url += '?' + urlencode(query_params)

        url = ws_client.get_websocket_url(url)

        if _preload_content:

            resp_all = ''
            async with self.rest_client.pool_manager.ws_connect(
                    url,
                    headers=headers,
                    heartbeat=30) as ws:
                async for msg in ws:
                    msg = msg.data.decode('utf-8')
                    if len(msg) > 1:
                        channel = ord(msg[0])
                        data = msg[1:]
                        if data:
                            if channel in [ws_client.STDOUT_CHANNEL, ws_client.STDERR_CHANNEL]:
                                resp_all += data

            return ws_client.WsResponse(resp_all.encode('utf-8'))

        else:

            return await self.rest_client.pool_manager.ws_connect(url, headers=headers, heartbeat=30)


class BaseHelmConnector(BaseConnector, ABC):

    def __init__(self,
                 streamflow_config_dir: Text,
                 inCluster: Optional[bool] = False,
                 kubeconfig: Optional[Text] = os.path.join(os.environ['HOME'], ".kube", "config"),
                 namespace: Optional[Text] = None,
                 releaseName: Optional[Text] = "release-%s" % str(uuid.uuid1()),
                 resourcesCacheTTL: int = 10,
                 transferBufferSize: int = (2 ** 25) - 1,
                 maxConcurrentConnections: int = 4096):
        super().__init__(
            streamflow_config_dir=streamflow_config_dir,
            transferBufferSize=transferBufferSize)
        self.inCluster = inCluster
        self.kubeconfig = kubeconfig
        self.namespace = namespace
        self.releaseName = releaseName
        self.resourcesCache: Cache = TTLCache(maxsize=10, ttl=resourcesCacheTTL)
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
                                     src: Text,
                                     dst: Text,
                                     resources: MutableSequence[Text],
                                     source_remote: Text,
                                     read_only: bool = False) -> None:
        effective_resources = await self._get_effective_resources(resources, dst)
        await super()._copy_remote_to_remote(
            src=src,
            dst=dst,
            resources=effective_resources,
            source_remote=source_remote,
            read_only=read_only)

    async def _copy_local_to_remote(self,
                                    src: Text,
                                    dst: Text,
                                    resources: MutableSequence[Text],
                                    read_only: bool = False):
        effective_resources = await self._get_effective_resources(resources, dst)
        await super()._copy_local_to_remote(
            src=src,
            dst=dst,
            resources=effective_resources,
            read_only=read_only)

    async def _copy_local_to_remote_single(self,
                                           resource: Text,
                                           tar_buffer: io.BufferedRandom,
                                           read_only: bool = False) -> None:
        resource_buffer = io.BufferedReader(tar_buffer.raw)
        pod, container = resource.split(':')
        command = ["tar", "xf", "-", "-C", "/"]
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
        while content := resource_buffer.read(self.transferBufferSize):
            channel_prefix = bytes(chr(ws_client.STDIN_CHANNEL), "ascii")
            payload = channel_prefix + content
            await response.send_bytes(payload)
        await response.close()

    async def _copy_remote_to_local(self,
                                    src: Text,
                                    dst: Text,
                                    resource: Text,
                                    read_only: bool = False):
        pod, container = resource.split(':')
        command = ["tar", "chf", "-", "-C", "/", posixpath.relpath(src, '/')]
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

    async def _get_container(self, resource: Text) -> Tuple[Text, V1Container]:
        pod_name, container_name = resource.split(':')
        pod = await self.client.read_namespaced_pod(name=pod_name, namespace=self.namespace or 'default')
        for container in pod.spec.containers:
            if container.name == container_name:
                return container.name, container

    async def _get_configuration(self) -> Configuration:
        if self.configuration is None:
            self.configuration = Configuration()
            if self.inCluster:
                loader = PatchedInClusterConfigLoader(token_filename=incluster_config.SERVICE_TOKEN_FILENAME,
                                                      cert_filename=incluster_config.SERVICE_CERT_FILENAME)
                loader.load_and_set(configuration=self.configuration)
                self._configure_incluster_namespace()
            else:
                await load_kube_config(config_file=self.kubeconfig, client_configuration=self.configuration)
        return self.configuration

    async def _get_effective_resources(self,
                                       resources: MutableSequence[Text],
                                       dest_path: Text,
                                       source_remote: Optional[Text] = None) -> MutableSequence[Text]:
        # Get containers
        container_tasks = []
        for resource in resources:
            container_tasks.append(asyncio.create_task(self._get_container(resource)))
        containers = {k: v for (k, v) in await asyncio.gather(*container_tasks)}
        # Check if some resources share volume mounts to the same path
        common_paths = {}
        effective_resources = []
        for resource in resources:
            container = containers[resource.split(':')[1]]
            add_resource = True
            for volume in container.volume_mounts:
                if dest_path.startswith(volume.mount_path):
                    path = ':'.join([volume.name, dest_path])
                    if path not in common_paths:
                        common_paths[path] = resource
                    elif resource == source_remote:
                        effective_resources.remove(common_paths[path])
                        common_paths[path] = resource
                    else:
                        add_resource = False
                    break
            if add_resource:
                effective_resources.append(resource)
        return effective_resources

    async def deploy(self, external: bool):
        # Init standard client
        configuration = await self._get_configuration()
        configuration.connection_pool_maxsize = self.maxConcurrentConnections
        self.client = client.CoreV1Api(api_client=ApiClient(configuration=configuration))
        # Init WebSocket client
        configuration = await self._get_configuration()
        configuration.connection_pool_maxsize = self.maxConcurrentConnections
        ws_api_client = PatchedWsApiClient(configuration=configuration)
        ws_api_client.set_default_header('Connection', 'upgrade,keep-alive')
        self.client_ws = client.CoreV1Api(api_client=ws_api_client)

    @cachedmethod(lambda self: self.resourcesCache)
    async def get_available_resources(self, service: Text) -> MutableMapping[Text, Resource]:
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
            # Filter out not ready and Terminating resources
            if is_ready and pod.metadata.deletion_timestamp is None:
                for container in pod.spec.containers:
                    if not service or service == container.name:
                        resource_name = pod.metadata.name + ':' + service
                        valid_targets[resource_name] = Resource(name=resource_name, hostname=pod.status.pod_ip)
                        break
        return valid_targets

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
        command = utils.create_command(
            command, environment, workdir, stdin, stdout, stderr)
        logger.debug("Executing command {command} on {resource} {job}".format(
            command=command,
            resource=resource,
            job="for job {job}".format(job=job_name) if job_name else ""))
        if encode:
            command = utils.encode_command(command)
        pod, container = resource.split(':')
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


class Helm2Connector(BaseHelmConnector):

    def __init__(self,
                 streamflow_config_dir: Text,
                 chart: Text,
                 debug: Optional[bool] = False,
                 home: Optional[Text] = os.path.join(os.environ['HOME'], ".helm"),
                 kubeContext: Optional[Text] = None,
                 kubeconfig: Optional[Text] = None,
                 tillerConnectionTimeout: Optional[int] = None,
                 tillerNamespace: Optional[Text] = None,
                 atomic: Optional[bool] = False,
                 caFile: Optional[Text] = None,
                 certFile: Optional[Text] = None,
                 depUp: Optional[bool] = False,
                 description: Optional[Text] = None,
                 devel: Optional[bool] = False,
                 inCluster: Optional[bool] = False,
                 init: Optional[bool] = False,
                 keyFile: Optional[Text] = None,
                 keyring: Optional[Text] = None,
                 releaseName: Optional[Text] = None,
                 nameTemplate: Optional[Text] = None,
                 namespace: Optional[Text] = None,
                 noCrdHook: Optional[bool] = False,
                 noHooks: Optional[bool] = False,
                 password: Optional[Text] = None,
                 renderSubchartNotes: Optional[bool] = False,
                 repo: Optional[Text] = None,
                 resourcesCacheTTL: int = 10,
                 commandLineValues: Optional[MutableSequence[Text]] = None,
                 fileValues: Optional[MutableSequence[Text]] = None,
                 stringValues: Optional[MutableSequence[Text]] = None,
                 timeout: Optional[int] = str(60000),
                 tls: Optional[bool] = False,
                 tlscacert: Optional[Text] = None,
                 tlscert: Optional[Text] = None,
                 tlshostname: Optional[Text] = None,
                 tlskey: Optional[Text] = None,
                 tlsverify: Optional[bool] = False,
                 username: Optional[Text] = None,
                 yamlValues: Optional[MutableSequence[Text]] = None,
                 verify: Optional[bool] = False,
                 chartVersion: Optional[Text] = None,
                 wait: Optional[bool] = True,
                 purge: Optional[bool] = True,
                 transferBufferSize: int = (2 ** 25) - 1):
        super().__init__(
            streamflow_config_dir=streamflow_config_dir,
            inCluster=inCluster,
            kubeconfig=kubeconfig,
            namespace=namespace,
            releaseName=releaseName,
            resourcesCacheTTL=resourcesCacheTTL,
            transferBufferSize=transferBufferSize
        )
        self.chart = os.path.join(streamflow_config_dir, chart)
        self.debug = debug
        self.home = home
        self.kubeContext = kubeContext
        self.tillerConnectionTimeout = tillerConnectionTimeout
        self.tillerNamespace = tillerNamespace
        self.atomic = atomic
        self.caFile = caFile
        self.certFile = certFile
        self.depUp = depUp
        self.description = description
        self.devel = devel
        self.keyFile = keyFile
        self.keyring = keyring
        self.nameTemplate = nameTemplate
        self.noCrdHook = noCrdHook
        self.noHooks = noHooks
        self.password = password
        self.renderSubchartNotes = renderSubchartNotes
        self.repo = repo
        self.commandLineValues = commandLineValues
        self.fileValues = fileValues
        self.stringValues = stringValues
        self.tlshostname = tlshostname
        self.username = username
        self.yamlValues = yamlValues
        self.verify = verify
        self.chartVersion = chartVersion
        self.wait = wait
        self.purge = purge
        self.timeout = timeout
        self.tls = tls
        self.tlscacert = tlscacert
        self.tlscert = tlscert
        self.tlskey = tlskey
        self.tlsverify = tlsverify
        if init:
            self._init_helm()

    def _init_helm(self):
        init_command = self.base_command() + "".join([
            "init "
            "--upgrade "
            "{wait}"
        ]).format(
            wait=self.get_option("wait", self.wait)
        )
        logger.debug("Executing {command}".format(command=init_command))
        return subprocess.run(shlex.split(init_command))

    def base_command(self):
        return (
            "helm "
            "{debug}"
            "{home}"
            "{kubeContext}"
            "{kubeconfig}"
            "{tillerConnectionTimeout}"
            "{tillerNamespace}"
        ).format(
            debug=self.get_option("debug", self.debug),
            home=self.get_option("home", self.home),
            kubeContext=self.get_option("kube-context", self.kubeContext),
            kubeconfig=self.get_option("kubeconfig", self.kubeconfig),
            tillerConnectionTimeout=self.get_option("tiller-connection-timeout", self.tillerConnectionTimeout),
            tillerNamespace=self.get_option("tiller-namespace", self.tillerNamespace)
        )

    async def deploy(self, external: bool) -> None:
        # Create clients
        await super().deploy(external)
        if not external:
            # Deploy Helm charts
            deploy_command = self.base_command() + "".join([
                "install "
                "{atomic}"
                "{caFile}"
                "{certFile}"
                "{depUp}"
                "{description}"
                "{devel}"
                "{keyFile}"
                "{keyring}"
                "{releaseName}"
                "{nameTemplate}"
                "{namespace}"
                "{noCrdHook}"
                "{noHooks}"
                "{password}"
                "{renderSubchartNotes}"
                "{repo}"
                "{commandLineValues}"
                "{fileValues}"
                "{stringValues}"
                "{timeout}"
                "{tls}"
                "{tlscacert}"
                "{tlscert}"
                "{tlshostname}"
                "{tlskey}"
                "{tlsverify}"
                "{username}"
                "{yamlValues}"
                "{verify}"
                "{chartVersion}"
                "{wait}"
                "{chart}"
            ]).format(
                atomic=self.get_option("atomic", self.atomic),
                caFile=self.get_option("ca-file", self.caFile),
                certFile=self.get_option("cert-file", self.certFile),
                depUp=self.get_option("dep-up", self.depUp),
                description=self.get_option("description", self.description),
                devel=self.get_option("devel", self.devel),
                keyFile=self.get_option("key-file", self.keyFile),
                keyring=self.get_option("keyring", self.keyring),
                releaseName=self.get_option("name", self.releaseName),
                nameTemplate=self.get_option("name-template", self.nameTemplate),
                namespace=self.get_option("namespace", self.namespace),
                noCrdHook=self.get_option("no-crd-hook", self.noCrdHook),
                noHooks=self.get_option("no-hooks", self.noHooks),
                password=self.get_option("password", self.password),
                renderSubchartNotes=self.get_option("render-subchart-notes", self.renderSubchartNotes),
                repo=self.get_option("repo", self.repo),
                commandLineValues=self.get_option("set", self.commandLineValues),
                fileValues=self.get_option("set-file", self.fileValues),
                stringValues=self.get_option("set-string", self.stringValues),
                timeout=self.get_option("timeout", self.timeout),
                tls=self.get_option("tls", self.tls),
                tlscacert=self.get_option("tls-ca-cert", self.tlscacert),
                tlscert=self.get_option("tls-cert", self.tlscert),
                tlshostname=self.get_option("tls-hostname", self.tlshostname),
                tlskey=self.get_option("tls-key", self.tlskey),
                tlsverify=self.get_option("tls-verify", self.tlsverify),
                username=self.get_option("username", self.username),
                yamlValues=self.get_option("values", self.yamlValues),
                verify=self.get_option("verify", self.verify),
                chartVersion=self.get_option("version", self.chartVersion),
                wait=self.get_option("wait", self.wait),
                chart="\"{chart}\"".format(chart=self.chart)
            )
            logger.debug("Executing {command}".format(command=deploy_command))
            proc = await asyncio.create_subprocess_exec(*shlex.split(deploy_command))
            await proc.wait()

    async def undeploy(self, external: bool) -> None:
        if not external:
            undeploy_command = self.base_command() + (
                "delete "
                "{description}"
                "{noHooks}"
                "{purge}"
                "{timeout}"
                "{tls}"
                "{tlscacert}"
                "{tlscert}"
                "{tlshostname}"
                "{tlskey}"
                "{tlsverify}"
                "{releaseName}"
            ).format(
                description=self.get_option("description", self.description),
                noHooks=self.get_option("no-hooks", self.noHooks),
                timeout=self.get_option("timeout", self.timeout),
                purge=self.get_option("purge", self.purge),
                tls=self.get_option("tls", self.tls),
                tlscacert=self.get_option("tls-ca-cert", self.tlscacert),
                tlscert=self.get_option("tls-cert", self.tlscert),
                tlshostname=self.get_option("tls-hostname", self.tlshostname),
                tlskey=self.get_option("tls-key", self.tlskey),
                tlsverify=self.get_option("tls-verify", self.tlsverify),
                releaseName=self.releaseName
            )
            logger.debug("Executing {command}".format(command=undeploy_command))
            proc = await asyncio.create_subprocess_exec(*shlex.split(undeploy_command))
            await proc.wait()
        # Close connections
        await super().undeploy(external)


class Helm3Connector(BaseHelmConnector):
    def __init__(self,
                 streamflow_config_dir: Text,
                 chart: Text,
                 debug: Optional[bool] = False,
                 kubeContext: Optional[Text] = None,
                 kubeconfig: Optional[Text] = None,
                 atomic: Optional[bool] = False,
                 caFile: Optional[Text] = None,
                 certFile: Optional[Text] = None,
                 depUp: Optional[bool] = False,
                 devel: Optional[bool] = False,
                 inCluster: Optional[bool] = False,
                 keepHistory: Optional[bool] = False,
                 keyFile: Optional[Text] = None,
                 keyring: Optional[Text] = None,
                 releaseName: Optional[Text] = None,
                 nameTemplate: Optional[Text] = None,
                 namespace: Optional[Text] = None,
                 noHooks: Optional[bool] = False,
                 password: Optional[Text] = None,
                 renderSubchartNotes: Optional[bool] = False,
                 repo: Optional[Text] = None,
                 commandLineValues: Optional[MutableSequence[Text]] = None,
                 fileValues: Optional[MutableSequence[Text]] = None,
                 registryConfig: Optional[Text] = os.path.join(os.environ['HOME'], ".config/helm/registry.json"),
                 repositoryCache: Optional[Text] = os.path.join(os.environ['HOME'], ".cache/helm/repository"),
                 repositoryConfig: Optional[Text] = os.path.join(os.environ['HOME'], ".config/helm/repositories.yaml"),
                 resourcesCacheTTL: int = 10,
                 stringValues: Optional[MutableSequence[Text]] = None,
                 skipCrds: Optional[bool] = False,
                 timeout: Optional[Text] = "1000m",
                 username: Optional[Text] = None,
                 yamlValues: Optional[MutableSequence[Text]] = None,
                 verify: Optional[bool] = False,
                 chartVersion: Optional[Text] = None,
                 wait: Optional[bool] = True,
                 transferBufferSize: int = (32 << 20) - 1):
        super().__init__(
            streamflow_config_dir=streamflow_config_dir,
            inCluster=inCluster,
            kubeconfig=kubeconfig,
            namespace=namespace,
            releaseName=releaseName,
            resourcesCacheTTL=resourcesCacheTTL,
            transferBufferSize=transferBufferSize
        )
        self.chart = os.path.join(streamflow_config_dir, chart)
        self.debug = debug
        self.kubeContext = kubeContext
        self.atomic = atomic
        self.caFile = caFile
        self.certFile = certFile
        self.depUp = depUp
        self.devel = devel
        self.keepHistory = keepHistory
        self.keyFile = keyFile
        self.keyring = keyring
        self.nameTemplate = nameTemplate
        self.noHooks = noHooks
        self.password = password
        self.renderSubchartNotes = renderSubchartNotes
        self.repo = repo
        self.commandLineValues = commandLineValues
        self.fileValues = fileValues
        self.stringValues = stringValues
        self.skipCrds = skipCrds
        self.registryConfig = registryConfig
        self.repositoryCache = repositoryCache
        self.repositoryConfig = repositoryConfig
        self.username = username
        self.yamlValues = yamlValues
        self.verify = verify
        self.chartVersion = chartVersion
        self.wait = wait
        self.timeout = timeout

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

    async def undeploy(self, external: bool) -> None:
        if not external:
            # Undeploy model
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
