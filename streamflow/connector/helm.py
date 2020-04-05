import base64
import io
import os
import posixpath
import select
import shlex
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import types
import uuid
from abc import ABC
from pathlib import Path
from typing import MutableMapping, List, Optional, Any

import six
from kubernetes import client, config, stream
from kubernetes.client import Configuration, ApiClient
from kubernetes.config import incluster_config, ConfigException
from kubernetes.stream.ws_client import STDOUT_CHANNEL, STDERR_CHANNEL
from websocket import ABNF

from streamflow.connector import connector
from streamflow.connector.connector import Connector
from streamflow.log_handler import _logger

SERVICE_NAMESPACE_FILENAME = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

def patched_write_channel(self, channel, data):
    """Write data to a channel."""
    # check if we're writing binary data or not
    binary = six.PY3 and type(data) == six.binary_type
    opcode = ABNF.OPCODE_BINARY if binary else ABNF.OPCODE_TEXT

    channel_prefix = chr(channel)
    if binary:
        channel_prefix = six.binary_type(channel_prefix, "ascii")

    payload = channel_prefix + data
    self.sock.send(payload, opcode=opcode)


def patched_update(self, timeout=0):
    """Update channel buffers with at most one complete frame of input."""
    if not self.is_open():
        return
    if not self.sock.connected:
        self._connected = False
        return
    r, _, _ = select.select(
        (self.sock.sock,), (), (), timeout)
    if r:
        op_code, frame = self.sock.recv_data_frame(True)
        if op_code == ABNF.OPCODE_CLOSE:
            self._connected = False
            return
        elif op_code == ABNF.OPCODE_BINARY or op_code == ABNF.OPCODE_TEXT:
            data = frame.data
            if six.PY3 and op_code == ABNF.OPCODE_TEXT:
                data = data.decode("utf-8", "replace")
                if len(data) > 1:
                    channel = ord(data[0])
                    data = data[1:]
                    if data:
                        if channel in [STDOUT_CHANNEL, STDERR_CHANNEL]:
                            self._all.write(data)
                        if channel not in self._channels:
                            self._channels[channel] = data
                        else:
                            self._channels[channel] += data
            elif op_code == ABNF.OPCODE_BINARY:
                if len(data) > 1:
                    channel = data[0]
                    data = data[1:]
                    if len(data) > 0:
                        if channel in [STDOUT_CHANNEL, STDERR_CHANNEL]:
                            self._all.write(data)
                        if channel not in self._channels:
                            self._channels[channel] = data
                        else:
                            self._channels[channel] += data


def patched_read_all(self):
    """Return buffered data received on stdout and stderr channels.
    This is useful for non-interactive call where a set of command passed
    to the API call and their result is needed after the call is concluded.
    Should be called after run_forever() or update()
    """
    out = self._all.getvalue()
    self._all = self._all.__class__()
    self._channels = {}
    return out


def patch_response(response):
    response._all = six.BytesIO()
    response.write_channel = types.MethodType(patched_write_channel, response)
    response.update = types.MethodType(patched_update, response)
    response.read_all = types.MethodType(patched_read_all, response)
    return response


class BaseHelmConnector(Connector, ABC):

    def __init__(self,
                 inCluster: Optional[bool] = False,
                 kubeconfig: Optional[str] = os.path.join(os.environ['HOME'], ".kube", "config"),
                 namespace: Optional[str] = None,
                 releaseName: Optional[str] = "release-%s" % str(uuid.uuid1()),
                 transferBufferSize: Optional[int] = (32 << 20) - 1):
        super().__init__()
        self.inCluster = inCluster
        self.kubeconfig = kubeconfig
        self.namespace = namespace
        self.releaseName = releaseName
        self.transferBufferSize = transferBufferSize
        self._create_client()

    def __getstate__(self):
        keys_blacklist = ['kubectl']
        return dict((k, v) for (k, v) in self.__dict__.items() if k not in keys_blacklist)

    def __setstate__(self, state):
        self.__dict__ = state
        self._create_client()

    def _create_client(self):
        if self.inCluster:
            loader = incluster_config.InClusterConfigLoader(token_filename=incluster_config.SERVICE_TOKEN_FILENAME,
                                                            cert_filename=incluster_config.SERVICE_CERT_FILENAME)
            loader._load_config()
            configuration = Configuration()
            configuration.host = loader.host
            configuration.ssl_ca_cert = loader.ssl_ca_cert
            configuration.api_key['authorization'] = "bearer " + loader.token
            self.kubectl = client.CoreV1Api(api_client=ApiClient(configuration=configuration))
            self._configure_incluster_namespace()
        else:
            self.kubectl = client.CoreV1Api(api_client=config.new_client_from_config(config_file=self.kubeconfig))

    def _configure_incluster_namespace(self):
        if self.namespace is None:
            if not os.path.isfile(SERVICE_NAMESPACE_FILENAME):
                raise ConfigException(
                    "Service namespace file does not exists.")

            with open(SERVICE_NAMESPACE_FILENAME) as f:
                self.namespace = f.read()
                if not self.namespace:
                    raise ConfigException("Namespace file exists but empty.")

    def _build_helper_file(self,
                           target: str,
                           environment: MutableMapping[str, str] = None,
                           workdir: str = None
                           ) -> str:
        file_contents = "".join([
            '#!/bin/sh\n',
            '{environment}',
            '{workdir}',
            'sh -c "$(echo $@ | base64 --decode)"\n'
        ]).format(
            environment="".join(["export %s=\"%s\"\n" % (key, value) for (key, value) in
                                 environment.items()]) if environment is not None else "",
            workdir="cd {workdir}\n".format(workdir=workdir) if workdir is not None else ""
        )
        file_name = tempfile.mktemp()
        with open(file_name, mode='w') as file:
            file.write(file_contents)
        os.chmod(file_name, os.stat(file_name).st_mode | stat.S_IEXEC)
        parent_directory = str(Path(file_name).parent)
        pod, container = target.split(':')
        stream.stream(self.kubectl.connect_get_namespaced_pod_exec,
                      name=pod,
                      namespace=self.namespace or 'default',
                      container=container,
                      command=["mkdir", "-p", parent_directory],
                      stderr=True,
                      stdin=False,
                      stdout=True,
                      tty=False)
        self._copy_local_to_remote(file_name, file_name, target)
        return file_name

    def _copy_remote_to_remote(self, src: str, dst: str, resource: str, source_remote: str) -> None:
        source_remote = source_remote or resource
        if source_remote == resource:
            if src != dst:
                command = ['/bin/cp', "-rf", src, dst]
                self.run(resource, command)
                return
        else:
            temp_dir = tempfile.mkdtemp()
            self._copy_remote_to_local(src, temp_dir, source_remote)
            for element in os.listdir(temp_dir):
                self._copy_local_to_remote(os.path.join(temp_dir, element), dst, resource)
            shutil.rmtree(temp_dir)

    def _copy_local_to_remote(self, src: str, dst: str, resource: str):
        pod, container = resource.split(':')
        command = ['tar', 'xf', '-', '-C', '/']
        response = stream.stream(self.kubectl.connect_get_namespaced_pod_exec,
                                 name=pod,
                                 namespace=self.namespace or 'default',
                                 container=container,
                                 command=command,
                                 stderr=True,
                                 stdin=True,
                                 stdout=True,
                                 tty=False,
                                 _preload_content=False)
        with tempfile.TemporaryFile() as tar_buffer:
            with tarfile.open(fileobj=tar_buffer, mode='w') as tar:
                tar.add(src, arcname=dst)
            tar_buffer.seek(0)
            while response.is_open():
                content = tar_buffer.read(self.transferBufferSize)
                response.update(timeout=1)
                if content:
                    response = patch_response(response)
                    response.write_stdin(content)
                else:
                    break
            response.close()

    def _copy_remote_to_local(self, src: str, dst: str, resource: str):
        pod, container = resource.split(':')
        command = ['tar', 'cPf', '-', src]
        response = stream.stream(self.kubectl.connect_get_namespaced_pod_exec,
                                 name=pod,
                                 namespace=self.namespace or 'default',
                                 container=container,
                                 command=command,
                                 stderr=True,
                                 stdin=True,
                                 stdout=True,
                                 tty=False,
                                 _preload_content=False)
        with io.BytesIO() as byte_buffer:
            while response.is_open():
                response = patch_response(response)
                response.update(timeout=1)
                if response.peek_stdout():
                    out = response.read_stdout()
                    byte_buffer.write(out)
            response.close()
            byte_buffer.flush()
            byte_buffer.seek(0)
            with tarfile.open(fileobj=byte_buffer, mode='r:') as tar:
                for member in tar.getmembers():
                    if os.path.isdir(dst):
                        if member.path == src:
                            member.path = posixpath.basename(member.path)
                        else:
                            member.path = posixpath.relpath(member.path, src)
                        tar.extract(member, dst)
                    elif member.isfile():
                        with tar.extractfile(member) as inputfile:
                            with open(dst, 'wb') as outputfile:
                                outputfile.write(inputfile.read())
                    else:
                        parent_dir = str(Path(dst).parent)
                        member.path = posixpath.relpath(member.path, src)
                        tar.extract(member, parent_dir)

    def get_available_resources(self, service):
        pods = self.kubectl.list_namespaced_pod(
            namespace=self.namespace or 'default',
            label_selector="app.kubernetes.io/instance={}".format(self.releaseName),
            field_selector="status.phase=Running"
        )
        valid_targets = []
        for pod in pods.items:
            if pod.metadata.deletion_timestamp is not None:
                continue
            for container in pod.spec.containers:
                if service == container.name:
                    valid_targets.append(pod.metadata.name + ':' + service)
                    break
        return valid_targets

    def get_runtime(self,
                    resource: str,
                    environment: MutableMapping[str, str] = None,
                    workdir: str = None
                    ) -> str:
        args = {'resource': resource, 'environment': environment, 'workdir': workdir}
        return self._run_current_file(self, __file__, args)

    def run(self,
            resource: str,
            command: List[str],
            environment: MutableMapping[str, str] = None,
            workdir: str = None,
            capture_output: bool = False) -> Optional[Any]:
        helper_file_name = \
            self._build_helper_file(resource, environment, workdir)
        _logger.debug("Executing {command}".format(command=command, resource=resource))
        command = [helper_file_name, base64.b64encode(" ".join(command).encode('utf-8')).decode('utf-8')]
        pod, container = resource.split(':')
        response = stream.stream(self.kubectl.connect_get_namespaced_pod_exec,
                                 name=pod,
                                 namespace=self.namespace or 'default',
                                 container=container,
                                 command=command,
                                 stderr=True,
                                 stdin=False,
                                 stdout=True,
                                 tty=False)
        if capture_output:
            return response


class Helm2Connector(BaseHelmConnector):

    def __init__(self,
                 config_file: MutableMapping[str, str],
                 chart: str,
                 debug: Optional[bool] = False,
                 home: Optional[str] = os.path.join(os.environ['HOME'], ".helm"),
                 kubeContext: Optional[str] = None,
                 kubeconfig: Optional[str] = None,
                 tillerConnectionTimeout: Optional[int] = None,
                 tillerNamespace: Optional[str] = None,
                 atomic: Optional[bool] = False,
                 caFile: Optional[str] = None,
                 certFile: Optional[str] = None,
                 depUp: Optional[bool] = False,
                 description: Optional[str] = None,
                 devel: Optional[bool] = False,
                 inCluster: Optional[bool] = False,
                 init: Optional[bool] = False,
                 keyFile: Optional[str] = None,
                 keyring: Optional[str] = None,
                 releaseName: Optional[str] = None,
                 nameTemplate: Optional[str] = None,
                 namespace: Optional[str] = None,
                 noCrdHook: Optional[bool] = False,
                 noHooks: Optional[bool] = False,
                 password: Optional[str] = None,
                 renderSubchartNotes: Optional[bool] = False,
                 repo: Optional[str] = None,
                 commandLineValues: Optional[List[str]] = None,
                 fileValues: Optional[List[str]] = None,
                 stringValues: Optional[List[str]] = None,
                 timeout: Optional[int] = str(60000),
                 tls: Optional[bool] = False,
                 tlscacert: Optional[str] = None,
                 tlscert: Optional[str] = None,
                 tlshostname: Optional[str] = None,
                 tlskey: Optional[str] = None,
                 tlsverify: Optional[bool] = False,
                 username: Optional[str] = None,
                 yamlValues: Optional[List[str]] = None,
                 verify: Optional[bool] = False,
                 chartVersion: Optional[str] = None,
                 wait: Optional[bool] = True,
                 purge: Optional[bool] = True,
                 transferBufferSize: Optional[int] = None
                 ):
        super().__init__(
            inCluster=inCluster,
            kubeconfig=kubeconfig,
            namespace=namespace,
            releaseName=releaseName,
            transferBufferSize=transferBufferSize
        )
        config_dir = config_file['dirname']
        self.chart = os.path.join(config_dir, chart)
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
        _logger.debug("Executing {command}".format(command=init_command))
        return subprocess.run(shlex.split(init_command), check=True)

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

    def deploy(self) -> subprocess.CompletedProcess:
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
        _logger.debug("Executing {command}".format(command=deploy_command))
        return subprocess.run(shlex.split(deploy_command), check=True)

    def undeploy(self) -> subprocess.CompletedProcess:
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
        _logger.debug("Executing {command}".format(command=undeploy_command))
        return subprocess.run(shlex.split(undeploy_command), check=True)


class Helm3Connector(BaseHelmConnector):
    def __init__(self,
                 config_file: MutableMapping[str, str],
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
                 releaseName: Optional[str] = None,
                 nameTemplate: Optional[str] = None,
                 namespace: Optional[str] = None,
                 noHooks: Optional[bool] = False,
                 password: Optional[str] = None,
                 renderSubchartNotes: Optional[bool] = False,
                 repo: Optional[str] = None,
                 commandLineValues: Optional[List[str]] = None,
                 fileValues: Optional[List[str]] = None,
                 registryConfig: Optional[str] = os.path.join(os.environ['HOME'], ".config/helm/registry.json"),
                 repositoryCache: Optional[str] = os.path.join(os.environ['HOME'], ".cache/helm/repository"),
                 repositoryConfig: Optional[str] = os.path.join(os.environ['HOME'], ".config/helm/repositories.yaml"),
                 stringValues: Optional[List[str]] = None,
                 skipCrds: Optional[bool] = False,
                 timeout: Optional[str] = "1000m",
                 username: Optional[str] = None,
                 yamlValues: Optional[List[str]] = None,
                 verify: Optional[bool] = False,
                 chartVersion: Optional[str] = None,
                 wait: Optional[bool] = True,
                 transferBufferSize: Optional[int] = None
                 ):
        super().__init__(
            inCluster=inCluster,
            kubeconfig=kubeconfig,
            namespace=namespace,
            releaseName=releaseName,
            transferBufferSize=transferBufferSize
        )
        config_dir = config_file['dirname']
        self.chart = os.path.join(config_dir, chart)
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
            debug=self.get_option("debug", self.debug), \
            kubeContext=self.get_option("kube-context", self.kubeContext),
            kubeconfig=self.get_option("kubeconfig", self.kubeconfig),
            namespace=self.get_option("namespace", self.namespace),
            registryConfig=self.get_option("registry-config", self.registryConfig),
            repositoryCache=self.get_option("repository-cache", self.repositoryCache),
            repositoryConfig=self.get_option("repository-config", self.repositoryConfig),
        )

    def deploy(self) -> subprocess.CompletedProcess:
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
        _logger.debug("Executing {command}".format(command=deploy_command))
        return subprocess.run(shlex.split(deploy_command), check=True)

    def undeploy(self) -> subprocess.CompletedProcess:
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
        _logger.debug("Executing {command}".format(command=undeploy_command))
        return subprocess.run(shlex.split(undeploy_command), check=True)


if __name__ == "__main__":
    connector.run_script(sys.argv[1:])
