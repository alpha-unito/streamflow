import base64
import os
import posixpath
import random
import re
import string
import sys
from datetime import datetime
from typing import List, MutableMapping, Optional, Any

import paramiko
from ruamel.yaml import YAML
from scp import SCPClient

from streamflow.connector import connector
from streamflow.connector.connector import Connector
from streamflow.log_handler import _logger


def _parse_hostname(hostname):
    if ':' in hostname:
        hostname, port = hostname.split(':')
        port = int(port)
    else:
        port = 22
    return hostname, port


class OccamConnector(Connector):
    _occam_jobs_table = {}

    @classmethod
    def _add_service_job_id(cls, service, job_id):
        if service not in cls._occam_jobs_table:
            cls._occam_jobs_table[service] = []
        cls._occam_jobs_table[service].append(job_id)

    @classmethod
    def _get_service_from_job_id(cls, job_id):
        for service in cls._occam_jobs_table:
            if job_id in cls._occam_jobs_table[service]:
                return service

    @classmethod
    def _get_job_ids_from_service(cls, service):
        return cls._occam_jobs_table.get(service, [])

    def __init__(self,
                 config_file: MutableMapping[str, str],
                 file: str,
                 sshKey: str,
                 username: str,
                 sshKeyPassphrase: Optional[str] = None,
                 hostname: Optional[str] = "occam.c3s.unito.it",
                 socketTimeout: Optional[float] = 60.0
                 ) -> None:
        super().__init__()
        config_dir = config_file['dirname']
        yaml = YAML(typ='safe')
        with open(os.path.join(config_dir, file)) as f:
            self.env_description = yaml.load(f)
        self.hostname = hostname
        self.sshKey = sshKey
        self.sshKeyPassphrase = sshKeyPassphrase
        self.username = username
        self.socketTimeout = socketTimeout
        self.archive_home = '/archive/home/{username}'.format(username=username)
        self.scratch_home = '/scratch/home/{username}'.format(username=username)
        self.generator = random.Random(datetime.now())
        self.ssh_client = self._open_ssh_client()

    def __getstate__(self):
        keys_blacklist = ['generator', 'ssh_client']
        serialized_obj = dict((k, v) for (k, v) in self.__dict__.items() if k not in keys_blacklist)
        serialized_obj['_occam_jobs_table'] = self._occam_jobs_table
        return serialized_obj

    def __setstate__(self, state):
        type(self)._occam_jobs_table = state.pop('_occam_jobs_table')
        self.__dict__ = state
        self.generator = random.Random(datetime.now())
        self.ssh_client = self._open_ssh_client()

    def _open_ssh_client(self):
        (hostname, port) = _parse_hostname(self.hostname)
        ssh_client = paramiko.SSHClient()
        ssh_client.load_system_host_keys()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(
            hostname=hostname,
            port=port,
            username=self.username,
            key_filename=self.sshKey,
            passphrase=self.sshKeyPassphrase
        )
        return ssh_client

    def _open_occam_shell(self, resource):
        open_shell_command = "exec occam-shell {resource}".format(resource=resource)
        return self.ssh_client.exec_command(open_shell_command, get_pty=True)

    def _get_tmpdir(self):
        temp_dir = posixpath.join(self.scratch_home, 'streamflow',
                                  "".join([self.generator.choice(string.ascii_letters) for _ in range(12)]))
        stdin_, stdout_, stderr_ = self.ssh_client.exec_command('mkdir -p {dir}'.format(dir=temp_dir))
        stdout_.channel.recv_exit_status()
        return temp_dir

    def _get_persistent_path(self, resource: str, path: str) -> Optional[str]:
        if path.startswith(self.archive_home) or path.startswith(self.scratch_home):
            return path
        service = OccamConnector._get_service_from_job_id(resource)
        volumes = self.env_description[service].get('volumes', [])
        for volume in volumes:
            local, remote = volume.split(':')
            if path.startswith(remote):
                return posixpath.normpath(posixpath.join(local, posixpath.relpath(path, remote)))
        return None

    def _copy_remote_to_remote(self, src: str, dst: str, resource: str, source_remote: str) -> None:
        if source_remote == resource:
            command = ['/bin/cp', "-rf", src, dst]
            self.run(resource, command)
        else:
            temp_dir = self._get_tmpdir()
            copy1_command = ['/bin/cp', '-rf', src, temp_dir]
            self.run(source_remote, copy1_command)
            copy2_command = ['/bin/cp', '-rf', temp_dir + "/*", dst, '&&',
                             'rm', '-rf', temp_dir]
            self.run(resource, copy2_command)

    def _copy_local_to_remote(self, src: str, dst: str, resource: str):
        persistent_path = self._get_persistent_path(resource, dst)
        if persistent_path is not None:
            with SCPClient(
                    transport=self.ssh_client.get_transport(),
                    socket_timeout=self.socketTimeout
            ) as scp_client:
                scp_client.put(src, recursive=True, remote_path=persistent_path)
        else:
            temp_dir = self._get_tmpdir()
            with SCPClient(
                    transport=self.ssh_client.get_transport(),
                    socket_timeout=self.socketTimeout
            ) as scp_client:
                scp_client.put(src, recursive=True, remote_path=temp_dir)
            copy_command = ['/bin/cp', "-rf", temp_dir + "/*", dst, '&&',
                            'rm', '-rf', temp_dir]
            self.run(resource, copy_command)

    def _copy_remote_to_local(self, src: str, dst: str, resource: str):
        persistent_path = self._get_persistent_path(resource, src)
        if persistent_path is not None:
            with SCPClient(
                    transport=self.ssh_client.get_transport(),
                    socket_timeout=self.socketTimeout
            ) as scp_client:
                scp_client.get(persistent_path, recursive=True, local_path=dst)
        else:
            temp_dir = self._get_tmpdir()
            copy_command = ['/bin/cp', "-rf", src, temp_dir, '&&',
                            'find', temp_dir, '-maxdepth', '1', '-mindepth', '1', '-exec', 'basename', '{}', '\\;']
            contents = self.run(resource, copy_command, capture_output=True).split()
            with SCPClient(
                    transport=self.ssh_client.get_transport(),
                    socket_timeout=self.socketTimeout
            ) as scp_client:
                for content in contents:
                    scp_client.get(posixpath.join(temp_dir, content), recursive=True, local_path=dst)
            delete_command = ['rm', '-rf', temp_dir]
            self.run(resource, delete_command)

    def deploy(self) -> None:
        for (name, service) in self.env_description.items():
            nodes = service.get('nodes', ['node22'])
            for node in nodes:
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
                    command=" ".join(service.get('command')) if 'command' in service else ""
                )
                _logger.debug("Executing {command}".format(command=deploy_command))
                stdin_, stdout_, stderr_ = self.ssh_client.exec_command(deploy_command)
                stdout_.channel.recv_exit_status()
                output = stdout_.read().decode('utf-8')
                search_result = re.findall('({node}-[0-9]+).*'.format(node=node), output, re.MULTILINE)
                if search_result:
                    OccamConnector._add_service_job_id(name, search_result[0])
                    _logger.info("Deployed {name} on {resource}".format(name=name, resource=search_result[0]))
                else:
                    raise Exception

    def get_available_resources(self, service: str) -> List[str]:
        return OccamConnector._get_job_ids_from_service(service)

    def get_runtime(self, resource: str, environment: MutableMapping[str, str] = None, workdir: str = None) -> str:
        args = {'resource': resource, 'environment': environment, 'workdir': workdir}
        return self._run_current_file(self, __file__, args)

    def undeploy(self) -> None:
        for name in self._occam_jobs_table:
            for job_id in self._occam_jobs_table[name]:
                undeploy_command = "".join([
                    "occam-kill ",
                    "{job_id}"
                ]).format(
                    job_id=job_id
                )
                _logger.debug("Executing {command}".format(command=undeploy_command))
                stdin_, stdout_, stderr_ = self.ssh_client.exec_command(undeploy_command)
                stdout_.channel.recv_exit_status()
                _logger.info("Killed {resource}".format(resource=job_id))

    def run(self, resource: str, command: List[str], environment: MutableMapping[str, str] = None, workdir: str = None,
            capture_output: bool = False) -> Optional[Any]:
        exec_command = "".join(
            "{workdir}"
            "{environment}"
            "{command}"
        ).format(
            resource=resource,
            workdir="cd {workdir} && ".format(workdir=workdir) if workdir is not None else "",
            environment="".join(["export %s=%s && " % (key, value) for (key, value) in
                                 environment.items()]) if environment is not None else "",
            command=" ".join(command)
        )
        occam_command = "".join(
            "occam-exec "
            "{resource} "
            "sh -c "
            "\"$(echo {command} | base64 --decode)\""
        ).format(
            resource=resource,
            command=base64.b64encode(exec_command.encode('utf-8')).decode('utf-8')
        )
        _logger.debug("Executing {command}".format(command=occam_command))
        stdin_, stdout_, stderr_ = self.ssh_client.exec_command(occam_command)
        stdout_.channel.recv_exit_status()
        if capture_output:
            stdin_.channel.shutdown_write()
            lines = (line for line in stdout_.readlines())
            out = ""
            for line in lines:
                if line.startswith("Trying to exec commands into container"):
                    break
            try:
                out = next(lines)
            except StopIteration:
                return out
            for line in lines:
                out = "\n".join([out, line])
            return out


if __name__ == "__main__":
    connector.run_script(sys.argv[1:])
