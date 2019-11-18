import os
import shutil
import tempfile
from pathlib import Path
from threading import RLock
from typing import Optional, MutableMapping, List

from streamflow.connector.connector import ConnectorCopyKind
from streamflow.connector.deployment_manager import DeploymentManager
from streamflow.data import remote_fs
from streamflow.data.utils import RemotePath
from streamflow.log_handler import _logger
from streamflow.scheduling.scheduler import Scheduler


class DataManager(object):

    def __init__(self,
                 scheduler: Scheduler,
                 deployment_manager: DeploymentManager) -> None:
        super().__init__()
        self.lock = RLock()
        self.scheduler = scheduler
        self.deployment_manager: DeploymentManager = deployment_manager
        self.remote_paths: MutableMapping[str, List[RemotePath]] = {}

    def add_remote_path_mapping(self,
                                resource: str,
                                local_path: str,
                                remote_path: str) -> None:
        with self.lock:
            if local_path not in self.remote_paths:
                self.remote_paths[local_path] = []
            self.remote_paths[local_path].append(RemotePath(resource, remote_path))

    def _find_remote_path(self, local_path: str) -> Optional[RemotePath]:
        with self.lock:
            mappings = []
            if local_path in self.remote_paths:
                for remote_path in self.remote_paths[local_path]:
                    model = self.scheduler.get_resource(remote_path.resource).model
                    if self.deployment_manager.is_deployed(model):
                        mappings.append(remote_path)
                # TODO: select best resource
                return mappings[0]
            else:
                return None

    def collect_output(self, path: str):
        remote_path = self._find_remote_path(path)
        remote_resource = self.scheduler.get_resource(remote_path.resource)
        connector = self.deployment_manager.get_connector(remote_resource.model)
        connector.copy(remote_path.path, path, remote_path.resource,
                       ConnectorCopyKind.remoteToLocal)

    def transfer_data(self,
                      src: str,
                      dst: str,
                      target: str):
        target_resource = self.scheduler.get_resource(target)
        connector = self.deployment_manager.get_connector(target_resource.model)
        remote_fs.mkdir(connector, target, str(Path(dst).parent))
        if remote_fs.exists(connector, target, src):
            _logger.info("Path {resolved} found on {resource}".format(resolved=src, resource=target))
            if src != dst:
                connector.copy(src, dst, target, ConnectorCopyKind.remoteToRemote)
        else:
            remote_path = self._find_remote_path(src)
            if remote_path is not None:
                src_resource = self.scheduler.get_resource(remote_path.resource)
                if src_resource.model == target_resource.model:
                    _logger.info(
                        "Path {resolved} found on {resource}".format(resolved=src,
                                                                     resource=remote_path.resource))
                    connector.copy(remote_path.path, dst, target,
                                   ConnectorCopyKind.remoteToRemote, remote_path.resource)
                    return
                else:
                    source_connector = self.deployment_manager.get_connector(src_resource.model)
                    _logger.info(
                        "Path {resolved} found on {model}:{resource}".format(resolved=src,
                                                                             model=src_resource.model,
                                                                             resource=remote_path.resource))
                    temp_dir = tempfile.mkdtemp()
                    source_connector.copy(remote_path.path, temp_dir, remote_path.resource,
                                          ConnectorCopyKind.remoteToLocal)
                    for element in os.listdir(temp_dir):
                        connector.copy(os.path.join(temp_dir, element), dst, target,
                                       ConnectorCopyKind.localToRemote)
                    shutil.rmtree(temp_dir)
                    return
            _logger.info(
                "Path {resolved} not found on {resource}".format(resolved=src, resource=target))
            connector.copy(src, dst, target, ConnectorCopyKind.localToRemote)
