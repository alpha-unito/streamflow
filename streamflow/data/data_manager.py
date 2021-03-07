from __future__ import annotations

import asyncio
import os
import posixpath
import shutil
import tempfile
from pathlib import Path, PosixPath
from typing import TYPE_CHECKING

from streamflow.core.data import DataManager, DataLocation, LOCAL_RESOURCE
from streamflow.data import remotepath
from streamflow.deployment.connector.base import ConnectorCopyKind
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.workflow import Job
    from typing import Optional, MutableMapping, Set, MutableSequence
    from typing_extensions import Text


class DefaultDataManager(DataManager):

    def __init__(self, context: StreamFlowContext):
        super().__init__(context)
        self.path_mapper = RemotePathMapper()

    async def _transfer_from_resource(self,
                                      src_job: Optional[Job],
                                      src_resource: Optional[Text],
                                      src: Text,
                                      dst_job: Optional[Job],
                                      dst_resources: MutableSequence[Text],
                                      dst: Text,
                                      writable: bool):
        src_connector = src_job.step.get_connector() if src_job is not None else None
        dst_connector = dst_job.step.get_connector() if dst_job is not None else None
        # Register source path among data locations
        path_processor = os.path if src_connector is None else posixpath
        src = path_processor.abspath(src)
        # Create destination folder
        await remotepath.mkdir(dst_connector, dst_resources, str(Path(dst).parent))
        # Follow symlink for source path
        src = await remotepath.follow_symlink(src_connector, src_resource, src)
        # If jobs are both local
        if src_connector is None and dst_connector is None:
            if src != dst:
                if not writable:
                    await remotepath.symlink(None, None, src, dst)
                else:
                    if os.path.isdir(src):
                        os.makedirs(dst, exist_ok=True)
                        shutil.copytree(src, dst, dirs_exist_ok=True)
                    else:
                        shutil.copy(src, dst)
                await self.path_mapper.create_mapping(
                    src, src_job, None,
                    dst, dst_job, None,
                    valid_dst=not writable)
        # If jobs are scheduled on the same model
        elif src_connector == dst_connector:
            remote_resources = []
            mapping_tasks = []
            for dst_resource in dst_resources:
                # If jobs are scheduled on the same resource and it is possible to link, only create a symlink
                if not writable and len(dst_resources) == 1 and src_resource == dst_resource:
                    if src != dst:
                        await remotepath.symlink(dst_connector, dst_resource, src, dst)
                        mapping_tasks.append(asyncio.create_task(
                            self.path_mapper.create_mapping(
                                src, src_job, src_resource,
                                dst, dst_job, dst_resource)))
                # Otherwise perform a remote copy managed by the connector
                else:
                    remote_resources.append(dst_resource)
            await asyncio.gather(*mapping_tasks)
            if remote_resources:
                await dst_connector.copy(src, dst, remote_resources, ConnectorCopyKind.REMOTE_TO_REMOTE, src_resource)
                # Register the new remote copies of the data
                await asyncio.gather(*[asyncio.create_task(
                    self.path_mapper.create_mapping(
                        src, src_job, src_resource,
                        dst, dst_job, dst_resource,
                        valid_dst=not writable)
                ) for dst_resource in remote_resources])
        # If source job is local, copy files to the remote resources
        elif src_connector is None:
            await dst_connector.copy(src, dst, dst_resources, ConnectorCopyKind.LOCAL_TO_REMOTE)
            # Register the new remote copies of the data
            await asyncio.gather(*[asyncio.create_task(
                self.path_mapper.create_mapping(
                    src, src_job, src_resource,
                    dst, dst_job, dst_resource,
                    valid_dst=not writable)
            ) for dst_resource in dst_resources])
        # If destination job is local, copy files from the remote resource
        elif dst_connector is None:
            await src_connector.copy(src, dst, [src_resource], ConnectorCopyKind.REMOTE_TO_LOCAL)
            # Register the new local copy of the data
            await self.path_mapper.create_mapping(
                src, src_job, src_resource,
                dst, dst_job, None,
                valid_dst=not writable)
        # If jobs are both remote and scheduled on different models, perform an intermediate local copy
        else:
            temp_dir = tempfile.mkdtemp()
            await src_connector.copy(src, temp_dir, [src_resource], ConnectorCopyKind.REMOTE_TO_LOCAL)
            await asyncio.gather(*[asyncio.create_task(dst_connector.copy(
                os.path.join(temp_dir, element), dst, dst_resources, ConnectorCopyKind.LOCAL_TO_REMOTE)
            ) for element in os.listdir(temp_dir)])
            shutil.rmtree(temp_dir)
            # Register the new remote copies of the data
            await asyncio.gather(*[asyncio.create_task(
                self.path_mapper.create_mapping(
                    src, src_job, src_resource,
                    dst, dst_job, dst_resource,
                    valid_dst=not writable)
            ) for dst_resource in dst_resources])

    def get_data_locations(self, resource: Text, path: Text) -> Set[DataLocation]:
        data_locations = self.path_mapper.get(resource, path)
        return set(filter(lambda l: l.valid, data_locations))

    def invalidate_location(self, resource: Text, path: Text) -> None:
        self.path_mapper.invalidate_location(resource, path)

    async def register_path(self,
                            job: Optional[Job],
                            resource: Optional[Text],
                            path: Text):
        await self.path_mapper.create_mapping(path, job, resource)
        self.context.checkpoint_manager.register_path(job, path)

    async def transfer_data(self,
                            src: Text,
                            src_job: Optional[Job],
                            dst: Text,
                            dst_job: Optional[Job],
                            writable: bool = False):
        # Get connectors and resources from steps
        src_connector = src_job.step.get_connector() if src_job is not None else None
        src_resources = src_job.get_resources() if src_job is not None else []
        dst_connector = dst_job.step.get_connector() if dst_job is not None else None
        dst_resources = dst_job.get_resources() if dst_job is not None else []
        src_found = False
        # If source file is local, simply transfer it
        if not src_resources:
            if await remotepath.exists(src_connector, None, src):
                src_found = True
                await self._transfer_from_resource(
                    src_job, None, src,
                    dst_job, dst_resources, dst,
                    writable)
        # Otherwise process each source resource that actually contains the source path
        else:
            for src_resource in src_resources:
                if await remotepath.exists(src_connector, src_resource, src):
                    src_found = True
                    await self._transfer_from_resource(
                        src_job, src_resource, src,
                        dst_job, dst_resources, dst,
                        writable)
        # If source path does not exist
        if not src_found:
            # Search it on destination resources
            for dst_resource in dst_resources:
                # If it exists, switch source connector and resources with the new ones
                if await remotepath.exists(dst_connector, dst_resource, src):
                    logger.debug("Path {path} found {resource}.".format(
                        path=src,
                        resource="on resource {resource}".format(
                            resource=dst_resource) if dst_resource is not None else "on local file-system"
                    ))
                    await self._transfer_from_resource(
                        dst_job, dst_resource, src,
                        dst_job, dst_resources, dst,
                        writable)
                    break


class RemotePathNode(object):
    __slots__ = ('children', 'locations')

    def __init__(self):
        self.children: MutableMapping[Text, RemotePathNode] = {}
        self.locations: Set[DataLocation] = set()


class RemotePathMapper(object):

    def __init__(self):
        self._filesystems: MutableMapping[Text, RemotePathNode] = {LOCAL_RESOURCE: RemotePathNode()}

    def _process_resource(self, resource: Text) -> Text:
        if resource is None:
            resource = LOCAL_RESOURCE
        if resource not in self._filesystems:
            self._filesystems[resource] = RemotePathNode()
        return resource

    async def create_mapping(self,
                             src_path: Text,
                             src_job: Optional[Job],
                             src_resource: Optional[Text],
                             dst_path: Optional[Text] = None,
                             dst_job: Optional[Job] = None,
                             dst_resource: Optional[Text] = None,
                             valid_dst: bool = True):
        src_resource = self._process_resource(src_resource)
        src_data_location = DataLocation(
            path=src_path,
            job=src_job.name if src_job is not None else None,
            resource=src_resource)
        src_connector = src_job.step.get_connector() if src_job is not None else None
        src_path = await remotepath.follow_symlink(src_connector, src_resource, src_path)
        data_locations = self.get(src_resource, src_path)
        if src_data_location not in data_locations:
            data_locations.add(src_data_location)
        if dst_path is not None:
            dst_resource = self._process_resource(dst_resource)
            dst_data_location = DataLocation(
                path=dst_path,
                job=dst_job.name if dst_job is not None else None,
                resource=dst_resource,
                valid=valid_dst)
            if dst_data_location not in data_locations:
                data_locations.add(dst_data_location)
        self.put(src_resource, src_path, data_locations)
        if dst_path is not None:
            self.put(dst_resource, dst_path, data_locations)

    def _remove_node(self, resource: Text, node: RemotePathNode):
        node.locations = set(filter(lambda l: l.resource != resource, node.locations))
        for location in node.locations:
            self.put(location.resource, location.path, node.locations)
        for n in node.children.values():
            self._remove_node(resource, n)
        return node

    def get(self, resource: Text, path: Text) -> Set[DataLocation]:
        resource = resource or LOCAL_RESOURCE
        node = self._filesystems[resource]
        path = Path(path) if resource == LOCAL_RESOURCE else PosixPath(path)
        for token in path.parts:
            if token in node.children:
                node = node.children[token]
            else:
                return set()
        return node.locations

    def invalidate_location(self, resource: Optional[Text], path: Text) -> None:
        resource = resource or LOCAL_RESOURCE
        locations = self.get(resource, path)
        for location in locations:
            if location.resource == resource:
                location.valid = False
        self.put(resource, path, locations)

    def put(self, resource: Text, path: Text, data_locations: Set[DataLocation]) -> None:
        resource = resource or LOCAL_RESOURCE
        node = self._filesystems[resource]
        path = Path(path) if resource == LOCAL_RESOURCE else PosixPath(path)
        for token in path.parts:
            if token not in node.children:
                node.children[token] = RemotePathNode()
            node = node.children[token]
        node.locations = data_locations

    def remove_resource(self, resource: Text):
        if resource in self._filesystems:
            node = self._filesystems[resource]
            self._remove_node(resource, node)
            del self._filesystems[resource]
