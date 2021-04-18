from __future__ import annotations

import asyncio
import os
import posixpath
import shutil
import tempfile
from pathlib import Path, PosixPath
from typing import TYPE_CHECKING

from streamflow.core.data import DataManager, DataLocation, LOCAL_RESOURCE, DataLocationType
from streamflow.core.deployment import Connector
from streamflow.data import remotepath
from streamflow.deployment.connector.base import ConnectorCopyKind
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.workflow import Job
    from typing import Optional, MutableMapping, Set, MutableSequence
    from typing_extensions import Text


async def _copy(src_connector: Optional[Connector],
                src_resource: Optional[Text],
                src: Text,
                dst_connector: Optional[Connector],
                dst_resources: Optional[MutableSequence[Text]],
                dst: Text,
                writable: False) -> None:
    if src_connector is None and dst_connector is None:
        if os.path.isdir(src):
            os.makedirs(dst, exist_ok=True)
            shutil.copytree(src, dst, dirs_exist_ok=True)
        else:
            shutil.copy(src, dst)
    elif src_connector == dst_connector:
        await dst_connector.copy(
            src=src,
            dst=dst,
            resources=dst_resources,
            kind=ConnectorCopyKind.REMOTE_TO_REMOTE,
            source_remote=src_resource,
            read_only=not writable)
    elif src_connector is None:
        await dst_connector.copy(
            src=src,
            dst=dst,
            resources=dst_resources,
            kind=ConnectorCopyKind.LOCAL_TO_REMOTE,
            read_only=not writable)
    elif dst_connector is None:
        await src_connector.copy(
            src=src,
            dst=dst,
            resources=[src_resource],
            kind=ConnectorCopyKind.REMOTE_TO_LOCAL,
            read_only=not writable)
    else:
        temp_dir = tempfile.mkdtemp()
        await src_connector.copy(
            src=src,
            dst=temp_dir,
            resources=[src_resource],
            kind=ConnectorCopyKind.REMOTE_TO_LOCAL,
            read_only=not writable)
        await asyncio.gather(*[asyncio.create_task(dst_connector.copy(
            src=os.path.join(temp_dir, element),
            dst=dst,
            resources=dst_resources,
            kind=ConnectorCopyKind.LOCAL_TO_REMOTE,
            read_only=not writable
        )) for element in os.listdir(temp_dir)])
        shutil.rmtree(temp_dir)


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
        primary_locations = self.path_mapper.get(src_resource, src, DataLocationType.PRIMARY)
        copy_tasks = []
        remote_resources = []
        data_locations = []
        for dst_resource in (dst_resources or [None]):
            # Check if a primary copy of the source path is already present on the destination resource
            found_existing_loc = False
            for primary_loc in primary_locations:
                if primary_loc.resource == (dst_resource or LOCAL_RESOURCE):
                    # Wait for the source location to be available on the destination path
                    await primary_loc.available.wait()
                    # If yes, perform a symbolic link if possible
                    if not writable:
                        await remotepath.symlink(dst_connector, dst_resource, primary_loc.path, dst)
                        self.path_mapper.create_mapping(
                            location_type=DataLocationType.SYMBOLIC_LINK,
                            src_path=src,
                            src_resource=src_resource,
                            dst_path=dst,
                            dst_job=dst_job,
                            dst_resource=dst_resource,
                            available=True)
                    # Otherwise, perform a copy operation
                    else:
                        copy_tasks.append(asyncio.create_task(_copy(
                            src_connector=dst_connector,
                            src_resource=dst_resource,
                            src=primary_loc.path,
                            dst_connector=dst_connector,
                            dst_resources=[dst_resource],
                            dst=dst,
                            writable=True)))
                        data_locations.append(self.path_mapper.create_mapping(
                            location_type=DataLocationType.WRITABLE_COPY,
                            src_path=src,
                            src_resource=src_resource,
                            dst_path=dst,
                            dst_job=dst_job,
                            dst_resource=dst_resource))
                    found_existing_loc = True
                    break
            # Otherwise, perform a remote copy and mark the destination as primary
            if not found_existing_loc:
                remote_resources.append(dst_resource)
                data_locations.append(self.path_mapper.create_mapping(
                    location_type=DataLocationType.WRITABLE_COPY if writable else DataLocationType.PRIMARY,
                    src_path=src,
                    src_resource=src_resource,
                    dst_path=dst,
                    dst_job=dst_job,
                    dst_resource=dst_resource))
        # Perform all the copy operations
        if remote_resources:
            copy_tasks.append(asyncio.create_task(_copy(
                src_connector=src_connector,
                src_resource=src_resource,
                src=src,
                dst_connector=dst_connector,
                dst_resources=remote_resources,
                dst=dst,
                writable=writable)))
        await asyncio.gather(*copy_tasks)
        # Mark all destination data locations as available
        for data_location in data_locations:
            data_location.available.set()

    def get_data_locations(self,
                           resource: Text,
                           path: Text,
                           location_type: Optional[DataLocationType] = None) -> Set[DataLocation]:
        data_locations = self.path_mapper.get(resource, path, location_type)
        return {loc for loc in data_locations if loc.location_type != DataLocationType.INVALID}

    def invalidate_location(self, resource: Text, path: Text) -> None:
        self.path_mapper.invalidate_location(resource, path)

    def register_path(self,
                      job: Optional[Job],
                      resource: Optional[Text],
                      path: Text):
        self.path_mapper.put(
            resource=resource,
            path=path,
            data_locations={DataLocation(
                path=path,
                job=job.name if job is not None else None,
                resource=resource or LOCAL_RESOURCE,
                location_type=DataLocationType.PRIMARY,
                available=True)})
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

    def create_mapping(self,
                       location_type: DataLocationType,
                       src_path: Text,
                       src_resource: Optional[Text],
                       dst_path: Text,
                       dst_job: Optional[Job],
                       dst_resource: Optional[Text],
                       available: bool = False) -> DataLocation:
        src_resource = self._process_resource(src_resource)
        data_locations = self.get(src_resource, src_path)
        dst_resource = self._process_resource(dst_resource)
        dst_data_location = DataLocation(
            path=dst_path,
            job=dst_job.name if dst_job is not None else None,
            location_type=location_type,
            resource=dst_resource,
            available=available)
        if dst_data_location not in data_locations:
            data_locations.add(dst_data_location)
        self.put(dst_resource, dst_path, data_locations)
        return dst_data_location

    def _remove_node(self, resource: Text, node: RemotePathNode):
        node.locations = set(filter(lambda l: l.resource != resource, node.locations))
        for location in node.locations:
            self.put(location.resource, location.path, node.locations)
        for n in node.children.values():
            self._remove_node(resource, n)
        return node

    def get(self, resource: Text, path: Text, location_type: Optional[DataLocationType] = None) -> Set[DataLocation]:
        resource = resource or LOCAL_RESOURCE
        node = self._filesystems.get(resource)
        if not node:
            return set()
        path = Path(path) if resource == LOCAL_RESOURCE else PosixPath(path)
        for token in path.parts:
            if token in node.children:
                node = node.children[token]
            else:
                return set()
        return ({loc for loc in node.locations if loc.location_type == location_type} if location_type
                else node.locations)

    def invalidate_location(self, resource: Optional[Text], path: Text) -> None:
        resource = resource or LOCAL_RESOURCE
        locations = self.get(resource, path)
        for location in locations:
            if location.resource == resource:
                location.location_type = DataLocationType.INVALID
        self.put(resource, path, locations)

    def put(self, resource: Text, path: Text, data_locations: Set[DataLocation]) -> None:
        resource = self._process_resource(resource)
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
