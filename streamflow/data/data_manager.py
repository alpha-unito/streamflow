from __future__ import annotations

import asyncio
import os
import posixpath
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

import pkg_resources

from streamflow.core import utils
from streamflow.core.data import DataLocation, DataManager, DataType, LOCAL_LOCATION
from streamflow.core.deployment import Connector
from streamflow.data import remotepath
from streamflow.deployment.connector.base import ConnectorCopyKind
from streamflow.deployment.connector.local import LocalConnector

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from typing import Optional, MutableMapping, Set, MutableSequence


async def _copy(src_connector: Optional[Connector],
                src_location: Optional[str],
                src: str,
                dst_connector: Optional[Connector],
                dst_locations: Optional[MutableSequence[str]],
                dst: str,
                writable: False) -> None:
    if isinstance(src_connector, LocalConnector):
        await dst_connector.copy(
            src=src,
            dst=dst,
            locations=dst_locations,
            kind=ConnectorCopyKind.LOCAL_TO_REMOTE,
            read_only=not writable)
    elif isinstance(dst_connector, LocalConnector):
        await src_connector.copy(
            src=src,
            dst=dst,
            locations=[src_location],
            kind=ConnectorCopyKind.REMOTE_TO_LOCAL,
            read_only=not writable)
    else:
        await dst_connector.copy(
            src=src,
            dst=dst,
            locations=dst_locations,
            kind=ConnectorCopyKind.REMOTE_TO_REMOTE,
            source_connector=src_connector,
            source_location=src_location,
            read_only=not writable)


class DefaultDataManager(DataManager):

    def __init__(self, context: StreamFlowContext):
        super().__init__(context)
        self.path_mapper = RemotePathMapper(context)

    async def _transfer_from_location(self,
                                      src_connector: Connector,
                                      src_location: str,
                                      src: str,
                                      dst_connector: Connector,
                                      dst_locations: MutableSequence[str],
                                      dst: str,
                                      writable: bool):
        # Register source path among data locations
        path_processor = os.path if isinstance(src_connector, LocalConnector) is None else posixpath
        src = path_processor.abspath(src)
        # Create destination folder
        await remotepath.mkdir(dst_connector, dst_locations, str(Path(dst).parent))
        # Follow symlink for source path
        src = await remotepath.follow_symlink(src_connector, src_location, src)
        primary_locations = self.path_mapper.get(src, DataType.PRIMARY)
        copy_tasks = []
        remote_locations = []
        data_locations = []
        for dst_location in dst_locations:
            # Check if a primary copy of the source path is already present on the destination location
            found_existing_loc = False
            for primary_loc in primary_locations:
                if primary_loc.location == dst_location:
                    # Wait for the source location to be available on the destination path
                    await primary_loc.available.wait()
                    # If yes, perform a symbolic link if possible
                    if not writable:
                        await remotepath.symlink(dst_connector, dst_location, primary_loc.path, dst)
                        self.path_mapper.create_and_map(
                            location_type=DataType.SYMBOLIC_LINK,
                            src_path=src,
                            dst_path=dst,
                            dst_deployment=dst_connector.deployment_name,
                            dst_location=dst_location,
                            available=True)
                    # Otherwise, perform a copy operation
                    else:
                        copy_tasks.append(asyncio.create_task(_copy(
                            src_connector=dst_connector,
                            src_location=dst_location,
                            src=primary_loc.path,
                            dst_connector=dst_connector,
                            dst_locations=[dst_location],
                            dst=dst,
                            writable=True)))
                        data_locations.append(self.path_mapper.put(
                            path=dst,
                            data_location=DataLocation(
                                path=dst,
                                relpath=list(self.path_mapper.get(src))[0].relpath,
                                deployment=dst_connector.deployment_name,
                                data_type=DataType.PRIMARY,
                                location=dst_location,
                                available=False)))
                    found_existing_loc = True
                    break
            # Otherwise, perform a remote copy and mark the destination as primary
            if not found_existing_loc:
                remote_locations.append(dst_location)
                if writable:
                    data_locations.append(self.path_mapper.put(
                        path=dst,
                        data_location=DataLocation(
                            path=dst,
                            relpath=list(self.path_mapper.get(src))[0].relpath,
                            deployment=dst_connector.deployment_name,
                            data_type=DataType.PRIMARY,
                            location=dst_location,
                            available=False)))
                else:
                    data_locations.append(self.path_mapper.create_and_map(
                        location_type=DataType.PRIMARY,
                        src_path=src,
                        dst_path=dst,
                        dst_deployment=dst_connector.deployment_name,
                        dst_location=dst_location))
        # Perform all the copy operations
        if remote_locations:
            copy_tasks.append(asyncio.create_task(_copy(
                src_connector=src_connector,
                src_location=src_location,
                src=src,
                dst_connector=dst_connector,
                dst_locations=remote_locations,
                dst=dst,
                writable=writable)))
        await asyncio.gather(*copy_tasks)
        # Mark all destination data locations as available
        for data_location in data_locations:
            data_location.available.set()

    async def close(self):
        pass

    def get_data_locations(self,
                           path: str,
                           deployment: Optional[str] = None,
                           location: Optional[str] = None,
                           location_type: Optional[DataType] = None) -> Set[DataLocation]:
        data_locations = self.path_mapper.get(path, location_type)
        data_locations = {loc for loc in data_locations if loc.data_type != DataType.INVALID}
        if deployment is not None:
            data_locations = {loc for loc in data_locations if loc.deployment == deployment}
        if location is not None:
            data_locations = {loc for loc in data_locations if loc.location == location}
        return data_locations

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join('schemas', 'data_manager.json'))

    def get_source_location(self,
                            path: str,
                            dst_deployment: str) -> Optional[DataLocation]:
        if data_locations := self.get_data_locations(path=path):
            dst_connector = self.context.deployment_manager.get_connector(dst_deployment)
            same_connector_locations = {loc for loc in data_locations if
                                        loc.deployment == dst_connector.deployment_name}
            if same_connector_locations:
                for loc in same_connector_locations:
                    if loc.data_type == DataType.PRIMARY:
                        return loc
                return list(same_connector_locations)[0]
            else:
                local_locations = {loc for loc in data_locations if isinstance(
                                   self.context.deployment_manager.get_connector(loc.deployment), LocalConnector)}
                if local_locations:
                    for loc in local_locations:
                        if loc.data_type == DataType.PRIMARY:
                            return loc
                    return list(local_locations)[0]
                else:
                    for loc in data_locations:
                        if loc.data_type == DataType.PRIMARY:
                            return loc
                    return list(data_locations)[0]
        else:
            return None

    def invalidate_location(self, location: str, path: str) -> None:
        self.path_mapper.invalidate_location(location, path)

    def register_path(self,
                      deployment: str,
                      location: str,
                      path: str,
                      relpath: Optional[str] = None,
                      data_type: DataType = DataType.PRIMARY) -> DataLocation:
        data_location = DataLocation(
            path=path,
            relpath=relpath or path,
            deployment=deployment,
            location=location or LOCAL_LOCATION,
            data_type=data_type,
            available=True)
        self.path_mapper.put(
            path=path,
            data_location=data_location,
            recursive=True)
        self.context.checkpoint_manager.register(data_location)
        return data_location

    def register_relation(self,
                          src_location: DataLocation,
                          dst_location: DataLocation) -> None:
        data_locations = self.path_mapper.get(src_location.path)
        for data_location in list(data_locations):
            self.path_mapper.put(data_location.path, dst_location)
            self.path_mapper.put(dst_location.path, data_location)

    async def transfer_data(self,
                            src_deployment: str,
                            src_locations: MutableSequence[str],
                            src_path: str,
                            dst_deployment: str,
                            dst_locations: MutableSequence[str],
                            dst_path: str,
                            writable: bool = False):
        # Get connectors and locations from steps
        src_connector = self.context.deployment_manager.get_connector(src_deployment)
        dst_connector = self.context.deployment_manager.get_connector(dst_deployment)
        await asyncio.gather(*(asyncio.create_task(self._transfer_from_location(
                src_connector, src_location, src_path,
                dst_connector, dst_locations, dst_path,
                writable)) for src_location in src_locations))


class RemotePathNode(object):
    __slots__ = ('children', 'locations')

    def __init__(self):
        self.children: MutableMapping[str, RemotePathNode] = {}
        self.locations: Set[DataLocation] = set()


class RemotePathMapper(object):

    def __init__(self,
                 context: StreamFlowContext):
        self._filesystem: RemotePathNode = RemotePathNode()
        self.context: StreamFlowContext = context

    def _remove_node(self, location: DataLocation, node: RemotePathNode):
        node.locations.remove(location)
        for n in node.children.values():
            self._remove_node(location, n)

    def create_and_map(self,
                       location_type: DataType,
                       src_path: str,
                       dst_path: str,
                       dst_deployment: str,
                       dst_location: Optional[str],
                       available: bool = False) -> DataLocation:
        data_locations = self.get(src_path)
        dst_data_location = DataLocation(
            path=dst_path,
            relpath=list(data_locations)[0].relpath,
            deployment=dst_deployment,
            data_type=location_type,
            location=dst_location,
            available=available)
        for data_location in list(data_locations):
            self.put(data_location.path, dst_data_location)
            self.put(dst_path, data_location)
        self.put(dst_path, dst_data_location)
        return dst_data_location

    def get(self, path: str, location_type: Optional[DataType] = None) -> Set[DataLocation]:
        path = PurePosixPath(Path(path).as_posix())
        node = self._filesystem
        for token in path.parts:
            if token in node.children:
                node = node.children[token]
            else:
                return set()
        return ({loc for loc in node.locations if loc.data_type == location_type} if location_type
                else node.locations)

    def invalidate_location(self, location: str, path: str) -> None:
        path = PurePosixPath(Path(path).as_posix())
        node = self._filesystem
        for token in path.parts:
            node = node.children[token]
        for loc in node.locations:
            if loc.location == location:
                loc.data_type = DataType.INVALID

    def put(self, path: str, data_location: DataLocation, recursive: bool = False) -> DataLocation:
        path = PurePosixPath(Path(path).as_posix())
        path_processor = utils.get_path_processor(
            self.context.deployment_manager.get_connector(data_location.deployment))
        node = self._filesystem
        nodes = {}
        # Create or navigate hierarchy
        for i, token in enumerate(path.parts):
            node = node.children.setdefault(token, RemotePathNode())
            if recursive:
                nodes[path_processor.join(*path.parts[:i + 1])] = node
        if not recursive:
            nodes[str(path)] = node
        # Process hierarchy bottom-up to add parent locations
        relpath = data_location.relpath
        for node_path in reversed(nodes):
            node = nodes[node_path]
            if node_path == str(path):
                location = data_location
            else:
                location = DataLocation(
                    path=node_path,
                    relpath=relpath if relpath and node_path.endswith(relpath) else path_processor.basename(node_path),
                    deployment=data_location.deployment,
                    data_type=DataType.PRIMARY,
                    location=data_location.location,
                    available=True)
            if location in node.locations:
                break
            else:
                node.locations.add(location)
                relpath = path_processor.dirname(relpath)
        # Return location
        return data_location

    def remove_location(self, location: str):
        data_location = None
        for loc in self._filesystem.locations:
            if loc.location == location:
                data_location = loc
                break
        if data_location:
            self._remove_node(data_location, self._filesystem)
