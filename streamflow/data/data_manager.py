from __future__ import annotations

import asyncio
import os
import posixpath
import shutil
import tempfile
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

from streamflow.core.data import DataManager, DataLocation, LOCAL_LOCATION, DataType
from streamflow.core.deployment import Connector
from streamflow.data import remotepath
from streamflow.deployment.connector.base import ConnectorCopyKind
from streamflow.deployment.connector.local import LocalConnector
from streamflow.log_handler import logger

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
    if src_connector == dst_connector:
        await dst_connector.copy(
            src=src,
            dst=dst,
            locations=dst_locations,
            kind=ConnectorCopyKind.REMOTE_TO_REMOTE,
            source_location=src_location,
            read_only=not writable)
    elif isinstance(src_connector, LocalConnector):
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
        temp_dir = tempfile.mkdtemp()
        await src_connector.copy(
            src=src,
            dst=temp_dir,
            locations=[src_location],
            kind=ConnectorCopyKind.REMOTE_TO_LOCAL,
            read_only=not writable)
        await asyncio.gather(*(asyncio.create_task(dst_connector.copy(
            src=os.path.join(temp_dir, element),
            dst=dst,
            locations=dst_locations,
            kind=ConnectorCopyKind.LOCAL_TO_REMOTE,
            read_only=not writable
        )) for element in os.listdir(temp_dir)))
        shutil.rmtree(temp_dir)


class DefaultDataManager(DataManager):

    def __init__(self, context: StreamFlowContext):
        super().__init__(context)
        self.path_mapper = RemotePathMapper()

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
                if primary_loc.location == (dst_location or LOCAL_LOCATION):
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

    def get_data_locations(self,
                           path: str,
                           deployment: Optional[str] = None,
                           location_type: Optional[DataType] = None) -> Set[DataLocation]:
        data_locations = self.path_mapper.get(path, location_type)
        data_locations = {loc for loc in data_locations if loc.data_type != DataType.INVALID}
        if deployment is not None:
            data_locations = {loc for loc in data_locations if loc.deployment == deployment}
        return data_locations

    def get_source_location(self,
                            path: str,
                            dst_deployment: str) -> Optional[DataLocation]:
        if data_locations := self.get_data_locations(path):
            dst_connector = self.context.deployment_manager.get_connector(dst_deployment)
            same_connector_locations = {loc for loc in data_locations if
                                        loc.deployment == dst_connector.deployment_name}
            if same_connector_locations:
                for loc in same_connector_locations:
                    if loc.data_type == DataType.PRIMARY:
                        return loc
                return list(same_connector_locations)[0]
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
        self.path_mapper.put(path, data_location)
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
        src_found = False
        for src_location in src_locations:
            if await remotepath.exists(src_connector, src_location, src_path):
                src_found = True
                await self._transfer_from_location(
                    src_connector, src_location, src_path,
                    dst_connector, dst_locations, dst_path,
                    writable)
        # If source path does not exist
        if not src_found:
            # Search it on destination locations
            for dst_location in dst_locations:
                # If it exists, switch source connector and locations with the new ones
                if await remotepath.exists(dst_connector, dst_location, src_path):
                    logger.debug("Path {path} found {location}.".format(
                        path=src_path,
                        location="on location {location}".format(
                            location=dst_location) if dst_location is not None else "on local file-system"
                    ))
                    await self._transfer_from_location(
                        src_connector, dst_location, src_path,
                        dst_connector, dst_locations, dst_path,
                        writable)
                    break


class RemotePathNode(object):
    __slots__ = ('children', 'locations')

    def __init__(self):
        self.children: MutableMapping[str, RemotePathNode] = {}
        self.locations: Set[DataLocation] = set()


class RemotePathMapper(object):

    def __init__(self):
        self._filesystem: RemotePathNode = RemotePathNode()

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

    def put(self, path: str, data_location: DataLocation) -> DataLocation:
        path = PurePosixPath(Path(path).as_posix())
        node = self._filesystem
        for token in path.parts:
            if token not in node.children:
                node.children[token] = RemotePathNode()
            node.locations.add(data_location)
            node = node.children[token]
        node.locations.add(data_location)
        return data_location

    def remove_location(self, location: str):
        data_location = None
        for loc in self._filesystem.locations:
            if loc.location == location:
                data_location = loc
                break
        if data_location:
            self._remove_node(data_location, self._filesystem)
