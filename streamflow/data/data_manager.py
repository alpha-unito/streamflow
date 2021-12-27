from __future__ import annotations

import asyncio
import os
import posixpath
import shutil
import tempfile
from pathlib import Path, PosixPath
from typing import TYPE_CHECKING

from streamflow.core import utils
from streamflow.core.data import DataManager, DataLocation, LOCAL_LOCATION, DataType
from streamflow.core.deployment import Connector
from streamflow.data import remotepath
from streamflow.deployment.connector.base import ConnectorCopyKind
from streamflow.deployment.connector.local import LocalConnector
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.workflow import Job
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
                                      src_job: Optional[Job],
                                      src_location: Optional[str],
                                      src: str,
                                      dst_job: Optional[Job],
                                      dst_locations: MutableSequence[str],
                                      dst: str,
                                      writable: bool):
        src_connector = utils.get_connector(src_job, self.context)
        dst_connector = utils.get_connector(dst_job, self.context)
        # Register source path among data locations
        path_processor = os.path if isinstance(src_connector, LocalConnector) is None else posixpath
        src = path_processor.abspath(src)
        # Create destination folder
        await remotepath.mkdir(dst_connector, dst_locations, str(Path(dst).parent))
        # Follow symlink for source path
        src = await remotepath.follow_symlink(src_connector, src_location, src)
        primary_locations = self.path_mapper.get(src_location, src, DataType.PRIMARY)
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
                        self.path_mapper.create_mapping(
                            location_type=DataType.SYMBOLIC_LINK,
                            src_path=src,
                            src_location=src_location,
                            dst_path=dst,
                            dst_job=dst_job,
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
                        data_locations.append(self.path_mapper.create_mapping(
                            location_type=DataType.WRITABLE_COPY,
                            src_path=src,
                            src_location=src_location,
                            dst_path=dst,
                            dst_job=dst_job,
                            dst_location=dst_location))
                    found_existing_loc = True
                    break
            # Otherwise, perform a remote copy and mark the destination as primary
            if not found_existing_loc:
                remote_locations.append(dst_location)
                data_locations.append(self.path_mapper.create_mapping(
                    location_type=DataType.WRITABLE_COPY if writable else DataType.PRIMARY,
                    src_path=src,
                    src_location=src_location,
                    dst_path=dst,
                    dst_job=dst_job,
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
                           location: str,
                           path: str,
                           location_type: Optional[DataType] = None) -> Set[DataLocation]:
        data_locations = self.path_mapper.get(location, path, location_type)
        return {loc for loc in data_locations if loc.data_type != DataType.INVALID}

    def invalidate_location(self, location: str, path: str) -> None:
        self.path_mapper.invalidate_location(location, path)

    def register_path(self,
                      job: Optional[Job],
                      location: Optional[str],
                      path: str):
        self.path_mapper.put(
            location=location,
            path=path,
            data_locations={DataLocation(
                path=path,
                job=job.name if job is not None else None,
                location=location or LOCAL_LOCATION,
                data_type=DataType.PRIMARY,
                available=True)})
        self.context.checkpoint_manager.register_path(job, path)

    async def transfer_data(self,
                            src: str,
                            src_job: Optional[Job],
                            dst: str,
                            dst_job: Optional[Job],
                            writable: bool = False):
        # Get connectors and locations from steps
        src_connector = utils.get_connector(src_job, self.context)
        src_locations = utils.get_locations(src_job)
        dst_connector = utils.get_connector(dst_job, self.context)
        dst_locations = utils.get_locations(dst_job)
        src_found = False
        # If source file is local, simply transfer it
        if not src_locations:
            if await remotepath.exists(src_connector, None, src):
                src_found = True
                await self._transfer_from_location(
                    src_job, None, src,
                    dst_job, dst_locations, dst,
                    writable)
        # Otherwise process each source location that actually contains the source path
        else:
            for src_location in src_locations:
                if await remotepath.exists(src_connector, src_location, src):
                    src_found = True
                    await self._transfer_from_location(
                        src_job, src_location, src,
                        dst_job, dst_locations, dst,
                        writable)
        # If source path does not exist
        if not src_found:
            # Search it on destination locations
            for dst_location in dst_locations:
                # If it exists, switch source connector and locations with the new ones
                if await remotepath.exists(dst_connector, dst_location, src):
                    logger.debug("Path {path} found {location}.".format(
                        path=src,
                        location="on location {location}".format(
                            location=dst_location) if dst_location is not None else "on local file-system"
                    ))
                    await self._transfer_from_location(
                        dst_job, dst_location, src,
                        dst_job, dst_locations, dst,
                        writable)
                    break


class RemotePathNode(object):
    __slots__ = ('children', 'locations')

    def __init__(self):
        self.children: MutableMapping[str, RemotePathNode] = {}
        self.locations: Set[DataLocation] = set()


class RemotePathMapper(object):

    def __init__(self):
        self._filesystems: MutableMapping[str, RemotePathNode] = {LOCAL_LOCATION: RemotePathNode()}

    def _process_location(self, location: str) -> str:
        if location is None:
            location = LOCAL_LOCATION
        if location not in self._filesystems:
            self._filesystems[location] = RemotePathNode()
        return location

    def create_mapping(self,
                       location_type: DataType,
                       src_path: str,
                       src_location: Optional[str],
                       dst_path: str,
                       dst_job: Optional[Job],
                       dst_location: Optional[str],
                       available: bool = False) -> DataLocation:
        src_location = self._process_location(src_location)
        data_locations = self.get(src_location, src_path)
        dst_location = self._process_location(dst_location)
        dst_data_location = DataLocation(
            path=dst_path,
            job=dst_job.name if dst_job is not None else None,
            data_type=location_type,
            location=dst_location,
            available=available)
        if dst_data_location not in data_locations:
            data_locations.add(dst_data_location)
        self.put(dst_location, dst_path, data_locations)
        return dst_data_location

    def _remove_node(self, location: str, node: RemotePathNode):
        node.locations = set(filter(lambda l: l.location != location, node.locations))
        for location in node.locations:
            self.put(location.location, location.path, node.locations)
        for n in node.children.values():
            self._remove_node(location, n)
        return node

    def get(self, location: str, path: str, location_type: Optional[DataType] = None) -> Set[DataLocation]:
        location = location or LOCAL_LOCATION
        node = self._filesystems.get(location)
        if not node:
            return set()
        path = Path(path) if location == LOCAL_LOCATION else PosixPath(path)
        for token in path.parts:
            if token in node.children:
                node = node.children[token]
            else:
                return set()
        return ({loc for loc in node.locations if loc.data_type == location_type} if location_type
                else node.locations)

    def invalidate_location(self, location: Optional[str], path: str) -> None:
        location = location or LOCAL_LOCATION
        locations = self.get(location, path)
        for location in locations:
            if location.location == location:
                location.data_type = DataType.INVALID
        self.put(location, path, locations)

    def put(self, location: str, path: str, data_locations: Set[DataLocation]) -> None:
        location = self._process_location(location)
        node = self._filesystems[location]
        path = Path(path) if location == LOCAL_LOCATION else PosixPath(path)
        for token in path.parts:
            if token not in node.children:
                node.children[token] = RemotePathNode()
            node = node.children[token]
        node.locations = data_locations

    def remove_location(self, location: str):
        if location in self._filesystems:
            node = self._filesystems[location]
            self._remove_node(location, node)
            del self._filesystems[location]
