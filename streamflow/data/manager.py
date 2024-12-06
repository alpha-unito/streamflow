from __future__ import annotations

import asyncio
import os
import posixpath
from collections.abc import MutableMapping, MutableSequence
from importlib.resources import files
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

from streamflow.core.data import DataLocation, DataManager, DataType
from streamflow.core.exception import WorkflowExecutionException
from streamflow.data import remotepath
from streamflow.deployment.connector.local import LocalConnector
from streamflow.deployment.utils import get_path_processor
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.deployment import Connector, ExecutionLocation


async def _copy(
    src_connector: Connector | None,
    src_location: ExecutionLocation | None,
    src: str,
    dst_connector: Connector | None,
    dst_locations: MutableSequence[ExecutionLocation] | None,
    dst: str,
    writable: False,
) -> None:
    if isinstance(src_connector, LocalConnector):
        await dst_connector.copy_local_to_remote(
            src=src,
            dst=dst,
            locations=dst_locations,
            read_only=not writable,
        )
    elif isinstance(dst_connector, LocalConnector):
        await src_connector.copy_remote_to_local(
            src=src,
            dst=dst,
            location=src_location,
            read_only=not writable,
        )
    else:
        await dst_connector.copy_remote_to_remote(
            src=src,
            dst=dst,
            locations=dst_locations,
            source_location=src_location,
            source_connector=src_connector,
            read_only=not writable,
        )


def _get_inner_path(location: ExecutionLocation, path: str) -> str | None:
    if location.wraps:
        for mount in sorted(location.mounts.keys(), reverse=True):
            if path.startswith(mount):
                return os.path.normpath(
                    (posixpath if location.wraps.local else os.path).join(
                        location.mounts[mount],
                        (posixpath if location.local else os.path).relpath(path, mount),
                    )
                )
    return None


class _RemotePathNode:
    __slots__ = ("children", "locations")

    def __init__(self):
        self.children: MutableMapping[str, _RemotePathNode] = {}
        self.locations: MutableMapping[
            str, MutableMapping[str, MutableSequence[DataLocation]]
        ] = {}

    def __repr__(self):
        return " - ".join(
            [
                ",".join(
                    [
                        ",".join(
                            [
                                f"{loc.path} ({str(loc.data_type)}) on location {loc.location}"
                                for loc in name_locs
                            ]
                        )
                        for name_locs in dep_locs.values()
                    ]
                )
                for dep_locs in self.locations.values()
            ]
        )


class _RemotePathMapper:
    def __init__(self, context: StreamFlowContext):
        self._filesystem: _RemotePathNode = _RemotePathNode()
        self.context: StreamFlowContext = context

    def __repr__(self):
        return self._node_repr(next(iter(self._filesystem.children.values())), 0)

    def _node_repr(self, node: _RemotePathNode, level: int) -> str:
        tree = level * "\t" + "|-- " + repr(node) + "\n"
        for child in node.children.values():
            tree += self._node_repr(child, level + 1)
        return tree

    def _remove_node(self, location: DataLocation, node: _RemotePathNode):
        if location.deployment in node.locations:
            del node.locations[location.deployment][location.name]
        for n in node.children.values():
            self._remove_node(location, n)

    def get(
        self,
        path: str,
        data_type: DataType | None = None,
        deployment: str | None = None,
        name: str | None = None,
    ) -> MutableSequence[DataLocation]:
        path = PurePosixPath(Path(path).as_posix())
        node = self._filesystem
        for token in path.parts:
            if token in node.children:
                node = node.children[token]
            else:
                return []
        result = []
        for dep in [deployment] if deployment is not None else node.locations:
            for n in [name] if name is not None else node.locations.setdefault(dep, {}):
                locations = node.locations.setdefault(dep, {}).setdefault(n, [])
                result.extend(
                    [
                        loc
                        for loc in locations
                        if not (data_type and loc.data_type != data_type)
                    ]
                )
        return result

    def invalidate_location(self, location: ExecutionLocation, path: str) -> None:
        path = PurePosixPath(Path(path).as_posix())
        node = self._filesystem
        for token in path.parts:
            node = node.children[token]
        for node_child in node.children.values():
            for data_loc in node_child.locations.setdefault(
                location.deployment, {}
            ).get(location.name, []):
                if data_loc.data_type != DataType.INVALID:
                    self.invalidate_location(data_loc, data_loc.path)
                    data_loc.data_type = DataType.INVALID

    def put(
        self, path: str, data_location: DataLocation, recursive: bool = False
    ) -> DataLocation:
        path = PurePosixPath(Path(path).as_posix())
        path_processor = get_path_processor(
            self.context.deployment_manager.get_connector(data_location.deployment)
        )
        node = self._filesystem
        nodes = {}
        # Create or navigate hierarchy
        for i, token in enumerate(path.parts):
            node = node.children.setdefault(token, _RemotePathNode())
            if recursive:
                nodes[path_processor.join(*path.parts[: i + 1])] = node
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
                    location=data_location.location,
                    path=node_path,
                    relpath=(
                        relpath
                        if relpath and node_path.endswith(relpath)
                        else path_processor.basename(node_path)
                    ),
                    data_type=DataType.PRIMARY,
                    available=True,
                )
            node_location = node.locations.setdefault(
                location.deployment, {}
            ).setdefault(location.name, [])
            paths = [
                loc.path for loc in node_location if loc.data_type != DataType.INVALID
            ]
            if location.path in paths:
                break
            else:
                node.locations[location.deployment][location.name].append(location)
                relpath = path_processor.dirname(relpath)
        # Return location
        return data_location

    def remove_location(self, location: DataLocation):
        data_locations = self._filesystem.locations.setdefault(
            location.deployment, {}
        ).get(location.name)
        for data_location in data_locations:
            self._remove_node(data_location, self._filesystem)


class DefaultDataManager(DataManager):
    def __init__(self, context: StreamFlowContext):
        super().__init__(context)
        self.path_mapper = _RemotePathMapper(context)

    async def close(self):
        pass

    def get_data_locations(
        self,
        path: str,
        deployment: str | None = None,
        location_name: str | None = None,
        data_type: DataType | None = None,
    ) -> MutableSequence[DataLocation]:
        data_locations = self.path_mapper.get(
            path=path, data_type=data_type, deployment=deployment, name=location_name
        )
        data_locations = [
            loc for loc in data_locations if loc.data_type != DataType.INVALID
        ]
        return data_locations

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("data_manager.json")
            .read_text("utf-8")
        )

    def get_source_location(
        self, path: str, dst_deployment: str
    ) -> DataLocation | None:
        if data_locations := self.get_data_locations(
            path=path, data_type=DataType.PRIMARY
        ):
            if same_connector_locations := {
                loc for loc in data_locations if loc.deployment == dst_deployment
            }:
                return next(iter(same_connector_locations))
            elif local_locations := {
                loc
                for loc in data_locations
                if isinstance(
                    self.context.deployment_manager.get_connector(loc.deployment),
                    LocalConnector,
                )
            }:
                return next(iter(local_locations))
            else:
                return next(iter(data_locations))
        else:
            return None

    def invalidate_location(self, location: ExecutionLocation, path: str) -> None:
        self.path_mapper.invalidate_location(location, path)

    def register_path(
        self,
        location: ExecutionLocation,
        path: str,
        relpath: str | None = None,
        data_type: DataType = DataType.PRIMARY,
    ) -> DataLocation:
        data_location = DataLocation(
            location=location,
            path=path,
            relpath=relpath or path,
            data_type=data_type,
            available=True,
        )
        self.path_mapper.put(path=path, data_location=data_location, recursive=True)
        self.context.checkpoint_manager.register(data_location)
        # Process wrapped locations if any
        while (path := _get_inner_path(location=location, path=path)) is not None:
            inner_location = DataLocation(
                location=location.wraps,
                path=path,
                relpath=relpath or path,
                data_type=data_type,
                available=True,
            )
            self.path_mapper.put(
                path=path, data_location=inner_location, recursive=True
            )
            self.register_relation(
                src_location=data_location, dst_location=inner_location
            )
            location = location.wraps
        return data_location

    def register_relation(
        self, src_location: DataLocation, dst_location: DataLocation
    ) -> None:
        for data_location in self.path_mapper.get(path=src_location.path):
            self.path_mapper.put(data_location.path, dst_location)
            self.path_mapper.put(dst_location.path, data_location)

    async def transfer_data(
        self,
        src_location: ExecutionLocation,
        src_path: str,
        dst_locations: MutableSequence[ExecutionLocation],
        dst_path: str,
        writable: bool = False,
    ) -> None:
        src_connector = self.context.deployment_manager.get_connector(
            src_location.deployment
        )
        dst_connector = self.context.deployment_manager.get_connector(
            dst_locations[0].deployment
        )
        # Create destination folder
        await asyncio.gather(
            *(
                asyncio.create_task(
                    remotepath.mkdir(
                        dst_connector, location, str(Path(dst_path).parent)
                    )
                )
                for location in dst_locations
            )
        )
        # Follow symlink for source path
        await asyncio.gather(
            *(
                asyncio.create_task(src_data_loc.available.wait())
                for src_data_loc in self.get_data_locations(
                    path=src_path,
                    deployment=src_connector.deployment_name,
                    location_name=src_location.name,
                    data_type=DataType.PRIMARY,
                )
            )
        )
        if (
            src_realpath := await remotepath.follow_symlink(
                self.context, src_connector, src_location, src_path
            )
        ) is None:
            logger.info(f"Remote file system: {repr(self.path_mapper)}")
            raise WorkflowExecutionException(
                f"Error retrieving realpath for {src_path} on location {src_location} "
                f"while transferring it to {dst_path} on deployment {dst_connector.deployment_name}"
            )
        else:
            src_path = src_realpath
        primary_locations = self.path_mapper.get(
            path=src_path, data_type=DataType.PRIMARY
        )
        copy_tasks = []
        remote_locations = []
        data_locations = []
        for dst_location in dst_locations:
            # Check if a primary copy of the source path is already present on the destination location
            for primary_loc in primary_locations:
                if (
                    primary_loc.deployment == dst_location.deployment
                    and primary_loc.name == dst_location.name
                ):
                    # Wait for the source location to be available on the destination path
                    await primary_loc.available.wait()
                    # If yes, perform a symbolic link if possible
                    copy_tasks.append(
                        asyncio.create_task(
                            _copy(
                                src_connector=dst_connector,
                                src_location=dst_location,
                                src=primary_loc.path,
                                dst_connector=dst_connector,
                                dst_locations=[dst_location],
                                dst=dst_path,
                                writable=writable,
                            )
                        )
                    )
                    break
            # Otherwise, perform a remote copy and mark the destination as primary
            else:
                remote_locations.append(dst_location)
            # If the source path has already been registered
            if src_data_locations := self.path_mapper.get(path=src_path):
                src_data_location = next(iter(src_data_locations))
                # Compute actual destination path
                loc_dst_path = (
                    get_path_processor(dst_connector).join(
                        dst_path,
                        get_path_processor(src_connector).basename(src_path),
                    )
                    if await remotepath.isdir(
                        connector=dst_connector,
                        location=dst_location,
                        path=dst_path,
                    )
                    else dst_path
                )
                # Register path and data location for parent folder
                self.register_path(dst_location, str(Path(loc_dst_path).parent))
                # Register the new `DataLocation` object
                dst_data_location = DataLocation(
                    location=dst_location,
                    path=loc_dst_path,
                    relpath=src_data_location.relpath,
                    data_type=DataType.PRIMARY,
                )
                self.path_mapper.put(path=loc_dst_path, data_location=dst_data_location)
                data_locations.append(dst_data_location)
                # If the destination is not writable , map the new `DataLocation` object to the source locations
                if not writable:
                    self.register_relation(src_data_location, dst_data_location)
            # Otherwise, raise an exception
            else:
                raise WorkflowExecutionException(
                    f"No data locations found for path {src_path} "
                    f"while trying to map {dst_path} on {dst_location}"
                )
        # Perform all the copy operations
        if remote_locations:
            copy_tasks.append(
                asyncio.create_task(
                    _copy(
                        src_connector=src_connector,
                        src_location=src_location,
                        src=src_path,
                        dst_connector=dst_connector,
                        dst_locations=remote_locations,
                        dst=dst_path,
                        writable=writable,
                    )
                )
            )
        await asyncio.gather(*copy_tasks)
        # Mark all destination data locations as available
        for data_location in data_locations:
            if not writable:
                connector = self.context.deployment_manager.get_connector(
                    data_location.deployment
                )
                data_location.data_type = (
                    DataType.SYMBOLIC_LINK
                    if await remotepath.islink(
                        connector=connector,
                        location=data_location.location,
                        path=data_location.path,
                    )
                    else DataType.PRIMARY
                )
            # Process wrapped locations if any
            inner_path = data_location.path
            inner_location = data_location.location
            while (
                inner_path := _get_inner_path(inner_location, inner_path)
            ) is not None:
                inner_location = inner_location.wraps
                if inner_data_locs := self.path_mapper.get(
                    path=inner_path,
                    deployment=inner_location.deployment,
                    name=inner_location.name,
                ):
                    inner_data_location = inner_data_locs[0]
                else:
                    inner_data_location = DataLocation(
                        location=inner_location,
                        path=inner_path,
                        relpath=data_location.relpath,
                        data_type=data_location.data_type,
                        available=True,
                    )
                self.register_relation(data_location, inner_data_location)
            data_location.available.set()
