from __future__ import annotations

from typing import TYPE_CHECKING

from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.scheduling import Hardware, Storage
from streamflow.data.remotepath import StreamFlowPath
from streamflow.deployment.utils import get_path_processor

if TYPE_CHECKING:
    from streamflow.core.deployment import Connector
    from streamflow.core.scheduling import AvailableLocation


async def bind_mount_point(
    context: StreamFlowContext,
    connector: Connector,
    location: AvailableLocation,
    hardware: Hardware,
) -> Hardware:
    """
    In the case of wrapped locations, the `Hardware` of the upper location must be
    mapped to the below `location`. In particular, if the `Hardware` has some storages
    that are bound, i.e., the `bind` attribute is not None, the storages must be
    resolved with the paths of the below `location`.

    :param context: the `StreamFlowContext` object with global application status.
    :param connector: the `Connector` object to communicate with the location
    :param location: the `AvailableLocation` object of the location information
    :param hardware: the `Hardware` object with eventual binds to resolve
    :return: a new `Hardware` object with the eventual bind in the storages resolved
    """
    path_processor = get_path_processor(connector)
    storage = {}
    for disk in hardware.storage.values():
        if disk.bind is not None:
            mount_point = await get_mount_point(context, location, disk.bind)
            if mount_point not in storage.keys():
                storage[mount_point] = Storage(
                    mount_point=mount_point,
                    size=disk.size,
                    paths={
                        path_processor.normpath(
                            path_processor.join(
                                disk.bind,
                                path_processor.relpath(p, disk.mount_point),
                            )
                        )
                        for p in disk.paths
                    },
                )
    return Hardware(cores=hardware.cores, memory=hardware.memory, storage=storage)


async def get_mount_point(
    context: StreamFlowContext,
    location: AvailableLocation,
    path: str,
) -> str:
    """
    Get the mount point of a path in the given `location`

    :param context: the `StreamFlowContext` object with global application status.
    :param location: the `AvailableLocation` object with the location information
    :param path: the path whose mount point should be returned
    :return: the mount point containing the given path
    """
    try:
        return location.hardware.get_mount_point(path)
    except KeyError:
        path_to_resolve = StreamFlowPath(
            path, context=context, location=location.location
        )
        while (mount_point := await path_to_resolve.resolve()) is None:
            path_to_resolve = path_to_resolve.parent
            if not path_to_resolve:
                raise WorkflowExecutionException(
                    f"Impossible to find the mount point of {path} path on location {location}"
                )
        location_mount_points = location.hardware.get_mount_points()
        while (
            mount_point.parent != mount_point
            and str(mount_point) not in location_mount_points
        ):
            mount_point = mount_point.parent
        location.hardware.get_storage(str(mount_point)).add_path(path)
        return str(mount_point)
