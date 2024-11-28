from __future__ import annotations

import os
from typing import TYPE_CHECKING

from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.scheduling import Hardware, Storage
from streamflow.data.remotepath import follow_symlink
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
    return Hardware(
        cores=hardware.cores,
        memory=hardware.memory,
        storage={
            key: Storage(
                mount_point=await get_mount_point(
                    context, connector, location, disk.bind
                ),
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
            for key, disk in hardware.storage.items()
            if disk.bind is not None
        },
    )


async def get_mount_point(
    context: StreamFlowContext,
    connector: Connector,
    location: AvailableLocation,
    path: str,
) -> str:
    """
    Get the mount point of a path in the given `location`

    :param context: the `StreamFlowContext` object with global application status.
    :param connector: the `Connector` object to communicate with the location
    :param location: the `AvailableLocation` object with the location information
    :param path: the path whose mount point should be returned
    :return: the mount point containing the given path
    """
    try:
        return location.hardware.get_mount_point(path)
    except KeyError:
        path_processor = get_path_processor(connector)
        path_to_resolve = path
        while (
            mount_point := await follow_symlink(
                context, connector, location.location, path_to_resolve
            )
        ) is None:
            path_to_resolve = path_processor.dirname(path_to_resolve)
            if not path_to_resolve:
                raise WorkflowExecutionException(
                    f"Impossible to find the mount point of {path} path on location {location}"
                )
        location_mount_points = location.hardware.get_mount_points()
        while mount_point != os.sep and mount_point not in location_mount_points:
            mount_point = path_processor.dirname(mount_point)
        location.hardware.get_storage(mount_point).add_path(path)
        return mount_point
