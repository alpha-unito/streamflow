import os
import posixpath
from types import ModuleType

from streamflow.core.deployment import Connector
from streamflow.deployment.connector import LocalConnector


def get_path_processor(connector: Connector) -> ModuleType:
    return (
        posixpath
        if connector is not None and not isinstance(connector, LocalConnector)
        else os.path
    )
