import logging

from importlib_metadata import entry_points

from streamflow.core.exception import InvalidPluginException
from streamflow.core.utils import get_class_fullname
from streamflow.ext.plugin import StreamFlowPlugin
from streamflow.log_handler import logger


def load_extensions():
    plugins = entry_points(group="unito.streamflow.plugin")
    for plugin in plugins:
        plugin = (plugin.load())()
        if isinstance(plugin, StreamFlowPlugin):
            plugin.register()
            if logger.isEnabledFor(logging.INFO):
                logger.info(
                    f"Successfully registered plugin {get_class_fullname(type(plugin))}"
                )
        else:
            raise InvalidPluginException(
                "StreamFlow plugins must extend the streamflow.ext.StreamFlowPlugin class"
            )
