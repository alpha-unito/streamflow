
import psutil
import logging
from abc import ABC
from objsize import get_deep_size
from cachetools import Cache, LRUCache

from streamflow.log_handler import logger
from streamflow.core.persistence import Database
from streamflow.core.context import StreamFlowContext
def wrapper_get_size(x):
    if (
        logger.isEnabledFor(logging.WARN)
        and psutil.virtual_memory().available < 5 * 10**8
    ):  # 500 mb free
        logger.warn(f"Memory almost finished {psutil.virtual_memory()}.")
        # raise FailureHandlingException("Memory ends")
    return get_deep_size(x)


class CachedDatabase(Database, ABC):
    def __init__(self, context: StreamFlowContext, cache_size: int = 1000**3):
        super().__init__(context)
        max_size = cache_size / 6
        self.deployment_cache: Cache = LRUCache(
            maxsize=max_size, getsizeof=wrapper_get_size
        )
        self.port_cache: Cache = LRUCache(maxsize=max_size, getsizeof=wrapper_get_size)
        self.step_cache: Cache = LRUCache(maxsize=max_size, getsizeof=wrapper_get_size)
        self.target_cache: Cache = LRUCache(
            maxsize=max_size, getsizeof=wrapper_get_size
        )
        self.token_cache: Cache = LRUCache(maxsize=max_size, getsizeof=wrapper_get_size)
        self.workflow_cache: Cache = LRUCache(
            maxsize=max_size, getsizeof=wrapper_get_size
        )
