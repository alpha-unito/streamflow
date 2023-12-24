import psutil
import logging
from abc import ABC
from cachetools import Cache, LRUCache

from streamflow.core.utils import get_size_obj
from streamflow.log_handler import logger
from streamflow.core.persistence import Database
from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import FailureHandlingException


def wrapper_get_size(x):
    if psutil.virtual_memory().available < 2 * 10**8:
        # if logger.isEnabledFor(logger.isEnabledFor(logging.WARN)):
        logger.info(f"Memory almost finished {psutil.virtual_memory()}.")
        raise FailureHandlingException("Memory ends")
    return get_size_obj(x)


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
