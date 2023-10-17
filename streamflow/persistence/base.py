from sys import getsizeof

from abc import ABC

from cachetools import Cache, LRUCache
from objsize import get_deep_size

from streamflow.core.context import StreamFlowContext
from streamflow.core.persistence import Database


class CachedDatabase(Database, ABC):
    def __init__(self, context: StreamFlowContext, cache_size: int = 1000**3):
        super().__init__(context)
        max_size = cache_size / 6
        self.deployment_cache: Cache = LRUCache(
            maxsize=max_size, getsizeof=get_deep_size
        )
        self.port_cache: Cache = LRUCache(maxsize=max_size, getsizeof=get_deep_size)
        self.step_cache: Cache = LRUCache(maxsize=max_size, getsizeof=get_deep_size)
        self.target_cache: Cache = LRUCache(maxsize=max_size, getsizeof=get_deep_size)
        self.token_cache: Cache = LRUCache(maxsize=max_size, getsizeof=get_deep_size)
        self.workflow_cache: Cache = LRUCache(maxsize=max_size, getsizeof=get_deep_size)
