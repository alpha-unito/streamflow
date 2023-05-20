import sys
from abc import ABC

from cachetools import Cache, LRUCache

from streamflow.core.context import StreamFlowContext
from streamflow.core.persistence import Database


class CachedDatabase(Database, ABC):
    def __init__(self, context: StreamFlowContext):
        super().__init__(context)
        self.deployment_cache: Cache = LRUCache(maxsize=sys.maxsize)
        self.port_cache: Cache = LRUCache(maxsize=sys.maxsize)
        self.step_cache: Cache = LRUCache(maxsize=sys.maxsize)
        self.target_cache: Cache = LRUCache(maxsize=sys.maxsize)
        self.token_cache: Cache = LRUCache(maxsize=sys.maxsize)
        self.workflow_cache: Cache = LRUCache(maxsize=sys.maxsize)
