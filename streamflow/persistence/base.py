import sys
from abc import ABC
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any

from cachebox import LRUCache

from streamflow.core.context import StreamFlowContext
from streamflow.core.persistence import Database

if TYPE_CHECKING:
    from cachebox import BaseCacheImpl


class CachedDatabase(Database, ABC):
    def __init__(self, context: StreamFlowContext):
        super().__init__(context)
        self.deployment_cache: BaseCacheImpl[int, MutableMapping[str, Any]] = LRUCache(
            maxsize=sys.maxsize
        )
        self.port_cache: BaseCacheImpl[int, MutableMapping[str, Any]] = LRUCache(
            maxsize=sys.maxsize
        )
        self.step_cache: BaseCacheImpl[int, MutableMapping[str, Any]] = LRUCache(
            maxsize=sys.maxsize
        )
        self.target_cache: BaseCacheImpl[int, MutableMapping[str, Any]] = LRUCache(
            maxsize=sys.maxsize
        )
        self.filter_cache: BaseCacheImpl[int, MutableMapping[str, Any]] = LRUCache(
            maxsize=sys.maxsize
        )
        self.token_cache: BaseCacheImpl[int, MutableMapping[str, Any]] = LRUCache(
            maxsize=sys.maxsize
        )
        self.workflow_cache: BaseCacheImpl[int, MutableMapping[str, Any]] = LRUCache(
            maxsize=sys.maxsize
        )
