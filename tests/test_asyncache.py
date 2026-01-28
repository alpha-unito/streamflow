import logging
from typing import Any, AsyncContextManager

import cachetools
import pytest
from cachetools import LRUCache
from pytest import LogCaptureFixture

from streamflow.core.asyncache import cached, cachedmethod
from tests.utils.utils import caplog_streamflow


class AsyncCached:
    def __init__(self, cache: Any) -> None:
        self.cache: Any = cache
        self.count: int = 0

    @cachedmethod(lambda self: self.cache)
    async def get(self, value: Any) -> int:
        self.count += 1
        return self.count


class AsyncCountedLock:
    def __init__(self):
        self.count: int = 0

    async def __aenter__(self):
        self.count += 1
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class AsyncLocked(AsyncContextManager):
    def __init__(self, cache):
        self.cache: Any = cache
        self.count: int = 0
        self.lock_count: int = 0

    @cachedmethod(lambda self: self.cache, lock=lambda self: self)
    async def get(self, value: Any) -> int:
        self.count += 1
        return self.count

    async def __aenter__(self):
        self.lock_count += 1
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class AsyncTarget:
    def __init__(self, cache: Any):
        self.cache: Any = cache
        self.call_count: int = 0

    @cachedmethod(lambda self: self.cache)
    async def get(self, *args, **kwargs) -> int:
        self.call_count += 1
        return self.call_count


class Counter:
    def __init__(self):
        self.count: int = 0

    async def async_func(self, *args, **kwargs) -> tuple[Any, ...]:
        self.count += 1
        return self.count, *args, *kwargs.values()


class IdentityObject:
    pass


class HashableObject:
    def __init__(self, item: int):
        self.item: int = item

    def __hash__(self):
        return hash(self.item)


class UnhashableObject:
    def __hash__(self):
        raise TypeError("unhashable type")


@pytest.mark.asyncio
class TestAsyncCachedMethod:
    async def test_async_dict_cache(self):
        """Test basic caching functionality with a standard dict."""
        cached_obj = AsyncCached({})

        assert await cached_obj.get(0) == 1
        assert await cached_obj.get(1) == 2
        assert await cached_obj.get(1) == 2  # Cache hit
        assert await cached_obj.get(1.0) == 2  # Hash hit (1 == 1.0)

        cached_obj.cache.clear()
        assert await cached_obj.get(1) == 3  # Cache miss

    @pytest.mark.asyncio
    async def test_async_lru_cache(self):
        """Test caching with LRU eviction policy."""
        cached_obj = AsyncCached(LRUCache(maxsize=2))

        assert await cached_obj.get(0) == 1
        assert await cached_obj.get(1) == 2
        assert await cached_obj.get(0) == 1  # Hit
        # Evict key 1
        assert await cached_obj.get(2) == 3
        assert await cached_obj.get(1) == 4  # Miss (evicted)

    @pytest.mark.asyncio
    async def test_async_no_cache(self):
        """Test behavior when the cache provider returns None."""
        cached_obj = AsyncCached(None)
        assert await cached_obj.get(0) == 1
        assert await cached_obj.get(0) == 2
        assert await cached_obj.get(0) == 3

    @pytest.mark.asyncio
    async def test_async_locked_cache(self):
        """Test that the async lock is properly acquired and released."""
        cached_obj = AsyncLocked({})
        # Lock for initial check (lock_count=1)
        # Cache empty so miss
        # Execute method
        # Lock to save the value in the cache (lock_count+=1)
        assert await cached_obj.get(0) == 1
        assert cached_obj.lock_count == 2

        # Lock for checking the cache (lock_count+=1)
        # Hit and return value
        assert await cached_obj.get(0) == 1
        assert cached_obj.lock_count == 3

    @pytest.mark.asyncio
    async def test_async_nospace_cache(self):
        """Test behavior when the cache has 0 capacity (ValueError handling)."""
        cached_obj = AsyncCached(LRUCache(maxsize=0))

        assert await cached_obj.get(0) == 1
        assert await cached_obj.get(0) == 2

    @pytest.mark.asyncio
    async def test_identity_hashing_warning_args(
        self, caplog: LogCaptureFixture
    ) -> None:
        """Test warning when an object with identity hashing is passed in args."""
        target = AsyncTarget({})
        with caplog_streamflow(caplog=caplog, level=logging.WARNING):
            await target.get(IdentityObject(), object())

        assert "argument type IdentityObject uses identity hashing" in caplog.text
        assert "argument type object uses identity hashing" in caplog.text
        assert target.call_count == 1

    @pytest.mark.asyncio
    async def test_identity_hashing_warning_kwargs(
        self, caplog: LogCaptureFixture
    ) -> None:
        """Test warning when an object with identity hashing is passed in kwargs."""
        target = AsyncTarget({})
        with caplog_streamflow(caplog=caplog, level=logging.WARNING):
            await target.get(item=IdentityObject(), item2=object())
            assert "argument type IdentityObject uses identity hashing" in caplog.text
            assert "argument type object uses identity hashing" in caplog.text

    @pytest.mark.asyncio
    async def test_hashable_types_no_warning(self, caplog: LogCaptureFixture) -> None:
        """Ensure hashable types do not trigger warnings."""
        target = AsyncTarget({})

        with caplog_streamflow(caplog=caplog, level=logging.WARNING):
            await target.get(
                1, "hello", HashableObject(1), key1="value", key2=HashableObject(2)
            )

            assert caplog.text == ""
            assert target.call_count == 1

    @pytest.mark.asyncio
    async def test_unhashable_object_raises_error(self) -> None:
        """Test that truly unhashable objects raise TypeError as expected."""
        target = AsyncTarget({})
        bad_obj = UnhashableObject()

        with pytest.raises(TypeError, match="unhashable type"):
            await target.get(bad_obj)

    async def test_mutable_builtin_raises_error(self):
        """Test that mutable builtins (like dict) raise TypeError."""
        target = AsyncTarget({})

        with pytest.raises(TypeError):
            await target.get({"data": 1})
            await target.get([])

    async def test_non_async_method(self):
        """Test decorator on a non-async method."""
        with pytest.raises(NotImplementedError):

            class AnonClass:
                @cachedmethod(None)
                def no_async_get(self, *args, **kwargs) -> int:
                    return -1


@pytest.mark.asyncio
class TestAsyncCached:

    async def test_decorator(self):
        cache = cachetools.LRUCache(maxsize=2)
        counter = Counter()
        wrapper = cached(cache)(counter.async_func)

        assert len(cache) == 0
        assert await wrapper(0) == (1, 0)
        assert len(cache) == 1
        assert cachetools.keys.hashkey(0) in cache

        assert await wrapper(1) == (2, 1)
        assert len(cache) == 2

        # Cache Hit
        assert await wrapper(1) == (2, 1)
        assert counter.count == 2

    async def test_decorator_typed(self):
        cache = cachetools.LRUCache(maxsize=3)
        counter = Counter()
        key = cachetools.keys.typedkey
        wrapper = cached(cache, key=key)(counter.async_func)

        # Typed keys distinguish between int and float
        assert await wrapper(1) == (1, 1)
        assert await wrapper(1.0) == (2, 1.0)
        assert len(cache) == 2
        assert counter.count == 2

    async def test_decorator_lock(self):
        cache = cachetools.LRUCache(maxsize=2)
        counter = Counter()
        lock = AsyncCountedLock()
        wrapper = cached(cache, lock=lock)(counter.async_func)

        assert len(cache) == 0
        assert await wrapper(0) == (1, 0)
        # Miss: 1 lock for initial check + 1 lock for save function return value = 2
        assert lock.count == 2

        assert await wrapper(0) == (1, 0)
        # Hit: 1 lock for checking the cache = 1 (Total 3)
        assert lock.count == 3
        assert counter.count == 1

    async def test_decorator_wrapped(self):
        cache = cachetools.LRUCache(maxsize=2)
        counter = Counter()
        wrapper = cached(cache)(counter.async_func)

        assert wrapper.__wrapped__ == counter.async_func
        assert len(cache) == 0
        # Call avoiding the decorator
        assert await wrapper.__wrapped__(0) == (1, 0)
        assert len(cache) == 0
        assert await wrapper(0) == (2, 0)  # First call with the cache
        assert len(cache) == 1
        assert await wrapper(0) == (2, 0)  # Cache hit
        assert len(cache) == 1

    async def test_zero_size_cache(self):
        cache = cachetools.LRUCache(maxsize=0)
        counter = Counter()
        wrapper = cached(cache)(counter.async_func)

        assert await wrapper(0) == (1, 0)
        assert len(cache) == 0
        assert await wrapper(0) == (2, 0)  # Always a miss
        assert counter.count == 2

    async def test_identity_hashing_warning_args(
        self, caplog: LogCaptureFixture
    ) -> None:
        """Test warning when an object with identity hashing is passed in args."""
        cache = cachetools.LRUCache(maxsize=10)
        counter = Counter()
        wrapper = cached(cache)(counter.async_func)

        with caplog_streamflow(caplog=caplog, level=logging.WARNING):
            await wrapper(IdentityObject(), key1=object())

        assert "argument type IdentityObject uses identity hashing" in caplog.text
        assert "argument type object" in caplog.text
        assert counter.count == 1

    async def test_identity_hashing_warning_kwargs(
        self, caplog: LogCaptureFixture
    ) -> None:
        """Test warning when an object with identity hashing is passed in kwargs."""
        cache = cachetools.LRUCache(maxsize=10)
        counter = Counter()
        wrapper = cached(cache)(counter.async_func)

        with caplog_streamflow(caplog=caplog, level=logging.WARNING):
            await wrapper(IdentityObject(), key1=object())

        assert "argument type IdentityObject uses identity hashing" in caplog.text
        assert "argument type object uses identity hashing" in caplog.text
        assert counter.count == 1

    async def test_hashable_types_no_warning(self, caplog: LogCaptureFixture) -> None:
        """Ensure hashable types do not trigger warnings."""
        cache = cachetools.LRUCache(maxsize=10)
        counter = Counter()
        wrapper = cached(cache)(counter.async_func)

        with caplog_streamflow(caplog=caplog, level=logging.WARNING):
            await wrapper(1, key1=HashableObject(1))

            assert caplog.text == ""
            assert counter.count == 1

    async def test_unhashable_object_raises_error(self):
        """Test that truly unhashable objects raise TypeError as expected."""
        cache = cachetools.LRUCache(maxsize=10)
        counter = Counter()
        wrapper = cached(cache)(counter.async_func)
        bad_obj = UnhashableObject()

        with pytest.raises(TypeError, match="unhashable type"):
            await wrapper(bad_obj)

    async def test_mutable_builtin_raises_error(self):
        """Test that mutable builtins (like dict) raise TypeError."""
        cache = cachetools.LRUCache(maxsize=10)
        counter = Counter()
        wrapper = cached(cache)(counter.async_func)

        with pytest.raises(TypeError):
            await wrapper({"data": 1})

        with pytest.raises(TypeError):
            await wrapper([])

    async def test_non_async_func(self):
        """Test decorator on a non-async function."""
        cache = cachetools.LRUCache(maxsize=10)

        with pytest.raises(NotImplementedError):
            cached(cache)(lambda x: x + 1)
