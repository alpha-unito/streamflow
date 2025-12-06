from __future__ import annotations

"""
Helpers to use [cachetools](https://github.com/tkem/cachetools) with
asyncio.
"""

import functools
import inspect

__all__ = ["cached", "cachedmethod"]

from contextlib import AbstractAsyncContextManager, suppress
from typing import Any

import cachetools


# noinspection PyUnresolvedReferences
def cached(
    cache: cachetools.Cache,
    key=cachetools.keys.hashkey,
    lock: AbstractAsyncContextManager[Any, bool | None] | None = None,
):
    """
    Decorator to wrap a function or a coroutine with a memoizing callable
    that saves results in a cache.
    When ``lock`` is provided for a standard function, it's expected to
    implement ``__enter__`` and ``__exit__`` that will be used to lock
    the cache when gets updated. If it wraps a coroutine, ``lock``
    must implement ``__aenter__`` and ``__aexit__``.
    """

    def decorator(func):
        if inspect.iscoroutinefunction(func):
            if cache is None:

                async def wrapper(*args, **kwargs):
                    return await func(*args, **kwargs)

            elif lock is None:

                async def wrapper(*args, **kwargs):
                    k = key(*args, **kwargs)
                    with suppress(KeyError):
                        return cache[k]
                    v = await func(*args, **kwargs)
                    with suppress(ValueError):
                        cache[k] = v
                    return v

            else:

                async def wrapper(*args, **kwargs):
                    k = key(*args, **kwargs)
                    with suppress(KeyError):
                        async with lock:
                            return cache[k]
                    v = await func(*args, **kwargs)
                    # in case of a race, prefer the item already in the cache
                    try:
                        async with lock:
                            return cache.setdefault(k, v)
                    except ValueError:
                        return v  # value too large

            return functools.update_wrapper(wrapper, func)
        else:
            raise NotImplementedError("Use cachetools.cached for non-async functions")

    return decorator


# noinspection PyUnresolvedReferences
def cachedmethod(cache, key=cachetools.keys.hashkey, lock=None):
    """Decorator to wrap a class or instance method with a memoizing
    callable that saves results in a cache.
    When ``lock`` is provided for a standard function, it's expected to
    implement ``__enter__`` and ``__exit__`` that will be used to lock
    the cache when gets updated. If it wraps a coroutine, ``lock``
    must implement ``__aenter__`` and ``__aexit__``.
    """

    def decorator(method):
        if inspect.iscoroutinefunction(method):
            if lock is None:

                async def wrapper(self, *args, **kwargs):
                    c = cache(self)
                    if c is None:
                        return await method(self, *args, **kwargs)
                    k = key(*args, **kwargs)
                    with suppress(KeyError):
                        return c[k]
                    v = await method(self, *args, **kwargs)
                    with suppress(ValueError):
                        c[k] = v
                    return v

            else:

                async def wrapper(self, *args, **kwargs):
                    c = cache(self)
                    if c is None:
                        return await method(self, *args, **kwargs)
                    k = key(*args, **kwargs)
                    with suppress(KeyError):
                        async with lock(self):
                            return c[k]
                    v = await method(self, *args, **kwargs)
                    # in case of a race, prefer the item already in the cache
                    try:
                        async with lock(self):
                            return c.setdefault(k, v)
                    except ValueError:
                        return v  # value too large

            return functools.update_wrapper(wrapper, method)
        else:
            raise NotImplementedError(
                "Use cachetools.cachedmethod for non-async methods"
            )

    return decorator
