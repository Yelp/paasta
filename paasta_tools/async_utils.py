import asyncio
import functools
import time
import weakref
from collections import defaultdict
from typing import Any
from typing import AsyncIterable
from typing import Awaitable
from typing import Callable
from typing import DefaultDict
from typing import Dict
from typing import List
from typing import TypeVar


T = TypeVar('T')


def async_ttl_cache(
    ttl: float = 300,
    cleanup_self: bool = False,
) -> Callable[
    [Callable[..., Awaitable[T]]],  # wrapped
    Callable[..., Awaitable[T]],  # inner
]:
    async def call_or_get_from_cache(cache, coro, args, kwargs):
        key = functools._make_key(args, kwargs, typed=False)
        try:
            future, last_update = cache[key]
            if ttl > 0 and time.time() - last_update > ttl:
                raise KeyError
        except KeyError:
            future = asyncio.ensure_future(coro)
            # set the timestamp to +infinity so that we always wait on the in-flight request.
            cache[key] = (future, float('Inf'))
        value = await future
        cache[key] = (future, time.time())
        return value

    if cleanup_self:
        cache: DefaultDict[Any, Dict] = defaultdict(dict)

        def on_delete(w):
            del cache[w]

        def outer(wrapped):
            @functools.wraps(wrapped)
            async def inner(self, *args, **kwargs):
                w = weakref.ref(self, on_delete)
                self_cache = cache[w]
                return await call_or_get_from_cache(
                    self_cache,
                    wrapped(self, *args, **kwargs),
                    args,
                    kwargs,
                )
            return inner
    else:
        cache2: Dict = {}  # Should be Dict[Any, T] but that doesn't work.

        def outer(wrapped):
            @functools.wraps(wrapped)
            async def inner(*args, **kwargs):
                return await call_or_get_from_cache(
                    cache2,
                    wrapped(*args, **kwargs),
                    args,
                    kwargs,
                )
            return inner
    return outer


async def aiter_to_list(
    aiter: AsyncIterable[T],
) -> List[T]:
    return [x async for x in aiter]


def async_timeout(
    seconds: int = 10,
) -> Callable[
    [Callable[..., Awaitable[T]]],  # wrapped
    Callable[..., Awaitable[T]],  # inner
]:
    def outer(wrapped):
        @functools.wraps(wrapped)
        async def inner(*args, **kwargs):
            return await asyncio.wait_for(wrapped(*args, **kwargs), timeout=seconds)
        return inner
    return outer
