import asyncio
import functools
import time
import weakref
from collections import defaultdict
from typing import AsyncIterable
from typing import Awaitable
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import TypeVar


T = TypeVar("T")


# NOTE: this method is not thread-safe due to lack of locking while checking
# and updating the cache
def async_ttl_cache(
    ttl: Optional[float] = 300,
    cleanup_self: bool = False,
    *,
    cache: Optional[Dict] = None,
) -> Callable[
    [Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]  # wrapped  # inner
]:
    async def call_or_get_from_cache(cache, async_func, args_for_key, args, kwargs):
        # Please note that anything which is put into `key` will be in the
        # cache forever, potentially causing memory leaks.  The most common
        # case is the `self` arg pointing to a huge object.  To mitigate that
        # we're using `args_for_key`, which is supposed not contain any huge
        # objects.
        key = functools._make_key(args_for_key, kwargs, typed=False)
        try:
            future, last_update = cache[key]
            if ttl is not None and time.time() - last_update > ttl:
                raise KeyError
        except KeyError:
            future = asyncio.ensure_future(async_func(*args, **kwargs))
            # set the timestamp to +infinity so that we always wait on the in-flight request.
            cache[key] = (future, float("Inf"))

        try:
            value = await future
        except Exception:
            # Only update the cache if it's the same future we awaited and
            # it hasn't already been updated by another coroutine
            # Note also that we use get() in case the key was deleted from the
            # cache by another coroutine
            if cache.get(key) == (future, float("Inf")):
                del cache[key]
            raise
        else:
            if cache.get(key) == (future, float("Inf")):
                cache[key] = (future, time.time())
            return value

    if cleanup_self:
        instance_caches: Dict = cache if cache is not None else defaultdict(dict)

        def on_delete(w):
            del instance_caches[w]

        def outer(wrapped):
            @functools.wraps(wrapped)
            async def inner(self, *args, **kwargs):
                w = weakref.ref(self, on_delete)
                self_cache = instance_caches[w]
                return await call_or_get_from_cache(
                    self_cache, wrapped, args, (self,) + args, kwargs
                )

            return inner

    else:
        cache2: Dict = cache if cache is not None else {}  # Should be Dict[Any, T] but that doesn't work.

        def outer(wrapped):
            @functools.wraps(wrapped)
            async def inner(*args, **kwargs):
                return await call_or_get_from_cache(cache2, wrapped, args, args, kwargs)

            return inner

    return outer


async def aiter_to_list(aiter: AsyncIterable[T],) -> List[T]:
    return [x async for x in aiter]


def async_timeout(
    seconds: int = 10,
) -> Callable[
    [Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]  # wrapped  # inner
]:
    def outer(wrapped):
        @functools.wraps(wrapped)
        async def inner(*args, **kwargs):
            return await asyncio.wait_for(wrapped(*args, **kwargs), timeout=seconds)

        return inner

    return outer
