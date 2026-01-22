import asyncio
import functools
import inspect
import threading
import time
import weakref
from collections import defaultdict
from typing import Any
from typing import AsyncIterable
from typing import Awaitable
from typing import Callable
from typing import Coroutine
from typing import Dict
from typing import List
from typing import Optional
from typing import ParamSpec
from typing import TypeVar

P = ParamSpec("P")
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
        cache2: Dict = (
            cache if cache is not None else {}
        )  # Should be Dict[Any, T] but that doesn't work.

        def outer(wrapped):
            @functools.wraps(wrapped)
            async def inner(*args, **kwargs):
                return await call_or_get_from_cache(cache2, wrapped, args, args, kwargs)

            return inner

    return outer


async def aiter_to_list(
    aiter: AsyncIterable[T],
) -> List[T]:
    return [x async for x in aiter]


def async_timeout(
    seconds: int = 10,
) -> Callable[
    [Callable[..., Coroutine[Any, Any, T]]],
    Callable[..., Coroutine[Any, Any, T]],  # wrapped  # inner
]:
    def outer(wrapped):
        @functools.wraps(wrapped)
        async def inner(*args, **kwargs):
            return await asyncio.wait_for(wrapped(*args, **kwargs), timeout=seconds)

        return inner

    return outer


def _ensure_coroutine(awaitable: Awaitable[T]) -> Coroutine[Any, Any, T]:
    # Normalize any awaitable into a coroutine so run_sync can drive it.
    if inspect.iscoroutine(awaitable):
        return awaitable
    if inspect.isawaitable(awaitable):

        async def _await_wrapper() -> T:
            return await awaitable

        return _await_wrapper()
    raise TypeError("run_sync expected an awaitable or coroutine")


# run_sync must reuse a loop to avoid cached Futures (from async_ttl_cache)
# being tied to a closed loop on subsequent synchronous calls.
# Thread-local storage keeps loops isolated per thread.
_run_sync_loop_local = threading.local()


def _get_run_sync_loop() -> asyncio.AbstractEventLoop:
    # Lazily create one loop per thread; keep it open for reuse.
    loop = getattr(_run_sync_loop_local, "loop", None)
    if loop is None or loop.is_closed():
        loop = asyncio.new_event_loop()
        _run_sync_loop_local.loop = loop
    return loop


# Based on ideas from notion/a_sync (Apache-2.0); reimplemented with stdlib asyncio.
# https://github.com/notion/a_sync
def run_sync(
    async_fn_or_awaitable: Callable[P, Awaitable[T]] | Awaitable[T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    """Run an async function or awaitable from sync code using a shared loop."""
    # Accept either a callable or a pre-built awaitable.
    # Enforce sync-only usage: this must not run inside an active event loop.
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        if inspect.iscoroutine(async_fn_or_awaitable):
            async_fn_or_awaitable.close()
        raise RuntimeError(
            "run_sync cannot be called from a running event loop; use await instead"
        )

    if callable(async_fn_or_awaitable):
        awaitable = async_fn_or_awaitable(*args, **kwargs)
    else:
        if args or kwargs:
            raise TypeError("run_sync got args for a non-callable awaitable")
        awaitable = async_fn_or_awaitable

    # Reuse a per-thread loop so cached Futures remain attached to a live loop.
    loop = _get_run_sync_loop()
    return loop.run_until_complete(_ensure_coroutine(awaitable))


def to_blocking(
    async_fn: Callable[P, Awaitable[T]],
) -> Callable[P, T]:
    @functools.wraps(async_fn)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        return run_sync(async_fn, *args, **kwargs)

    return wrapper
