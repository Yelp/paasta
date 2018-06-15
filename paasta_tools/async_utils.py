import asyncio
import functools
import time
from typing import Awaitable
from typing import Callable
from typing import Dict  # noqa: imported for typing
from typing import TypeVar


_AsyncCacheRetT = TypeVar('_AsyncCacheRetT')


def async_ttl_cache(
    ttl: int=300,
) -> Callable[
    [Callable[..., Awaitable[_AsyncCacheRetT]]],  # wrapped
    Callable[..., Awaitable[_AsyncCacheRetT]],  # inner
]:
    _cache: Dict = {}  # Should be Dict[Any, _AsyncCacheRetT] but that doesn't work.

    def outer(wrapped):
        @functools.wraps(wrapped)
        async def inner(*args, **kwargs):
            key = functools._make_key(args, kwargs, typed=False)
            try:
                future, last_update = _cache[key]
                if ttl > 0 and time.time() - last_update > ttl:
                    raise KeyError
            except KeyError:
                future = asyncio.ensure_future(wrapped(*args, **kwargs))
                # set the timestamp to +infinity so that we always wait on the in-flight request.
                _cache[key] = (future, float('Inf'))
            value = await future
            _cache[key] = (future, time.time())
            return value
        return inner
    return outer
