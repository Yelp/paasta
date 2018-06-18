import asyncio
import functools
import time
from typing import AsyncIterable
from typing import Awaitable
from typing import Callable
from typing import Dict  # noqa: imported for typing
from typing import List
from typing import TypeVar


T = TypeVar('T')


def async_ttl_cache(
    ttl: float=300,
) -> Callable[
    [Callable[..., Awaitable[T]]],  # wrapped
    Callable[..., Awaitable[T]],  # inner
]:
    _cache: Dict = {}  # Should be Dict[Any, T] but that doesn't work.

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


async def aiter_to_list(
    aiter: AsyncIterable[T],
) -> List[T]:
    return [x async for x in aiter]


def async_timeout(
    seconds: int=10,
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
