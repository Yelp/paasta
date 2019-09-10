import asyncio
import functools

import mock
import pytest

from paasta_tools.async_utils import async_ttl_cache


@pytest.mark.asyncio
async def test_async_ttl_cache_hit():
    return_values = iter(range(10))

    @async_ttl_cache(ttl=None)
    async def range_coroutine():
        return next(return_values)

    assert await range_coroutine() == await range_coroutine()


@pytest.mark.asyncio
async def test_async_ttl_cache_miss():
    return_values = iter(range(10))

    @async_ttl_cache(ttl=0)
    async def range_coroutine():
        return next(return_values)

    assert await range_coroutine() != await range_coroutine()


@pytest.mark.asyncio
async def test_async_ttl_cache_doesnt_cache_failures():
    flaky_error_raiser = mock.Mock(side_effect=[Exception, None])

    @async_ttl_cache(ttl=None)
    async def flaky_coroutine():
        return flaky_error_raiser()

    with pytest.raises(Exception):
        await flaky_coroutine()

    # if we were caching failures, this would fail
    assert await flaky_coroutine() is None


@pytest.mark.asyncio
async def test_async_ttl_cache_returns_in_flight_future():
    return_values = iter(range(10))
    condition = asyncio.Condition()
    event = asyncio.Event()

    class WaitingCoroutines:
        count = 0

    # Wait until we have enough coroutines waiting to return a result.  This
    # ensures that dependent coroutines have a chance to get a future out of
    # the cache
    @async_ttl_cache(ttl=0)
    async def range_coroutine():
        await event.wait()
        return next(return_values)

    # Wait until we have enough coroutines waiting on range_coroutine, then
    # wake range_coroutine
    async def event_setter():
        async with condition:
            while WaitingCoroutines.count != 2:
                await condition.wait()
            event.set()

    # Keep track of how many waiting range_coroutines we have to ensure both
    # have had a chance to get the in-flight future out of the cache.  This has
    # to be separate from range_coroutine since we only end up with one
    # invocation of that method due to caching.  It also has to be separate
    # from event_setter to ensure that the event is not set until both
    # coroutines are waiting.
    async def cache_waiter():
        async with condition:
            WaitingCoroutines.count += 1
            condition.notify_all()
        return await range_coroutine()

    event_setter_future = asyncio.ensure_future(event_setter())
    future1 = asyncio.ensure_future(cache_waiter())
    future2 = asyncio.ensure_future(cache_waiter())
    await asyncio.wait([event_setter_future, future1, future2])

    assert future1.result() == future2.result() == 0


@pytest.mark.asyncio
async def test_async_ttl_cache_dont_overwrite_new_cache_entry():
    """Make sure that we don't overwrite a new cache entry that was placed
    while we were waiting to handle the result of a previously cached future
    """
    range_continue_event = asyncio.Event()
    update_cache_event = asyncio.Event()
    return_values = iter(range(10))

    # Wait until awaiter has had a chance to get the in-flight future out of
    # the cache, then signal to the cache_updater to replace the cached future
    # before returning.  Because cache_updater is signalled first, it will
    # replace the previously cached future before async_ttl_cache decides
    # whether save the result of that future in the cache
    async def range_coroutine():
        await range_continue_event.wait()
        update_cache_event.set()
        return next(return_values)

    range_coroutine_future = asyncio.ensure_future(range_coroutine())
    cache_key = functools._make_key((), {}, typed=False)
    cache = {cache_key: (range_coroutine_future, float("Inf"))}

    cached_range_coroutine = async_ttl_cache(cache=cache, ttl=0)(range_coroutine)

    new_range_coroutine_future = asyncio.ensure_future(range_coroutine())

    async def awaiter():
        range_continue_event.set()
        await cached_range_coroutine()

    async def cache_updater():
        await update_cache_event.wait()
        cache[cache_key] = (new_range_coroutine_future, float("Inf"))

    await asyncio.gather(awaiter(), cache_updater())
    assert cache[cache_key] == (new_range_coroutine_future, float("Inf"))
