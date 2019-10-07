import time
from concurrent.futures import ThreadPoolExecutor
from queue import Empty

from pytest import fixture
from pytest import raises
from zake.fake_client import FakeClient

from paasta_tools.deployd.common import BaseServiceInstance
from paasta_tools.deployd.queue import ZKDelayDeadlineQueue


def make_si(wait_until, bounce_by):
    """Just using mock.Mock(wait_until=wait_until, bounce_by=bounce_by) mostly works, but our PriorityQueues
    occasionally will compare two ServiceInstances directly, and Mocks aren't comparable unless you define an __eq__."""
    return BaseServiceInstance(
        service="service",
        instance="instance",
        cluster="cluster",
        bounce_by=bounce_by,
        wait_until=wait_until,
        watcher="watcher",
        bounce_timers=None,
        failures=0,
        processed_count=0,
    )


class TestDelayDeadlineQueue:
    @fixture
    def queue(self):
        client = FakeClient()
        client.start()
        yield ZKDelayDeadlineQueue(client, "/")

    @fixture
    def multiple_queues(self):
        client = FakeClient()
        client.start()
        yield [ZKDelayDeadlineQueue(client, "/") for _ in range(5)]

    def test_put_then_get_single_threaded(self, queue):
        si = make_si(wait_until=time.time() - 0.01, bounce_by=time.time())
        queue.put(si)
        with queue.get(block=False) as result:
            assert result == si

    def test_put_then_get_different_instances(self, multiple_queues):
        queue1 = multiple_queues[0]
        queue2 = multiple_queues[1]

        si = make_si(wait_until=time.time() - 0.01, bounce_by=time.time())
        queue1.put(si)
        with queue2.get(block=False) as result:
            assert result == si

    def test_dont_block_indefinitely_when_wait_until_is_in_future(self, queue):
        """Regression test for a specific bug in the first implementation of DelayDeadlineQueue"""
        # First, put an item with a distant wait_until
        queue.put(make_si(wait_until=time.time() + 100, bounce_by=time.time() + 100))
        # an immediate get should fail.
        with raises(Empty):
            with queue.get(block=False) as result:
                print(f"Should have raised, got {result}")
        # a get with a short timeout should fail.
        with raises(Empty):
            with queue.get(timeout=0.0001) as result:
                print(f"Should have raised, got {result}")

        wait_until = time.time() + 0.01
        queue.put(make_si(wait_until=wait_until, bounce_by=wait_until))
        # but if we wait a short while it should return.
        with queue.get(
            timeout=1.0
        ) as result:  # This timeout is only there so that if this test fails it doesn't take forever.
            pass
        assert (
            time.time() + 0.001 > wait_until
        )  # queue rounds to millisecond, so we might be slightly under.

    def test_return_immediately_when_blocking_on_empty_queue_and_available_task_comes_in(
        self
    ):
        client = FakeClient()
        client.start()
        queue = ZKDelayDeadlineQueue(client, "/")

        """
        Set up several threads waiting for work; insert several pieces of work; make sure each thread finishes.
        """
        tpe = ThreadPoolExecutor()

        def time_get():
            queue = ZKDelayDeadlineQueue(client, "/")
            start_time = time.time()
            with queue.get(timeout=1.0) as si:
                pass
            return time.time() - start_time, si

        fut1 = tpe.submit(time_get)
        fut2 = tpe.submit(time_get)
        fut3 = tpe.submit(time_get)

        begin = time.time()
        si1 = make_si(wait_until=begin, bounce_by=begin)
        queue.put(si1)
        si2 = make_si(wait_until=begin + 0.01, bounce_by=begin + 0.01)
        queue.put(si2)
        si3 = make_si(wait_until=begin + 0.02, bounce_by=begin + 0.02)
        queue.put(si3)

        times = sorted([x.result(timeout=2.0) for x in [fut1, fut2, fut3]])
        assert times[0][0] < 0.011
        assert times[0][1] == si1
        assert 0.009 < times[1][0] < 0.021
        assert times[1][1] == si2
        assert 0.019 < times[2][0] < 0.031
        assert times[2][1] == si3

    def test_return_immediately_when_blocking_on_distant_wait_until_and_available_task_comes_in(
        self, queue
    ):
        """Same as above, except there's a far-off unavailable item already."""
        queue.put(make_si(wait_until=time.time() + 100, bounce_by=time.time() + 100))
        self.test_return_immediately_when_blocking_on_empty_queue_and_available_task_comes_in()
