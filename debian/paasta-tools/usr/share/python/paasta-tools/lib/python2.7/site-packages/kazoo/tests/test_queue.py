import uuid

from nose import SkipTest
from nose.tools import eq_, ok_

from kazoo.testing import KazooTestCase


class KazooQueueTests(KazooTestCase):

    def _makeOne(self):
        path = "/" + uuid.uuid4().hex
        return self.client.Queue(path)

    def test_queue_validation(self):
        queue = self._makeOne()
        self.assertRaises(TypeError, queue.put, {})
        self.assertRaises(TypeError, queue.put, b"one", b"100")
        self.assertRaises(TypeError, queue.put, b"one", 10.0)
        self.assertRaises(ValueError, queue.put, b"one", -100)
        self.assertRaises(ValueError, queue.put, b"one", 100000)

    def test_empty_queue(self):
        queue = self._makeOne()
        eq_(len(queue), 0)
        self.assertTrue(queue.get() is None)
        eq_(len(queue), 0)

    def test_queue(self):
        queue = self._makeOne()
        queue.put(b"one")
        queue.put(b"two")
        queue.put(b"three")
        eq_(len(queue), 3)

        eq_(queue.get(), b"one")
        eq_(queue.get(), b"two")
        eq_(queue.get(), b"three")
        eq_(len(queue), 0)

    def test_priority(self):
        queue = self._makeOne()
        queue.put(b"four", priority=101)
        queue.put(b"one", priority=0)
        queue.put(b"two", priority=0)
        queue.put(b"three", priority=10)

        eq_(queue.get(), b"one")
        eq_(queue.get(), b"two")
        eq_(queue.get(), b"three")
        eq_(queue.get(), b"four")


class KazooLockingQueueTests(KazooTestCase):

    def setUp(self):
        KazooTestCase.setUp(self)
        ver = self.client.server_version()
        if ver[1] < 4:
            raise SkipTest("Must use zookeeper 3.4 or above")

    def _makeOne(self):
        path = "/" + uuid.uuid4().hex
        return self.client.LockingQueue(path)

    def test_queue_validation(self):
        queue = self._makeOne()
        self.assertRaises(TypeError, queue.put, {})
        self.assertRaises(TypeError, queue.put, b"one", b"100")
        self.assertRaises(TypeError, queue.put, b"one", 10.0)
        self.assertRaises(ValueError, queue.put, b"one", -100)
        self.assertRaises(ValueError, queue.put, b"one", 100000)
        self.assertRaises(TypeError, queue.put_all, {})
        self.assertRaises(TypeError, queue.put_all, [{}])
        self.assertRaises(TypeError, queue.put_all, [b"one"], b"100")
        self.assertRaises(TypeError, queue.put_all, [b"one"], 10.0)
        self.assertRaises(ValueError, queue.put_all, [b"one"], -100)
        self.assertRaises(ValueError, queue.put_all, [b"one"], 100000)

    def test_empty_queue(self):
        queue = self._makeOne()
        eq_(len(queue), 0)
        self.assertTrue(queue.get(0) is None)
        eq_(len(queue), 0)

    def test_queue(self):
        queue = self._makeOne()
        queue.put(b"one")
        queue.put_all([b"two", b"three"])
        eq_(len(queue), 3)

        ok_(not queue.consume())
        ok_(not queue.holds_lock())
        eq_(queue.get(1), b"one")
        ok_(queue.holds_lock())
        # Without consuming, should return the same element
        eq_(queue.get(1), b"one")
        ok_(queue.consume())
        ok_(not queue.holds_lock())
        eq_(queue.get(1), b"two")
        ok_(queue.holds_lock())
        ok_(queue.consume())
        ok_(not queue.holds_lock())
        eq_(queue.get(1), b"three")
        ok_(queue.holds_lock())
        ok_(queue.consume())
        ok_(not queue.holds_lock())
        ok_(not queue.consume())
        eq_(len(queue), 0)

    def test_consume(self):
        queue = self._makeOne()

        queue.put(b"one")
        ok_(not queue.consume())
        queue.get(.1)
        ok_(queue.consume())
        ok_(not queue.consume())

    def test_holds_lock(self):
        queue = self._makeOne()

        ok_(not queue.holds_lock())
        queue.put(b"one")
        queue.get(.1)
        ok_(queue.holds_lock())
        queue.consume()
        ok_(not queue.holds_lock())

    def test_priority(self):
        queue = self._makeOne()
        queue.put(b"four", priority=101)
        queue.put(b"one", priority=0)
        queue.put(b"two", priority=0)
        queue.put(b"three", priority=10)

        eq_(queue.get(1), b"one")
        ok_(queue.consume())
        eq_(queue.get(1), b"two")
        ok_(queue.consume())
        eq_(queue.get(1), b"three")
        ok_(queue.consume())
        eq_(queue.get(1), b"four")
        ok_(queue.consume())

    def test_concurrent_execution(self):
        queue = self._makeOne()
        value1 = []
        value2 = []
        value3 = []
        event1 = self.client.handler.event_object()
        event2 = self.client.handler.event_object()
        event3 = self.client.handler.event_object()

        def get_concurrently(value, event):
            q = self.client.LockingQueue(queue.path)
            value.append(q.get(.1))
            event.set()

        self.client.handler.spawn(get_concurrently, value1, event1)
        self.client.handler.spawn(get_concurrently, value2, event2)
        self.client.handler.spawn(get_concurrently, value3, event3)
        queue.put(b"one")
        event1.wait(.2)
        event2.wait(.2)
        event3.wait(.2)

        result = value1 + value2 + value3
        eq_(result.count(b"one"), 1)
        eq_(result.count(None), 2)
