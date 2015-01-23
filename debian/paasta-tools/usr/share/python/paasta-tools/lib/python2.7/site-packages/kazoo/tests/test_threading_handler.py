import threading
import unittest

import mock
from nose.tools import assert_raises
from nose.tools import eq_
from nose.tools import raises


class TestThreadingHandler(unittest.TestCase):
    def _makeOne(self, *args):
        from kazoo.handlers.threading import SequentialThreadingHandler
        return SequentialThreadingHandler(*args)

    def _getAsync(self, *args):
        from kazoo.handlers.threading import AsyncResult
        return AsyncResult

    def test_proper_threading(self):
        h = self._makeOne()
        h.start()
        # In Python 3.3 _Event is gone, before Event is function
        event_class = getattr(threading, '_Event', threading.Event)
        assert isinstance(h.event_object(), event_class)

    def test_matching_async(self):
        h = self._makeOne()
        h.start()
        async = self._getAsync()
        assert isinstance(h.async_result(), async)

    def test_exception_raising(self):
        h = self._makeOne()

        @raises(h.timeout_exception)
        def testit():
            raise h.timeout_exception("This is a timeout")
        testit()

    def test_double_start_stop(self):
        h = self._makeOne()
        h.start()
        self.assertTrue(h._running)
        h.start()
        h.stop()
        h.stop()
        self.assertFalse(h._running)


class TestThreadingAsync(unittest.TestCase):
    def _makeOne(self, *args):
        from kazoo.handlers.threading import AsyncResult
        return AsyncResult(*args)

    def _makeHandler(self):
        from kazoo.handlers.threading import SequentialThreadingHandler
        return SequentialThreadingHandler()

    def test_ready(self):
        mock_handler = mock.Mock()
        async = self._makeOne(mock_handler)

        eq_(async.ready(), False)
        async.set('val')
        eq_(async.ready(), True)
        eq_(async.successful(), True)
        eq_(async.exception, None)

    def test_callback_queued(self):
        mock_handler = mock.Mock()
        mock_handler.completion_queue = mock.Mock()
        async = self._makeOne(mock_handler)

        async.rawlink(lambda a: a)
        async.set('val')

        assert mock_handler.completion_queue.put.called

    def test_set_exception(self):
        mock_handler = mock.Mock()
        mock_handler.completion_queue = mock.Mock()
        async = self._makeOne(mock_handler)
        async.rawlink(lambda a: a)
        async.set_exception(ImportError('Error occured'))

        assert isinstance(async.exception, ImportError)
        assert mock_handler.completion_queue.put.called

    def test_get_wait_while_setting(self):
        mock_handler = mock.Mock()
        async = self._makeOne(mock_handler)

        lst = []
        bv = threading.Event()
        cv = threading.Event()

        def wait_for_val():
            bv.set()
            val = async.get()
            lst.append(val)
            cv.set()
        th = threading.Thread(target=wait_for_val)
        th.start()
        bv.wait()

        async.set('fred')
        cv.wait()
        eq_(lst, ['fred'])
        th.join()

    def test_get_with_nowait(self):
        mock_handler = mock.Mock()
        async = self._makeOne(mock_handler)
        timeout = self._makeHandler().timeout_exception

        @raises(timeout)
        def test_it():
            async.get(block=False)
        test_it()

        @raises(timeout)
        def test_nowait():
            async.get_nowait()
        test_nowait()

    def test_get_with_exception(self):
        mock_handler = mock.Mock()
        async = self._makeOne(mock_handler)

        lst = []
        bv = threading.Event()
        cv = threading.Event()

        def wait_for_val():
            bv.set()
            try:
                val = async.get()
            except ImportError:
                lst.append('oops')
            else:
                lst.append(val)
            cv.set()
        th = threading.Thread(target=wait_for_val)
        th.start()
        bv.wait()

        async.set_exception(ImportError)
        cv.wait()
        eq_(lst, ['oops'])
        th.join()

    def test_wait(self):
        mock_handler = mock.Mock()
        async = self._makeOne(mock_handler)

        lst = []
        bv = threading.Event()
        cv = threading.Event()

        def wait_for_val():
            bv.set()
            try:
                val = async.wait(10)
            except ImportError:
                lst.append('oops')
            else:
                lst.append(val)
            cv.set()
        th = threading.Thread(target=wait_for_val)
        th.start()
        bv.wait(10)

        async.set("fred")
        cv.wait(15)
        eq_(lst, [True])
        th.join()

    def test_set_before_wait(self):
        mock_handler = mock.Mock()
        async = self._makeOne(mock_handler)

        lst = []
        cv = threading.Event()
        async.set('fred')

        def wait_for_val():
            val = async.get()
            lst.append(val)
            cv.set()
        th = threading.Thread(target=wait_for_val)
        th.start()
        cv.wait()
        eq_(lst, ['fred'])
        th.join()

    def test_set_exc_before_wait(self):
        mock_handler = mock.Mock()
        async = self._makeOne(mock_handler)

        lst = []
        cv = threading.Event()
        async.set_exception(ImportError)

        def wait_for_val():
            try:
                val = async.get()
            except ImportError:
                lst.append('ooops')
            else:
                lst.append(val)
            cv.set()
        th = threading.Thread(target=wait_for_val)
        th.start()
        cv.wait()
        eq_(lst, ['ooops'])
        th.join()

    def test_linkage(self):
        mock_handler = mock.Mock()
        async = self._makeOne(mock_handler)
        cv = threading.Event()

        lst = []

        def add_on():
            lst.append(True)

        def wait_for_val():
            async.get()
            cv.set()

        th = threading.Thread(target=wait_for_val)
        th.start()

        async.rawlink(add_on)
        async.set('fred')
        assert mock_handler.completion_queue.put.called
        async.unlink(add_on)
        cv.wait()
        eq_(async.value, 'fred')
        th.join()

    def test_linkage_not_ready(self):
        mock_handler = mock.Mock()
        async = self._makeOne(mock_handler)

        lst = []

        def add_on():
            lst.append(True)

        async.set('fred')
        assert not mock_handler.completion_queue.called
        async.rawlink(add_on)
        assert mock_handler.completion_queue.put.called

    def test_link_and_unlink(self):
        mock_handler = mock.Mock()
        async = self._makeOne(mock_handler)

        lst = []

        def add_on():
            lst.append(True)

        async.rawlink(add_on)
        assert not mock_handler.completion_queue.put.called
        async.unlink(add_on)
        async.set('fred')
        assert not mock_handler.completion_queue.put.called

    def test_captured_exception(self):
        from kazoo.handlers.utils import capture_exceptions

        mock_handler = mock.Mock()
        async = self._makeOne(mock_handler)

        @capture_exceptions(async)
        def exceptional_function():
            return 1/0

        exceptional_function()

        assert_raises(ZeroDivisionError, async.get)

    def test_no_capture_exceptions(self):
        from kazoo.handlers.utils import capture_exceptions

        mock_handler = mock.Mock()
        async = self._makeOne(mock_handler)

        lst = []

        def add_on():
            lst.append(True)

        async.rawlink(add_on)

        @capture_exceptions(async)
        def regular_function():
            return True

        regular_function()

        assert not mock_handler.completion_queue.put.called

    def test_wraps(self):
        from kazoo.handlers.utils import wrap

        mock_handler = mock.Mock()
        async = self._makeOne(mock_handler)

        lst = []

        def add_on(result):
            lst.append(result.get())

        async.rawlink(add_on)

        @wrap(async)
        def regular_function():
            return 'hello'

        assert regular_function() == 'hello'
        assert mock_handler.completion_queue.put.called
        assert async.get() == 'hello'
