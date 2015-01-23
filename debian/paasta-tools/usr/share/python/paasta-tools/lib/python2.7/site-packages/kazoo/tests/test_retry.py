import unittest

from nose.tools import eq_


class TestRetrySleeper(unittest.TestCase):

    def _pass(self):
        pass

    def _fail(self, times=1):
        from kazoo.retry import ForceRetryError
        scope = dict(times=0)

        def inner():
            if scope['times'] >= times:
                pass
            else:
                scope['times'] += 1
                raise ForceRetryError('Failed!')
        return inner

    def _makeOne(self, *args, **kwargs):
        from kazoo.retry import KazooRetry
        return KazooRetry(*args, **kwargs)

    def test_reset(self):
        retry = self._makeOne(delay=0, max_tries=2)
        retry(self._fail())
        eq_(retry._attempts, 1)
        retry.reset()
        eq_(retry._attempts, 0)

    def test_too_many_tries(self):
        from kazoo.retry import RetryFailedError
        retry = self._makeOne(delay=0)
        self.assertRaises(RetryFailedError, retry, self._fail(times=999))
        eq_(retry._attempts, 1)

    def test_maximum_delay(self):
        def sleep_func(_time):
            pass

        retry = self._makeOne(delay=10, max_tries=100, sleep_func=sleep_func)
        retry(self._fail(times=10))
        self.assertTrue(retry._cur_delay < 4000, retry._cur_delay)
        # gevent's sleep function is picky about the type
        eq_(type(retry._cur_delay), float)

    def test_copy(self):
        _sleep = lambda t: None
        retry = self._makeOne(sleep_func=_sleep)
        rcopy = retry.copy()
        self.assertTrue(rcopy.sleep_func is _sleep)


class TestKazooRetry(unittest.TestCase):

    def _makeOne(self, **kw):
        from kazoo.retry import KazooRetry
        return KazooRetry(**kw)

    def test_connection_closed(self):
        from kazoo.exceptions import ConnectionClosedError
        retry = self._makeOne()

        def testit():
            raise ConnectionClosedError()
        self.assertRaises(ConnectionClosedError, retry, testit)

    def test_session_expired(self):
        from kazoo.exceptions import SessionExpiredError
        retry = self._makeOne(max_tries=1)

        def testit():
            raise SessionExpiredError()
        self.assertRaises(Exception, retry, testit)
