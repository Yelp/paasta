import os

from nose import SkipTest

from kazoo.testing import KazooTestCase


class TestBuildEnvironment(KazooTestCase):

    def setUp(self):
        KazooTestCase.setUp(self)
        if not os.environ.get('TRAVIS'):
            raise SkipTest('Only run build config tests on Travis.')

    def test_gevent_version(self):
        try:
            import gevent
        except ImportError:
            raise SkipTest('gevent not available.')
        env_version = os.environ.get('GEVENT_VERSION')
        if env_version:
            self.assertEqual(env_version, gevent.__version__)

    def test_zookeeper_version(self):
        server_version = self.client.server_version()
        server_version = '.'.join([str(i) for i in server_version])
        env_version = os.environ.get('ZOOKEEPER_VERSION')
        if env_version:
            self.assertEqual(env_version, server_version)
