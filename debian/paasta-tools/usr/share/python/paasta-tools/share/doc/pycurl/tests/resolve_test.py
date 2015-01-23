# -*- coding: iso-8859-1 -*-

import pycurl
import unittest
import nose.plugins.skip

from . import appmanager
from . import util

setup_module, teardown_module = appmanager.setup(('app', 8380))

class ResolveTest(unittest.TestCase):
    def setUp(self):
        self.curl = pycurl.Curl()
    
    def tearDown(self):
        self.curl.close()
    
    def test_resolve(self):
        if util.pycurl_version_less_than(7, 21, 3) and not hasattr(pycurl, 'RESOLVE'):
            raise nose.plugins.skip.SkipTest('libcurl < 7.21.3 or no RESOLVE')
        
        self.curl.setopt(pycurl.URL, 'http://p.localhost:8380/success')
        self.curl.setopt(pycurl.RESOLVE, ['p.localhost:8380:127.0.0.1'])
        self.curl.perform()
        self.assertEqual(200, self.curl.getinfo(pycurl.RESPONSE_CODE))
