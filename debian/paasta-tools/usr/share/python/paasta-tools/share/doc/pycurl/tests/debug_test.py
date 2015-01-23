#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et

import pycurl
import unittest

from . import appmanager
from . import util

setup_module, teardown_module = appmanager.setup(('app', 8380))

class DebugTest(unittest.TestCase):
    def setUp(self):
        self.curl = pycurl.Curl()
        self.debug_entries = []
    
    def tearDown(self):
        self.curl.close()
    
    def debug_function(self, t, b):
        self.debug_entries.append((t, b))
    
    def test_perform_get_with_debug_function(self):
        self.curl.setopt(pycurl.VERBOSE, 1)
        self.curl.setopt(pycurl.DEBUGFUNCTION, self.debug_function)
        self.curl.setopt(pycurl.URL, 'http://localhost:8380/success')
        sio = util.StringIO()
        self.curl.setopt(pycurl.WRITEFUNCTION, sio.write)
        self.curl.perform()
        
        # Some checks with no particular intent
        self.check(0, 'About to connect')
        if util.pycurl_version_less_than(7, 24):
            self.check(0, 'connected')
        else:
            self.check(0, 'Connected to localhost')
        self.check(0, 'port 8380')
        # request
        self.check(2, 'GET /success HTTP/1.1')
        # response
        self.check(1, 'HTTP/1.0 200 OK')
        self.check(1, 'Content-Length: 7')
        # result
        self.check(3, 'success')
    
    def check(self, wanted_t, wanted_b):
        for t, b in self.debug_entries:
            if t == wanted_t and wanted_b in b:
                return
        assert False, "%d: %s not found in debug entries\nEntries are:\n%s" % \
            (wanted_t, wanted_b, repr(self.debug_entries))
