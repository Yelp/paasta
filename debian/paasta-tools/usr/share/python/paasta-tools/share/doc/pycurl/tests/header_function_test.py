#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et

import pycurl
import unittest
import time as _time

from . import appmanager
from . import util

setup_module, teardown_module = appmanager.setup(('app', 8380))

class HeaderFunctionTest(unittest.TestCase):
    def setUp(self):
        self.curl = pycurl.Curl()
        self.header_lines = []
    
    def tearDown(self):
        self.curl.close()
    
    def header_function(self, line):
        self.header_lines.append(line)
    
    def test_get(self):
        self.curl.setopt(pycurl.URL, 'http://localhost:8380/success')
        sio = util.StringIO()
        self.curl.setopt(pycurl.WRITEFUNCTION, sio.write)
        self.curl.setopt(pycurl.HEADERFUNCTION, self.header_function)
        self.curl.perform()
        self.assertEqual('success', sio.getvalue())
        
        assert len(self.header_lines) > 0
        self.assertEqual("HTTP/1.0 200 OK\r\n", self.header_lines[0])
        # day of week
        # important: must be in utc
        todays_day = _time.strftime('%a', _time.gmtime())
        # Date: Sun, 03 Mar 2013 05:38:12 GMT\r\n
        self.check('Date: %s' % todays_day)
        # Server: WSGIServer/0.1 Python/2.7.3\r\n
        self.check('Server: WSGIServer')
        self.check('Content-Length: 7')
        self.check('Content-Type: text/html')
    
    def check(self, wanted_text):
        for line in self.header_lines:
            if wanted_text in line:
                return
        assert False, "%s not found in header lines" % wanted_text
