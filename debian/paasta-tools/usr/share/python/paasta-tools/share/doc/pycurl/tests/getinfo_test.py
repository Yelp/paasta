#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et

import pycurl
import unittest

from . import appmanager
from . import util

setup_module, teardown_module = appmanager.setup(('app', 8380))

class GetinfoTest(unittest.TestCase):
    def setUp(self):
        self.curl = pycurl.Curl()
    
    def tearDown(self):
        self.curl.close()
    
    def test_getinfo(self):
        self.curl.setopt(pycurl.URL, 'http://localhost:8380/success')
        sio = util.StringIO()
        self.curl.setopt(pycurl.WRITEFUNCTION, sio.write)
        self.curl.perform()
        self.assertEqual('success', sio.getvalue())
        
        self.assertEqual(200, self.curl.getinfo(pycurl.HTTP_CODE))
        assert type(self.curl.getinfo(pycurl.TOTAL_TIME)) is float
        assert self.curl.getinfo(pycurl.TOTAL_TIME) > 0
        assert self.curl.getinfo(pycurl.TOTAL_TIME) < 1
        assert type(self.curl.getinfo(pycurl.SPEED_DOWNLOAD)) is float
        assert self.curl.getinfo(pycurl.SPEED_DOWNLOAD) > 0
        self.assertEqual(7, self.curl.getinfo(pycurl.SIZE_DOWNLOAD))
        self.assertEqual('http://localhost:8380/success', self.curl.getinfo(pycurl.EFFECTIVE_URL))
        self.assertEqual('text/html; charset=utf-8', self.curl.getinfo(pycurl.CONTENT_TYPE).lower())
        assert type(self.curl.getinfo(pycurl.NAMELOOKUP_TIME)) is float
        assert self.curl.getinfo(pycurl.NAMELOOKUP_TIME) > 0
        assert self.curl.getinfo(pycurl.NAMELOOKUP_TIME) < 1
        self.assertEqual(0, self.curl.getinfo(pycurl.REDIRECT_TIME))
        self.assertEqual(0, self.curl.getinfo(pycurl.REDIRECT_COUNT))
        # time not requested
        self.assertEqual(-1, self.curl.getinfo(pycurl.INFO_FILETIME))
