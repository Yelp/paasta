#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et

import pycurl
import unittest
try:
    import json
except ImportError:
    import simplejson as json
try:
    import urllib.parse as urllib_parse
except ImportError:
    import urllib as urllib_parse

from . import appmanager
from . import util

setup_module, teardown_module = appmanager.setup(('app', 8380))

POSTFIELDS = {
    'field1':'value1',
    'field2':'value2 with blanks',
    'field3':'value3',
}
POSTSTRING = urllib_parse.urlencode(POSTFIELDS)

class DataProvider(object):
    def __init__(self):
        self.finished = False

    def read_cb(self, size):
        assert len(POSTSTRING) <= size
        if not self.finished:
            self.finished = True
            return POSTSTRING
        else:
            # Nothing more to read
            return ""

class PostWithReadCallbackTest(unittest.TestCase):
    def setUp(self):
        self.curl = pycurl.Curl()
    
    def tearDown(self):
        self.curl.close()
    
    def test_post_with_read_callback(self):
        d = DataProvider()
        self.curl.setopt(self.curl.URL, 'http://localhost:8380/postfields')
        self.curl.setopt(self.curl.POST, 1)
        self.curl.setopt(self.curl.POSTFIELDSIZE, len(POSTSTRING))
        self.curl.setopt(self.curl.READFUNCTION, d.read_cb)
        #self.curl.setopt(self.curl.VERBOSE, 1)
        sio = util.StringIO()
        self.curl.setopt(pycurl.WRITEFUNCTION, sio.write)
        self.curl.perform()
        
        actual = json.loads(sio.getvalue())
        self.assertEqual(POSTFIELDS, actual)
