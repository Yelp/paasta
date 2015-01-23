#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et

import gc
import os.path
import pycurl
import unittest
try:
    import json
except ImportError:
    import simplejson as json

from . import appmanager
from . import util

setup_module, teardown_module = appmanager.setup(('app', 8380))

class TestString(str):
    def __del__(self):
        self.replace('1', '2')
        #print self
        #print 'd'

class SetoptLifecycleTest(unittest.TestCase):
    # separate method to permit pf to go out of scope and be
    # garbage collected before perform call
    def do_setopt(self, curl, index):
        pf = TestString('&'.join(50*['field=value%d' % (index,)]))
        curl.setopt(pycurl.URL, 'http://localhost:8380/postfields')
        curl.setopt(pycurl.POSTFIELDS, pf)
    
    # This test takes 6+ seconds to run.
    # It seems to pass with broken pycurl code when run by itself,
    # but fails when run as part of the entire test suite.
    def test_postfields_lifecycle(self):
        requests = []
        for i in range(1000):
            curl = pycurl.Curl()
            self.do_setopt(curl, i)
            gc.collect()
            requests.append(curl)
        
        # send requests here to permit maximum garbage recycling
        for i in range(100):
            curl = requests[i]
            #self.curl.setopt(pycurl.VERBOSE, 1)
            sio = util.StringIO()
            curl.setopt(pycurl.WRITEFUNCTION, sio.write)
            curl.perform()
            self.assertEqual(200, curl.getinfo(pycurl.HTTP_CODE))
            body = sio.getvalue()
            returned_fields = json.loads(body)
            self.assertEqual(dict(field='value%d' % i), returned_fields)
        
        for i in range(100):
            curl = requests[i]
            curl.close()
