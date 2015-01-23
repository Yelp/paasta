#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et

import threading
import pycurl
import unittest
try:
    import urllib.parse as urllib_parse
except ImportError:
    import urllib as urllib_parse

from . import appmanager
from . import util

setup_module, teardown_module = appmanager.setup(('app', 8380))

class WorkerThread(threading.Thread):

    def __init__(self, share):
        threading.Thread.__init__(self)
        self.curl = pycurl.Curl()
        self.curl.setopt(pycurl.URL, 'http://localhost:8380/success')
        self.curl.setopt(pycurl.SHARE, share)
        self.sio = util.StringIO()
        self.curl.setopt(pycurl.WRITEFUNCTION, self.sio.write)

    def run(self):
        self.curl.perform()
        self.curl.close()

class ShareTest(unittest.TestCase):
    def test_share(self):
        s = pycurl.CurlShare()
        s.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_COOKIE)
        s.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_DNS)

        t1 = WorkerThread(s)
        t2 = WorkerThread(s)

        t1.start()
        t2.start()
        
        t1.join()
        t2.join()
        
        del s
        
        self.assertEqual('success', t1.sio.getvalue())
        self.assertEqual('success', t2.sio.getvalue())
