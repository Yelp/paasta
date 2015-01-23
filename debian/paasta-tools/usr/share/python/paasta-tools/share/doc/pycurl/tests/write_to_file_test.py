#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et

import unittest
import pycurl
import tempfile

from . import appmanager

setup_module, teardown_module = appmanager.setup(('app', 8380))

class WriteToFileTest(unittest.TestCase):
    def setUp(self):
        self.curl = pycurl.Curl()
    
    def tearDown(self):
        self.curl.close()
    
    def test_get_to_file(self):
        self.curl.setopt(pycurl.URL, 'http://localhost:8380/success')
        f = tempfile.NamedTemporaryFile()
        try:
            self.curl.setopt(pycurl.WRITEFUNCTION, f.write)
            self.curl.perform()
            f.seek(0)
            body = f.read()
        finally:
            f.close()
        self.assertEqual('success', body)
