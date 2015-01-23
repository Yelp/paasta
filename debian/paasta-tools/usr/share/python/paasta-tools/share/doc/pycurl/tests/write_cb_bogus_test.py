#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et

import os.path
import pycurl
import sys
import unittest

class WriteAbortTest(unittest.TestCase):
    def setUp(self):
        self.curl = pycurl.Curl()

    def tearDown(self):
        self.curl.close()

    def write_cb_returning_string(self, data):
        return 'foo'

    def write_cb_returning_float(self, data):
        return 0.5

    def test_write_cb_returning_string(self):
        self.check(self.write_cb_returning_string)
    
    def test_write_cb_returning_float(self):
        self.check(self.write_cb_returning_float)
    
    def check(self, write_cb):
        # download the script itself through the file:// protocol into write_cb
        c = pycurl.Curl()
        self.curl.setopt(pycurl.URL, 'file://' + os.path.abspath(sys.argv[0]))
        self.curl.setopt(pycurl.WRITEFUNCTION, write_cb)
        try:
            self.curl.perform()
        except pycurl.error:
            err, msg = sys.exc_info()[1]
            # we expect pycurl.E_WRITE_ERROR as the response
            assert pycurl.E_WRITE_ERROR == err

        # actual error
        assert hasattr(sys, 'last_type')
        self.assertEqual(pycurl.error, sys.last_type)
        assert hasattr(sys, 'last_value')
        self.assertEqual('write callback must return int or None', str(sys.last_value))
