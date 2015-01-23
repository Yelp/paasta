#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et

import os.path
import pycurl
import sys
import unittest

class UnsetRangeTest(unittest.TestCase):
    def setUp(self):
        self.curl = pycurl.Curl()

    def tearDown(self):
        self.curl.close()

    def test_unset_range(self):
        def write_cb(data):
            self.read += len(data)
            return None

        # download bytes 0-9 of the script itself through the file:// protocol
        self.read = 0
        self.curl.setopt(pycurl.URL, 'file://' + os.path.abspath(sys.argv[0]))
        self.curl.setopt(pycurl.WRITEFUNCTION, write_cb)
        self.curl.setopt(pycurl.RANGE, '0-9')
        self.curl.perform()
        assert 10 == self.read

        # the RANGE setting should be preserved from the previous transfer
        self.read = 0
        self.curl.perform()
        assert 10 == self.read

        # drop the RANGE setting using unsetopt() and download entire script
        self.read = 0
        self.curl.unsetopt(pycurl.RANGE)
        self.curl.perform()
        assert 10 < self.read

        # now set the RANGE again and check that pycurl takes it into account
        self.read = 0
        self.curl.setopt(pycurl.RANGE, '0-9')
        self.curl.perform()
        assert 10 == self.read

        # now drop the RANGE setting using setopt(..., None)
        self.read = 0
        self.curl.setopt(pycurl.RANGE, None)
        self.curl.perform()
        assert 10 < self.read
