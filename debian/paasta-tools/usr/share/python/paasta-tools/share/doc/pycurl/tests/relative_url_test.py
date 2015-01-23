#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et

# uses the high level interface
import curl
import unittest

from . import appmanager

setup_module, teardown_module = appmanager.setup(('app', 8380))

class RelativeUrlTest(unittest.TestCase):
    def setUp(self):
        self.curl = curl.Curl('http://localhost:8380/')
    
    def tearDown(self):
        self.curl.close()
    
    def test_get_relative(self):
        self.curl.get('/success')
        self.assertEqual('success', self.curl.body())
