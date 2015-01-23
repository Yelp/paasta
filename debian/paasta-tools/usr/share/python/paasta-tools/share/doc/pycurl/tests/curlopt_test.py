#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et

import pycurl
import unittest
import nose.plugins.skip

from . import util

class CurloptTest(unittest.TestCase):
    def test_username(self):
        # CURLOPT_USERNAME was introduced in libcurl-7.19.1
        if util.pycurl_version_less_than(7, 19, 1):
            raise nose.plugins.skip.SkipTest('libcurl < 7.19.1')
        
        assert hasattr(pycurl, 'USERNAME')
        assert hasattr(pycurl, 'PASSWORD')
        assert hasattr(pycurl, 'PROXYUSERNAME')
        assert hasattr(pycurl, 'PROXYPASSWORD')
