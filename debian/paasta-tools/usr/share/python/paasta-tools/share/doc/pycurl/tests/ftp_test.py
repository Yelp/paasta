#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et

# Note: this test is meant to be run from pycurl project root.

import pycurl
import unittest

from . import util
from . import procmgr

setup_module, teardown_module = procmgr.vsftpd_setup()

class FtpTest(unittest.TestCase):
    def setUp(self):
        self.curl = pycurl.Curl()
    
    def tearDown(self):
        self.curl.close()
    
    def test_get_ftp(self):
        self.curl.setopt(pycurl.URL, 'ftp://localhost:8321')
        sio = util.StringIO()
        self.curl.setopt(pycurl.WRITEFUNCTION, sio.write)
        self.curl.perform()
        
        result = sio.getvalue()
        assert 'README.rst' in result
        assert 'INSTALL' in result
    
    # XXX this test needs to be fixed
    def test_quote(self):
        self.curl.setopt(pycurl.URL, 'ftp://localhost:8321')
        sio = util.StringIO()
        self.curl.setopt(pycurl.WRITEFUNCTION, sio.write)
        self.curl.setopt(pycurl.QUOTE, ['CWD tests'])
        self.curl.perform()
        
        result = sio.getvalue()
        assert 'README.rst' not in result
        assert 'ftp_test.py' in result
    
    def test_epsv(self):
        self.curl.setopt(pycurl.URL, 'ftp://localhost:8321')
        sio = util.StringIO()
        self.curl.setopt(pycurl.WRITEFUNCTION, sio.write)
        self.curl.setopt(pycurl.FTP_USE_EPSV, 1)
        self.curl.perform()
        
        result = sio.getvalue()
        assert 'README.rst' in result
        assert 'INSTALL' in result
