#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et

# Note: this test is meant to be run from pycurl project root.

import pycurl
import unittest
import os.path

from . import procmgr

setup_module, teardown_module = procmgr.vsftpd_setup()

class PartialFileSource:
    def __init__(self):
        self.__buf = '1234567890.1234567890'
        self.__maxread = None
        self.__bufptr = 0

    def read(self, size):
        p = self.__bufptr
        end = p+size
        if self.__maxread:
            end = min(self.__maxread, end)
        ret = self.__buf[p:end]
        self.__bufptr+= len(ret)
        #print 20*">>>", "read(%s)   ==> %s" % (size, len(ret))
        return ret
      
    def seek(self, offset, origin):
        #print 20*">>>",  "seek(%s, %s)" %  (offset, origin)
        self.__bufptr = offset

    def set_maxread(self, maxread):
        self.__maxread = maxread

class SeekFunctionTest(unittest.TestCase):
    def test_seek_function(self):
        c = pycurl.Curl()
        c.setopt(pycurl.UPLOAD, 1)
        c.setopt(pycurl.URL, "ftp://localhost:8321/tests/tmp/upload.txt")
        c.setopt(pycurl.RESUME_FROM, 0)
        #c.setopt(pycurl.VERBOSE, 1)
        upload_file = PartialFileSource()
        c.setopt(pycurl.READFUNCTION, upload_file.read)
        upload_file.set_maxread(10)
        c.perform()
        
        f = open(os.path.join(os.path.dirname(__file__), 'tmp', 'upload.txt'))
        try:
            content = f.read()
        finally:
            f.close()
        self.assertEqual('1234567890', content)

        c.close()
        del c
        del upload_file

        c = pycurl.Curl()
        c.setopt(pycurl.URL, "ftp://localhost:8321/tests/tmp/upload.txt")
        c.setopt(pycurl.RESUME_FROM, -1)
        c.setopt(pycurl.UPLOAD, 1)
        #c.setopt(pycurl.VERBOSE, 1)
        upload_file = PartialFileSource()
        c.setopt(pycurl.READFUNCTION, upload_file.read)
        c.setopt(pycurl.SEEKFUNCTION, upload_file.seek)
        c.perform()
        c.close()
        
        f = open(os.path.join(os.path.dirname(__file__), 'tmp', 'upload.txt'))
        try:
            content = f.read()
        finally:
            f.close()
        self.assertEqual('1234567890.1234567890', content)
