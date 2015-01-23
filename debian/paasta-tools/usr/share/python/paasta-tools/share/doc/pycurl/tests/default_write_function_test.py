#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et

import unittest
import pycurl
import sys
import tempfile
import os

from . import appmanager

setup_module, teardown_module = appmanager.setup(('app', 8380))

STDOUT_FD_NUM = 1

def try_fsync(fd):
    try:
        os.fsync(fd)
    except OSError:
        # On travis:
        # OSError: [Errno 22] Invalid argument
        # ignore
        pass

class DefaultWriteFunctionTest(unittest.TestCase):
    def setUp(self):
        self.curl = pycurl.Curl()
    
    def tearDown(self):
        self.curl.close()
    
    def test_perform_get(self):
        # This test performs a GET request without doing anything else.
        # Unfortunately, the default curl behavior is to print response
        # body to standard output, which spams test output.
        # As a result this test is commented out. Uncomment for debugging.
        # test_perform_get_with_default_write_function is the test
        # which exercises default curl write handler.
        
        self.curl.setopt(pycurl.URL, 'http://localhost:8380/success')
        self.curl.perform()
        # If this flush is not done, stdout output bleeds into the next test
        # that is executed (without nose output capture)
        sys.stdout.flush()
        try_fsync(STDOUT_FD_NUM)
    
    # I have a really hard time getting this to work with nose output capture
    def skip_perform_get_with_default_write_function(self):
        self.curl.setopt(pycurl.URL, 'http://localhost:8380/success')
        f = tempfile.NamedTemporaryFile()
        try:
        #with open('w', 'w+') as f:
            # nose output capture plugin replaces sys.stdout with a StringIO
            # instance. We want to redirect the underlying file descriptor
            # anyway because underlying C code uses it.
            # Therefore:
            # 1. Use file descriptor 1 rather than sys.stdout.fileno() to
            # reference the standard output file descriptor.
            # 2. We do not touch sys.stdout. This means anything written to
            # sys.stdout will be captured by nose, and not make it to our code.
            # But the output we care about happens at libcurl level, below
            # nose, therefore this is fine.
            saved_stdout_fd = os.dup(STDOUT_FD_NUM)
            os.dup2(f.fileno(), STDOUT_FD_NUM)
            #os.dup2(1, 100)
            #os.dup2(2, 1)
            # We also need to flush the output that libcurl wrote to stdout.
            # Since sys.stdout might be nose's StringIO instance, open the
            # stdout file descriptor manually.
            
            try:
                self.curl.perform()
                sys.stdout.flush()
            finally:
                try_fsync(STDOUT_FD_NUM)
                os.dup2(saved_stdout_fd, STDOUT_FD_NUM)
                os.close(saved_stdout_fd)
                #os.dup2(100, 1)
            f.seek(0)
            body = f.read()
        finally:
            f.close()
        self.assertEqual('success', body)
