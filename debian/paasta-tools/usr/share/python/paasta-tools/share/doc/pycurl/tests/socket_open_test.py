#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et

import socket
import pycurl
import unittest
try:
    import urllib.parse as urllib_parse
except ImportError:
    import urllib as urllib_parse

from . import appmanager
from . import util

setup_module, teardown_module = appmanager.setup(('app', 8380))

socket_open_called = False

def socket_open(family, socktype, protocol):
    global socket_open_called
    socket_open_called = True
    
    #print(family, socktype, protocol)
    s = socket.socket(family, socktype, protocol)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    return s

class SocketOpenTest(unittest.TestCase):
    def setUp(self):
        self.curl = pycurl.Curl()
    
    def tearDown(self):
        self.curl.close()
    
    def test_socket_open(self):
        self.curl.setopt(pycurl.OPENSOCKETFUNCTION, socket_open)
        self.curl.setopt(self.curl.URL, 'http://localhost:8380/success')
        sio = util.StringIO()
        self.curl.setopt(pycurl.WRITEFUNCTION, sio.write)
        self.curl.perform()
        
        assert socket_open_called
        self.assertEqual('success', sio.getvalue())
