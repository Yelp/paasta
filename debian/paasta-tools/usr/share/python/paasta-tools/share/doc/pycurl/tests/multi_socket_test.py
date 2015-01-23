#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et

import pycurl
import unittest

from . import appmanager
from . import util

setup_module_1, teardown_module_1 = appmanager.setup(('app', 8380))
setup_module_2, teardown_module_2 = appmanager.setup(('app', 8381))
setup_module_3, teardown_module_3 = appmanager.setup(('app', 8382))

def setup_module(mod):
    setup_module_1(mod)
    setup_module_2(mod)
    setup_module_3(mod)

def teardown_module(mod):
    teardown_module_3(mod)
    teardown_module_2(mod)
    teardown_module_1(mod)

class MultiSocketTest(unittest.TestCase):
    def test_multi_socket(self):
        urls = [
            'http://localhost:8380/success',
            'http://localhost:8381/success',
            'http://localhost:8382/success',
        ]

        socket_events = []

        # socket callback
        def socket(event, socket, multi, data):
            #print(event, socket, multi, data)
            socket_events.append((event, multi))

        # init
        m = pycurl.CurlMulti()
        m.setopt(pycurl.M_PIPELINING, 1)
        m.setopt(pycurl.M_SOCKETFUNCTION, socket)
        m.handles = []
        for url in urls:
            c = pycurl.Curl()
            # save info in standard Python attributes
            c.url = url
            c.body = util.StringIO()
            c.http_code = -1
            m.handles.append(c)
            # pycurl API calls
            c.setopt(c.URL, c.url)
            c.setopt(c.WRITEFUNCTION, c.body.write)
            m.add_handle(c)

        # get data
        num_handles = len(m.handles)
        while num_handles:
            while 1:
                ret, num_handles = m.socket_all()
                if ret != pycurl.E_CALL_MULTI_PERFORM:
                    break
            # currently no more I/O is pending, could do something in the meantime
            # (display a progress bar, etc.)
            m.select(0.1)

        for c in m.handles:
            # save info in standard Python attributes
            c.http_code = c.getinfo(c.HTTP_CODE)

        # at least in and remove events per socket
        assert len(socket_events) >= 6

        # print result
        for c in m.handles:
            self.assertEqual('success', c.body.getvalue())
            self.assertEqual(200, c.http_code)
            
            # multi, not curl handle
            self.check(pycurl.POLL_IN, m, socket_events)
            self.check(pycurl.POLL_REMOVE, m, socket_events)
        
        # close handles
        for c in m.handles:
            # pycurl API calls
            m.remove_handle(c)
            c.close()
        m.close()
    
    def check(self, event, multi, socket_events):
        for event_, multi_ in socket_events:
            if event == event_ and multi == multi_:
                return
        assert False, '%d %s not found in socket events' % (event, multi)
