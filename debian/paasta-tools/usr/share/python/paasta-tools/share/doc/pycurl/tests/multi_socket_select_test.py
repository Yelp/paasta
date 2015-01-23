#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et

import pycurl
import unittest
import select

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

class MultiSocketSelectTest(unittest.TestCase):
    def test_multi_socket_select(self):
        sockets = set()
        timeout = 0

        urls = [
            'http://localhost:8380/success',
            'http://localhost:8381/success',
            'http://localhost:8382/success',
        ]

        socket_events = []
        
        # socket callback
        def socket(event, socket, multi, data):
            if event == pycurl.POLL_REMOVE:
                #print("Remove Socket %d"%socket)
                sockets.remove(socket)
            else:
                if socket not in sockets:
                    #print("Add socket %d"%socket)
                    sockets.add(socket)
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

        while (pycurl.E_CALL_MULTI_PERFORM==m.socket_all()[0]):
            pass
            
        timeout = m.timeout()

        # timeout might be -1, indicating that all work is done
        # XXX make sure there is always work to be done here?
        while timeout >= 0:
            (rr, wr, er) = select.select(sockets,sockets,sockets,timeout/1000.0)
            socketSet = set(rr+wr+er)
            if socketSet:
                for s in socketSet:
                    while True:
                        (ret,running) = m.socket_action(s,0)
                        if ret!=pycurl.E_CALL_MULTI_PERFORM:
                            break
            else:
                (ret,running) = m.socket_action(pycurl.SOCKET_TIMEOUT,0)
            if running==0:
                break

        for c in m.handles:
            # save info in standard Python attributes
            c.http_code = c.getinfo(c.HTTP_CODE)

        # at least in and remove events per socket
        assert len(socket_events) >= 6, 'Less than 6 socket events: %s' % repr(socket_events)

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
