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
    def test_multi_timer(self):
        urls = [
            'http://localhost:8380/success',
            'http://localhost:8381/success',
            'http://localhost:8382/success',
        ]

        timers = []
        
        # timer callback
        def timer(msecs):
            #print('Timer callback msecs:', msecs)
            timers.append(msecs)

        # init
        m = pycurl.CurlMulti()
        m.setopt(pycurl.M_PIPELINING, 1)
        m.setopt(pycurl.M_TIMERFUNCTION, timer)
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
                ret, num_handles = m.perform()
                if ret != pycurl.E_CALL_MULTI_PERFORM:
                    break
                # currently no more I/O is pending, could do something in the meantime
                # (display a progress bar, etc.)
                m.select(1.0)

        for c in m.handles:
            # save info in standard Python attributes
            c.http_code = c.getinfo(c.HTTP_CODE)

        # print result
        for c in m.handles:
            self.assertEqual('success', c.body.getvalue())
            self.assertEqual(200, c.http_code)
        
        assert len(timers) > 0
        # libcurl 7.23.0 produces a 0 timer
        assert timers[0] >= 0
        # this assertion does not appear to hold on older libcurls
        # or apparently on any linuxes, see
        # https://github.com/p/pycurl/issues/19
        #if not util.pycurl_version_less_than(7, 24):
        #    self.assertEqual(-1, timers[-1])

        # close handles
        for c in m.handles:
            # pycurl API calls
            m.remove_handle(c)
            c.close()
        m.close()
