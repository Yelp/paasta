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

class MultiTest(unittest.TestCase):
    def test_multi(self):
        io1 = util.StringIO()
        io2 = util.StringIO()
        m = pycurl.CurlMulti()
        m.handles = []
        c1 = pycurl.Curl()
        c2 = pycurl.Curl()
        c1.setopt(c1.URL, 'http://localhost:8380/success')
        c1.setopt(c1.WRITEFUNCTION, io1.write)
        c2.setopt(c2.URL, 'http://localhost:8381/success')
        c2.setopt(c1.WRITEFUNCTION, io2.write)
        m.add_handle(c1)
        m.add_handle(c2)
        m.handles.append(c1)
        m.handles.append(c2)

        num_handles = len(m.handles)
        while num_handles:
            while 1:
                ret, num_handles = m.perform()
                if ret != pycurl.E_CALL_MULTI_PERFORM:
                    break
            m.select(1.0)

        m.remove_handle(c2)
        m.remove_handle(c1)
        del m.handles
        m.close()
        c1.close()
        c2.close()
        
        self.assertEqual('success', io1.getvalue())
        self.assertEqual('success', io2.getvalue())
    
    def test_multi_select_fdset(self):
        c1 = pycurl.Curl()
        c2 = pycurl.Curl()
        c3 = pycurl.Curl()
        c1.setopt(c1.URL, "http://localhost:8380/success")
        c2.setopt(c2.URL, "http://localhost:8381/success")
        c3.setopt(c3.URL, "http://localhost:8382/success")
        c1.body = util.StringIO()
        c2.body = util.StringIO()
        c3.body = util.StringIO()
        c1.setopt(c1.WRITEFUNCTION, c1.body.write)
        c2.setopt(c2.WRITEFUNCTION, c2.body.write)
        c3.setopt(c3.WRITEFUNCTION, c3.body.write)

        m = pycurl.CurlMulti()
        m.add_handle(c1)
        m.add_handle(c2)
        m.add_handle(c3)

        # Number of seconds to wait for a timeout to happen
        SELECT_TIMEOUT = 0.1

        # Stir the state machine into action
        while 1:
            ret, num_handles = m.perform()
            if ret != pycurl.E_CALL_MULTI_PERFORM:
                break

        # Keep going until all the connections have terminated
        while num_handles:
            select.select(*m.fdset() + (SELECT_TIMEOUT,))
            while 1:
                ret, num_handles = m.perform()
                if ret != pycurl.E_CALL_MULTI_PERFORM:
                    break

        # Cleanup
        m.remove_handle(c3)
        m.remove_handle(c2)
        m.remove_handle(c1)
        m.close()
        c1.close()
        c2.close()
        c3.close()
        
        self.assertEqual('success', c1.body.getvalue())
        self.assertEqual('success', c2.body.getvalue())
        self.assertEqual('success', c3.body.getvalue())
    
    def test_multi_status_codes(self):
        # init
        m = pycurl.CurlMulti()
        m.handles = []
        urls = [
            'http://localhost:8380/success',
            'http://localhost:8381/status/403',
            'http://localhost:8382/status/404',
        ]
        for url in urls:
            c = pycurl.Curl()
            # save info in standard Python attributes
            c.url = url.rstrip()
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
            m.select(0.1)

        # close handles
        for c in m.handles:
            # save info in standard Python attributes
            c.http_code = c.getinfo(c.HTTP_CODE)
            # pycurl API calls
            m.remove_handle(c)
            c.close()
        m.close()

        # check result
        self.assertEqual('success', m.handles[0].body.getvalue())
        self.assertEqual(200, m.handles[0].http_code)
        # bottle generated response body
        self.assertEqual('forbidden', m.handles[1].body.getvalue())
        self.assertEqual(403, m.handles[1].http_code)
        # bottle generated response body
        self.assertEqual('not found', m.handles[2].body.getvalue())
        self.assertEqual(404, m.handles[2].http_code)
    
    def check_adding_closed_handle(self, close_fn):
        # init
        m = pycurl.CurlMulti()
        m.handles = []
        urls = [
            'http://localhost:8380/success',
            'http://localhost:8381/status/403',
            'http://localhost:8382/status/404',
        ]
        for url in urls:
            c = pycurl.Curl()
            # save info in standard Python attributes
            c.url = url
            c.body = util.StringIO()
            c.http_code = -1
            c.debug = 0
            m.handles.append(c)
            # pycurl API calls
            c.setopt(c.URL, c.url)
            c.setopt(c.WRITEFUNCTION, c.body.write)
            m.add_handle(c)

        # debug - close a handle
        c = m.handles[2]
        c.debug = 1
        c.close()

        # get data
        num_handles = len(m.handles)
        while num_handles:
            while 1:
                ret, num_handles = m.perform()
                if ret != pycurl.E_CALL_MULTI_PERFORM:
                    break
            # currently no more I/O is pending, could do something in the meantime
            # (display a progress bar, etc.)
            m.select(0.1)

        # close handles
        for c in m.handles:
            # save info in standard Python attributes
            try:
                c.http_code = c.getinfo(c.HTTP_CODE)
            except pycurl.error:
                # handle already closed - see debug above
                assert c.debug
                c.http_code = -1
            # pycurl API calls
            close_fn(m, c)
        m.close()

        # check result
        self.assertEqual('success', m.handles[0].body.getvalue())
        self.assertEqual(200, m.handles[0].http_code)
        # bottle generated response body
        self.assertEqual('forbidden', m.handles[1].body.getvalue())
        self.assertEqual(403, m.handles[1].http_code)
        # bottle generated response body
        self.assertEqual('', m.handles[2].body.getvalue())
        self.assertEqual(-1, m.handles[2].http_code)
    
    def _remove_then_close(self, m, c):
        m.remove_handle(c)
        c.close()
    
    def _close_then_remove(self, m, c):
        # in the C API this is the wrong calling order, but pycurl
        # handles this automatically
        c.close()
        m.remove_handle(c)
    
    def _close_without_removing(self, m, c):
        # actually, remove_handle is called automatically on close
        c.close
    
    def test_adding_closed_handle_remove_then_close(self):
        self.check_adding_closed_handle(self._remove_then_close)
    
    def test_adding_closed_handle_close_then_remove(self):
        self.check_adding_closed_handle(self._close_then_remove)
    
    def test_adding_closed_handle_close_without_removing(self):
        self.check_adding_closed_handle(self._close_without_removing)
    
    def test_multi_select(self):
        c1 = pycurl.Curl()
        c2 = pycurl.Curl()
        c3 = pycurl.Curl()
        c1.setopt(c1.URL, "http://localhost:8380/success")
        c2.setopt(c2.URL, "http://localhost:8381/success")
        c3.setopt(c3.URL, "http://localhost:8382/success")
        c1.body = util.StringIO()
        c2.body = util.StringIO()
        c3.body = util.StringIO()
        c1.setopt(c1.WRITEFUNCTION, c1.body.write)
        c2.setopt(c2.WRITEFUNCTION, c2.body.write)
        c3.setopt(c3.WRITEFUNCTION, c3.body.write)

        m = pycurl.CurlMulti()
        m.add_handle(c1)
        m.add_handle(c2)
        m.add_handle(c3)

        # Number of seconds to wait for a timeout to happen
        SELECT_TIMEOUT = 1.0

        # Stir the state machine into action
        while 1:
            ret, num_handles = m.perform()
            if ret != pycurl.E_CALL_MULTI_PERFORM:
                break

        # Keep going until all the connections have terminated
        while num_handles:
            # The select method uses fdset internally to determine which file descriptors
            # to check.
            m.select(SELECT_TIMEOUT)
            while 1:
                ret, num_handles = m.perform()
                if ret != pycurl.E_CALL_MULTI_PERFORM:
                    break

        # Cleanup
        m.remove_handle(c3)
        m.remove_handle(c2)
        m.remove_handle(c1)
        m.close()
        c1.close()
        c2.close()
        c3.close()
        
        self.assertEqual('success', c1.body.getvalue())
        self.assertEqual('success', c2.body.getvalue())
        self.assertEqual('success', c3.body.getvalue())
    
    def test_multi_info_read(self):
        c1 = pycurl.Curl()
        c2 = pycurl.Curl()
        c3 = pycurl.Curl()
        c1.setopt(c1.URL, "http://localhost:8380/success")
        c2.setopt(c2.URL, "http://localhost:8381/success")
        c3.setopt(c3.URL, "http://localhost:8382/success")
        c1.body = util.StringIO()
        c2.body = util.StringIO()
        c3.body = util.StringIO()
        c1.setopt(c1.WRITEFUNCTION, c1.body.write)
        c2.setopt(c2.WRITEFUNCTION, c2.body.write)
        c3.setopt(c3.WRITEFUNCTION, c3.body.write)

        m = pycurl.CurlMulti()
        m.add_handle(c1)
        m.add_handle(c2)
        m.add_handle(c3)

        # Number of seconds to wait for a timeout to happen
        SELECT_TIMEOUT = 1.0

        # Stir the state machine into action
        while 1:
            ret, num_handles = m.perform()
            if ret != pycurl.E_CALL_MULTI_PERFORM:
                break

        infos = []
        # Keep going until all the connections have terminated
        while num_handles:
            # The select method uses fdset internally to determine which file descriptors
            # to check.
            m.select(SELECT_TIMEOUT)
            while 1:
                ret, num_handles = m.perform()
                info = m.info_read()
                infos.append(info)
                if ret != pycurl.E_CALL_MULTI_PERFORM:
                    break

        all_handles = []
        for info in infos:
            handles = info[1]
            # last info is an empty array
            if handles:
                all_handles.extend(handles)
        
        self.assertEqual(3, len(all_handles))
        assert c1 in all_handles
        assert c2 in all_handles
        assert c3 in all_handles
        
        # Cleanup
        m.remove_handle(c3)
        m.remove_handle(c2)
        m.remove_handle(c1)
        m.close()
        c1.close()
        c2.close()
        c3.close()
        
        self.assertEqual('success', c1.body.getvalue())
        self.assertEqual('success', c2.body.getvalue())
        self.assertEqual('success', c3.body.getvalue())
