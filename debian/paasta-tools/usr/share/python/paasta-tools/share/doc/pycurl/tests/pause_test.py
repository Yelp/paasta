#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et

import pycurl
import unittest, signal
import time as _time

from . import appmanager
from . import util

setup_module, teardown_module = appmanager.setup(('app', 8380))

class PauseTest(unittest.TestCase):
    def setUp(self):
        self.curl = pycurl.Curl()
    
    def tearDown(self):
        self.curl.close()
    
    def test_pause_via_call(self):
        self.check_pause(True)
    
    def test_pause_via_return(self):
        self.check_pause(False)
    
    def check_pause(self, call):
        # the app sleeps for 0.5 seconds
        self.curl.setopt(pycurl.URL, 'http://localhost:8380/pause')
        sio = util.StringIO()
        state = dict(paused=False, resumed=False)
        if call:
            def writefunc(data):
                rv = sio.write(data)
                if not state['paused']:
                    self.curl.pause(pycurl.PAUSE_ALL)
                    state['paused'] = True
                return rv
        else:
            def writefunc(data):
                if not state['paused']:
                    # cannot write to sio here, because
                    # curl takes pause return value to mean that
                    # nothing was written
                    state['paused'] = True
                    return pycurl.READFUNC_PAUSE
                else:
                    return sio.write(data)
        def resume(*args):
            state['resumed'] = True
            self.curl.pause(pycurl.PAUSE_CONT)
        signal.signal(signal.SIGALRM, resume)
        # alarm for 1 second which is 0.5 seconds more than the server side
        # should sleep for
        signal.alarm(1)
        start = _time.time()
        self.curl.setopt(pycurl.WRITEFUNCTION, writefunc)
        
        m = pycurl.CurlMulti()
        m.add_handle(self.curl)

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
                if _time.time() - start > 2:
                    # test is taking too long, fail
                    assert False, 'Test is taking too long'
                ret, num_handles = m.perform()
                if ret != pycurl.E_CALL_MULTI_PERFORM:
                    break
        
        # Cleanup
        m.remove_handle(self.curl)
        m.close()
        
        self.assertEqual('part1part2', sio.getvalue())
        end = _time.time()
        # check that client side waited
        self.assertTrue(end-start > 1)
        
        assert state['resumed']
