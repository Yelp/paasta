#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et

import pycurl
import unittest
try:
    import urllib.parse as urllib_parse
except ImportError:
    import urllib as urllib_parse

from . import appmanager
from . import util

setup_module, teardown_module = appmanager.setup(('app', 8380))

class ResetTest(unittest.TestCase):
    # XXX this test was broken when it was test_reset.py
    def skip_reset(self):
        outf = util.StringIO()
        cm = pycurl.CurlMulti()

        # Set multi handle's options
        cm.setopt(pycurl.M_PIPELINING, 1)

        eh = pycurl.Curl()

        for x in range(1, 20):
            eh.setopt(pycurl.WRITEFUNCTION, outf.write)
            eh.setopt(pycurl.URL, 'http://localhost:8380/success')
            cm.add_handle(eh)

            while 1:
                ret, active_handles = cm.perform()
                if ret != pycurl.E_CALL_MULTI_PERFORM:
                    break

            while active_handles:
                ret = cm.select(1.0)
                if ret == -1:
                    continue
                while 1:
                    ret, active_handles = cm.perform()
                    if ret != pycurl.E_CALL_MULTI_PERFORM:
                        break

            count, good, bad = cm.info_read()

            for h, en, em in bad:
                print("Transfer to %s failed with %d, %s\n" % \
                    (h.getinfo(pycurl.EFFECTIVE_URL), en, em))
                raise RuntimeError

            for h in good:
                httpcode = h.getinfo(pycurl.RESPONSE_CODE)
                if httpcode != 200:
                    print("Transfer to %s failed with code %d\n" %\
                        (h.getinfo(pycurl.EFFECTIVE_URL), httpcode))
                    raise RuntimeError

                else:
                    print("Recd %d bytes from %s" % \
                        (h.getinfo(pycurl.SIZE_DOWNLOAD),
                        h.getinfo(pycurl.EFFECTIVE_URL)))

            cm.remove_handle(eh)
            eh.reset()

        eh.close()
        cm.close()
        outf.close()
