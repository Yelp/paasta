#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et
# $Id$

import sys
import pycurl

class Test:
    def __init__(self):
        self.contents = ''

    def body_callback(self, buf):
        self.contents = self.contents + buf

sys.stderr.write("Testing %s\n" % pycurl.version)

t = Test()
c = pycurl.Curl()
c.setopt(c.URL, 'http://curl.haxx.se/dev/')
c.setopt(c.WRITEFUNCTION, t.body_callback)
c.perform()
c.close()

print(t.contents)
