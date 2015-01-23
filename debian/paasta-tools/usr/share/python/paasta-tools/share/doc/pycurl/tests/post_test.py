#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et

import os.path
import pycurl
import unittest
try:
    import json
except ImportError:
    import simplejson as json
try:
    import urllib.parse as urllib_parse
except ImportError:
    import urllib as urllib_parse

from . import appmanager
from . import util

setup_module, teardown_module = appmanager.setup(('app', 8380))

class PostTest(unittest.TestCase):
    def setUp(self):
        self.curl = pycurl.Curl()
    
    def tearDown(self):
        self.curl.close()
    
    def test_post_single_field(self):
        pf = {'field1': 'value1'}
        self.urlencode_and_check(pf)
    
    def test_post_multiple_fields(self):
        pf = {'field1':'value1', 'field2':'value2 with blanks', 'field3':'value3'}
        self.urlencode_and_check(pf)
    
    def test_post_fields_with_ampersand(self):
        pf = {'field1':'value1', 'field2':'value2 with blanks and & chars',
              'field3':'value3'}
        self.urlencode_and_check(pf)
    
    def urlencode_and_check(self, pf):
        self.curl.setopt(pycurl.URL, 'http://localhost:8380/postfields')
        postfields = urllib_parse.urlencode(pf)
        self.curl.setopt(pycurl.POSTFIELDS, postfields)
        
        # But directly passing urlencode result into setopt call:
        #self.curl.setopt(pycurl.POSTFIELDS, urllib_parse.urlencode(pf))
        # produces:
        # {'\x00\x00\x00\x00\x80\x00\x00\x00\x00\x00\x00\x00\x00': ''}
        # Traceback (most recent call last):
        #   File "/usr/local/bin/bottle.py", line 744, in _handle
        #     return route.call(**args)
        #   File "/usr/local/bin/bottle.py", line 1479, in wrapper
        #     rv = callback(*a, **ka)
        #   File "/home/pie/apps/pycurl/tests/app.py", line 21, in postfields
        #     return json.dumps(dict(bottle.request.forms))
        #   File "/usr/local/lib/python2.7/json/__init__.py", line 231, in dumps
        #     return _default_encoder.encode(obj)
        #   File "/usr/local/lib/python2.7/json/encoder.py", line 201, in encode
        #     chunks = self.iterencode(o, _one_shot=True)
        #   File "/usr/local/lib/python2.7/json/encoder.py", line 264, in iterencode
        #     return _iterencode(o, 0)
        # UnicodeDecodeError: 'utf8' codec can't decode byte 0x80 in position 4: invalid start byte
        
        #self.curl.setopt(pycurl.VERBOSE, 1)
        sio = util.StringIO()
        self.curl.setopt(pycurl.WRITEFUNCTION, sio.write)
        self.curl.perform()
        self.assertEqual(200, self.curl.getinfo(pycurl.HTTP_CODE))
        body = sio.getvalue()
        returned_fields = json.loads(body)
        self.assertEqual(pf, returned_fields)
    
    def test_post_with_null_byte(self):
        send = [
            ('field3', (pycurl.FORM_CONTENTS, 'this is wei\000rd, but null-bytes are okay'))
        ]
        expect = {
            'field3': 'this is wei\000rd, but null-bytes are okay',
        }
        self.check_post(send, expect, 'http://localhost:8380/postfields')
    
    def test_post_file(self):
        path = os.path.join(os.path.dirname(__file__), '..', 'README.rst')
        f = open(path)
        try:
            contents = f.read()
        finally:
            f.close()
        send = [
            #('field2', (pycurl.FORM_FILE, 'test_post.py', pycurl.FORM_FILE, 'test_post2.py')),
            ('field2', (pycurl.FORM_FILE, path)),
        ]
        expect = [{
            'name': 'field2',
            'filename': 'README.rst',
            'data': contents,
        }]
        self.check_post(send, expect, 'http://localhost:8380/files')
    
    # XXX this test takes about a second to run, check keep-alives?
    def check_post(self, send, expect, endpoint):
        self.curl.setopt(pycurl.URL, endpoint)
        self.curl.setopt(pycurl.HTTPPOST, send)
        #self.curl.setopt(pycurl.VERBOSE, 1)
        sio = util.StringIO()
        self.curl.setopt(pycurl.WRITEFUNCTION, sio.write)
        self.curl.perform()
        body = sio.getvalue()
        returned_fields = json.loads(body)
        self.assertEqual(expect, returned_fields)
