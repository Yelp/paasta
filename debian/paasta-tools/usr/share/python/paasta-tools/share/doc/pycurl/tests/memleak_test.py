#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et

import pycurl
import unittest
import gc

class MemleakTest(unittest.TestCase):
    def test_collection(self):
        gc.collect()
        flags = gc.DEBUG_COLLECTABLE | gc.DEBUG_UNCOLLECTABLE
        # python 3 has no DEBUG_OBJECTS
        #if hasattr(gc, 'DEBUG_OBJECTS'):
            #flags |= gc.DEBUG_OBJECTS
        #if 1:
            #flags = flags | gc.DEBUG_STATS
        #gc.set_debug(flags)
        gc.collect()

        #print("Tracked objects:", len(gc.get_objects()))

        multi = pycurl.CurlMulti()
        t = []
        searches = []
        for a in range(100):
            curl = pycurl.Curl()
            multi.add_handle(curl)
            t.append(curl)
            
            c_id = id(curl)
            searches.append(c_id)
        m_id = id(multi)
        searches.append(m_id)

        #print("Tracked objects:", len(gc.get_objects()))

        for curl in t:
            curl.close()
            multi.remove_handle(curl)

        #print("Tracked objects:", len(gc.get_objects()))

        del curl
        del t
        del multi

        #print("Tracked objects:", len(gc.get_objects()))
        gc.collect()
        #print("Tracked objects:", len(gc.get_objects()))
        
        objects = gc.get_objects()
        for search in searches:
            for object in objects:
                assert search != id(object)
