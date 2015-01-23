#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et

import pycurl
import unittest
from .util import StringIO
try:
    import cPickle
except ImportError:
    cPickle = None
import pickle
import gc
import copy

class InternalsTest(unittest.TestCase):
    def setUp(self):
        self.curl = pycurl.Curl()
    
    def tearDown(self):
        self.curl.close()
        del self.curl
    
    # /***********************************************************************
    # // test misc
    # ************************************************************************/
    
    def test_constant_aliasing(self):
        assert self.curl.URL is pycurl.URL
    
    # /***********************************************************************
    # // test handles
    # ************************************************************************/

    def test_remove_invalid_handle(self):
        m = pycurl.CurlMulti()
        try:
            m.remove_handle(self.curl)
        except pycurl.error:
            pass
        else:
            assert False, "No exception when trying to remove a handle that is not in CurlMulti"
        del m
    
    def test_remove_invalid_closed_handle(self):
        m = pycurl.CurlMulti()
        c = pycurl.Curl()
        c.close()
        m.remove_handle(c)
        del m, c
    
    def test_add_closed_handle(self):
        m = pycurl.CurlMulti()
        c = pycurl.Curl()
        c.close()
        try:
            m.add_handle(c)
        except pycurl.error:
            pass
        else:
            assert 0, "No exception when trying to add a close handle to CurlMulti"
        m.close()
        del m, c
    
    def test_add_handle_twice(self):
        m = pycurl.CurlMulti()
        m.add_handle(self.curl)
        try:
            m.add_handle(self.curl)
        except pycurl.error:
            pass
        else:
            assert 0, "No exception when trying to add the same handle twice"
        del m
    
    def test_add_handle_on_multiple_stacks(self):
        m1 = pycurl.CurlMulti()
        m2 = pycurl.CurlMulti()
        m1.add_handle(self.curl)
        try:
            m2.add_handle(self.curl)
        except pycurl.error:
            pass
        else:
            assert 0, "No exception when trying to add the same handle on multiple stacks"
        del m1, m2
    
    def test_move_handle(self):
        m1 = pycurl.CurlMulti()
        m2 = pycurl.CurlMulti()
        m1.add_handle(self.curl)
        m1.remove_handle(self.curl)
        m2.add_handle(self.curl)
        del m1, m2
    
    # /***********************************************************************
    # // test copying and pickling - copying and pickling of
    # // instances of Curl and CurlMulti is not allowed
    # ************************************************************************/

    def test_copy_curl(self):
        try:
            copy.copy(self.curl)
        # python 2 raises copy.Error, python 3 raises TypeError
        except (copy.Error, TypeError):
            pass
        else:
            assert False, "No exception when trying to copy a Curl handle"
    
    def test_copy_multi(self):
        m = pycurl.CurlMulti()
        try:
            copy.copy(m)
        except (copy.Error, TypeError):
            pass
        else:
            assert False, "No exception when trying to copy a CurlMulti handle"
    
    def test_pickle_curl(self):
        fp = StringIO()
        p = pickle.Pickler(fp, 1)
        try:
            p.dump(self.curl)
        # python 2 raises pickle.PicklingError, python 3 raises TypeError
        except (pickle.PicklingError, TypeError):
            pass
        else:
            assert 0, "No exception when trying to pickle a Curl handle"
        del fp, p
    
    def test_pickle_multi(self):
        m = pycurl.CurlMulti()
        fp = StringIO()
        p = pickle.Pickler(fp, 1)
        try:
            p.dump(m)
        except (pickle.PicklingError, TypeError):
            pass
        else:
            assert 0, "No exception when trying to pickle a CurlMulti handle"
        del m, fp, p
    
    if cPickle is not None:
        def test_cpickle_curl(self):
            fp = StringIO()
            p = cPickle.Pickler(fp, 1)
            try:
                p.dump(self.curl)
            except cPickle.PicklingError:
                pass
            else:
                assert 0, "No exception when trying to pickle a Curl handle via cPickle"
            del fp, p
        
        def test_cpickle_multi(self):
            m = pycurl.CurlMulti()
            fp = StringIO()
            p = cPickle.Pickler(fp, 1)
            try:
                p.dump(m)
            except cPickle.PicklingError:
                pass
            else:
                assert 0, "No exception when trying to pickle a CurlMulti handle via cPickle"
            del m, fp, p

    # /***********************************************************************
    # // test refcounts
    # ************************************************************************/

    # basic check of reference counting (use a memory checker like valgrind)
    def test_reference_counting(self):
        c = pycurl.Curl()
        m = pycurl.CurlMulti()
        m.add_handle(c)
        del m
        m = pycurl.CurlMulti()
        c.close()
        del m, c
    
    def test_cyclic_gc(self):
        gc.collect()
        c = pycurl.Curl()
        c.m = pycurl.CurlMulti()
        c.m.add_handle(c)
        # create some nasty cyclic references
        c.c = c
        c.c.c1 = c
        c.c.c2 = c
        c.c.c3 = c.c
        c.c.c4 = c.m
        c.m.c = c
        c.m.m = c.m
        c.m.c = c
        # delete
        gc.collect()
        flags = gc.DEBUG_COLLECTABLE | gc.DEBUG_UNCOLLECTABLE
        # python 3 has no DEBUG_OBJECTS
        #if hasattr(gc, 'DEBUG_OBJECTS'):
            #flags |= gc.DEBUG_OBJECTS
        #if opts.verbose >= 1:
            #flags = flags | gc.DEBUG_STATS
        gc.set_debug(flags)
        gc.collect()
        ##print gc.get_referrers(c)
        ##print gc.get_objects()
        #if opts.verbose >= 1:
            #print("Tracked objects:", len(gc.get_objects()))
        c_id = id(c)
        # The `del' below should delete these 4 objects:
        #   Curl + internal dict, CurlMulti + internal dict
        del c
        gc.collect()
        objects = gc.get_objects()
        for object in objects:
            assert id(object) != c_id
        #if opts.verbose >= 1:
            #print("Tracked objects:", len(gc.get_objects()))
    
    def test_refcounting_bug_in_reset(self):
        try:
            range_generator = xrange
        except NameError:
            range_generator = range
        # Ensure that the refcounting error in "reset" is fixed:
        for i in range_generator(100000):
            c = pycurl.Curl()
            c.reset()
