# -*- coding: iso-8859-1 -*-
# vi:ts=4:et
# $Id$

import os, sys, socket
import time as _time
import pycurl

try:
    from cStringIO import StringIO
except ImportError:
    try:
        from StringIO import StringIO
    except ImportError:
        from io import StringIO

def version_less_than_spec(version_tuple, spec_tuple):
    # spec_tuple may have 2 elements, expect version_tuple to have 3 elements
    assert len(version_tuple) >= len(spec_tuple)
    for i in range(len(spec_tuple)):
        if version_tuple[i] < spec_tuple[i]:
            return True
        if version_tuple[i] > spec_tuple[i]:
            return False
    return False

def pycurl_version_less_than(*spec):
    version = [int(part) for part in pycurl.version_info()[1].split('.')]
    return version_less_than_spec(version, spec)

try:
    create_connection = socket.create_connection
except AttributeError:
    # python 2.5
    def create_connection(netloc, timeout=None):
        # XXX ipv4 only
        s = socket.socket()
        if timeout is not None:
            s.settimeout(timeout)
        s.connect(netloc)
        return s

def wait_for_network_service(netloc, check_interval, num_attempts):
    ok = False
    for i in range(num_attempts):
        try:
            conn = create_connection(netloc, check_interval)
        except socket.error:
            e = sys.exc_info()[1]
            _time.sleep(check_interval)
        else:
            conn.close()
            ok = True
            break
    return ok

#
# prepare sys.path in case we are still in the build directory
# see also: distutils/command/build.py (build_platlib)
#

def get_sys_path(p=None):
    if p is None:
        p = sys.path
    p = p[:]
    try:
        from distutils.util import get_platform
    except ImportError:
        return p
    p0 = ""
    if p:
        p0 = p[0]
    #
    plat = get_platform()
    plat_specifier = "%s-%s" % (plat, sys.version[:3])
    ##print plat, plat_specifier
    #
    for prefix in (p0, os.curdir, os.pardir,):
        if not prefix:
            continue
        d = os.path.join(prefix, "build")
        for subdir in ("lib", "lib." + plat_specifier, "lib." + plat):
            dir = os.path.normpath(os.path.join(d, subdir))
            if os.path.isdir(dir):
                if dir not in p:
                    p.insert(1, dir)
    #
    return p


