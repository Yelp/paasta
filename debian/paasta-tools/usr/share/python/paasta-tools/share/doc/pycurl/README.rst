PycURL: Python interface to libcurl
====================================

PycURL is a Python interface to `libcurl`_. PycURL can be used to fetch objects
identified by a URL from a Python program, similar to the `urllib`_ Python module.
PycURL is mature, very fast, and supports a lot of features.

Overview
--------

- libcurl is a free and easy-to-use client-side URL transfer library, supporting
  FTP, FTPS, HTTP, HTTPS, SCP, SFTP, TFTP, TELNET, DICT, LDAP, LDAPS, FILE, IMAP,
  SMTP, POP3 and RTSP. libcurl supports SSL certificates, HTTP POST, HTTP PUT,
  FTP uploading, HTTP form based upload, proxies, cookies, user+password
  authentication  (Basic, Digest, NTLM, Negotiate, Kerberos4), file transfer
  resume, http proxy tunneling and more!

- libcurl is highly portable, it builds and works identically on numerous
  platforms, including Solaris, NetBSD, FreeBSD, OpenBSD, Darwin, HPUX, IRIX,
  AIX, Tru64, Linux, UnixWare, HURD, Windows, Amiga, OS/2, BeOs, Mac OS X,
  Ultrix, QNX, OpenVMS, RISC OS, Novell NetWare, DOS and more...

- libcurl is `free`_, `thread-safe`_, `IPv6 compatible`_, `feature rich`_,
  `well supported`_, `fast`_, `thoroughly documented`_ and is already used by
  many known, big and successful `companies`_ and numerous `applications`_.

.. _free: http://curl.haxx.se/docs/copyright.html
.. _thread-safe: http://curl.haxx.se/libcurl/features.html#thread
.. _`IPv6 compatible`: http://curl.haxx.se/libcurl/features.html#ipv6
.. _`feature rich`: http://curl.haxx.se/libcurl/features.html#features
.. _`well supported`: http://curl.haxx.se/libcurl/features.html#support
.. _`fast`: http://curl.haxx.se/libcurl/features.html#fast
.. _`thoroughly documented`: http://curl.haxx.se/libcurl/features.html#docs
.. _companies: http://curl.haxx.se/docs/companies.html
.. _applications: http://curl.haxx.se/libcurl/using/apps.html

Installation
------------

You can install the most recent PycURL version using `easy_install`_::

    easy_install pycurl

or `pip`_::

    pip install pycurl


.. _easy_install: http://peak.telecommunity.com/DevCenter/EasyInstall
.. _pip: http://pypi.python.org/pypi/pip

Automated Tests
---------------

PycURL comes with an automated test suite. To run the tests, execute::

    make test

The suite depends on packages `nose`_, `bottle`_ and `cherrypy`_.

Some tests use vsftpd configured to accept anonymous uploads. These tests
are not run by default. As configured, vsftpd will allow reads and writes to
anything the user running the tests has read and write access. To run
vsftpd tests you must explicitly set PYCURL_VSFTPD_PATH variable like so::

    # use vsftpd in PATH
    export PYCURL_VSFTPD_PATH=vsftpd

    # specify full path to vsftpd
    export PYCURL_VSFTPD_PATH=/usr/local/libexec/vsftpd

.. _nose: https://nose.readthedocs.org/
.. _bottle: http://bottlepy.org/
.. _cherrypy: http://www.cherrypy.org/

Contribute
----------

For smaller changes:

#. Fork `the repository`_ on Github.
#. Create a branch off **master**.
#. Make your changes.
#. Write a test which shows that the bug was fixed or that the feature
   works as expected.
#. Send a pull request.

For larger changes:

#. Join the `mailing list`_.
#. Discuss your proposal on the mailing list.
#. When consensus is reached, implement it as described above.

.. image:: https://api.travis-ci.org/pycurl/pycurl.png
	   :target: https://travis-ci.org/pycurl/pycurl

License
-------

::

    Copyright (C) 2001-2008 by Kjetil Jacobsen <kjetilja at gmail.com>
    Copyright (C) 2001-2008 by Markus F.X.J. Oberhumer <markus at oberhumer.com>

    All rights reserved.

    PycURL is dual licensed under the LGPL and an MIT/X derivative license
    based on the cURL license.  A full copy of the LGPL license is included
    in the file COPYING.  A full copy of the MIT/X derivative license is
    included in the file COPYING2.  You can redistribute and/or modify PycURL
    according to the terms of either license.

.. _PycURL: http://pycurl.sourceforge.net/
.. _libcurl: http://curl.haxx.se/libcurl/
.. _urllib: http://docs.python.org/library/urllib.html
.. _`the repository`: https://github.com/pycurl/pycurl
.. _`mailing list`: http://cool.haxx.se/mailman/listinfo/curl-and-python
