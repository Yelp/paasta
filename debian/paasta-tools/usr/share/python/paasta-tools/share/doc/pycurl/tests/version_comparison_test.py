#! /usr/bin/env python
# -*- coding: iso-8859-1 -*-
# vi:ts=4:et

import unittest

from . import util

class VersionComparisonTest(unittest.TestCase):
    def test_comparison(self):
        assert util.version_less_than_spec((7, 22, 0), (7, 23, 0))
        assert util.version_less_than_spec((7, 22, 0), (7, 23))
        assert util.version_less_than_spec((7, 22, 0), (7, 22, 1))
        assert not util.version_less_than_spec((7, 22, 0), (7, 22, 0))
        assert not util.version_less_than_spec((7, 22, 0), (7, 22))
