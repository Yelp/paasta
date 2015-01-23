#!/usr/bin/env python
#coding=utf-8

#
# Copyright (C) 2014 - S. Zachariah Sprackett <zac@sprackett.com>
#
# Released under the same terms as Sensu (the MIT license); see LICENSE
# for details.

from __future__ import print_function
import json
import time
from sensu_plugin.plugin import SensuPlugin


class SensuPluginMetricJSON(SensuPlugin):
    def output(self, m):
        obj = m[0]
        if type(obj) is str or type(obj) is Exception:
            print(obj)
        elif type(obj) is dict or type(obj) is list:
            print(json.dumps(obj))


class SensuPluginMetricGraphite(SensuPlugin):
    def output(self, *m):
        if m[0] is None:
            print()
        elif type(m[0]) is Exception or m[1] is None:
            print(m[0])
        else:
            l_args = list(m)
            if len(l_args) < 3:
                l_args.append(None)
            if l_args[2] is None:
                l_args[2] = int(time.time())
            print("\t".join(str(s) for s in l_args[0:3]))


class SensuPluginMetricStatsd(SensuPlugin):
    def output(self, *m):
        if m[0] is None:
            print()
        elif type(m[0]) is Exception or m[1] is None:
            print(m[0])
        else:
            l_args = list(m)
            if len(l_args) < 3 or l_args[2] is None:
                stype = 'kv'
            else:
                stype = l_args[2]
            print("|".join([":".join(str(s) for s in l_args[0:2]), stype]))
