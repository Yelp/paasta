# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import absolute_import
from __future__ import print_function

import datetime
import os
import re

from . import exceptions
from . import framework
from . import mesos_file
from . import util


class Task(object):

    cmd_re = re.compile("\(Command: (.+)\)")

    def __init__(self, master, items):
        self.master = master
        self.__items = items

    def __str__(self):
        return "{0}:{1}".format(self.slave, self["id"])

    def __getitem__(self, name):
        return self.__items[name]

    @property
    def executor(self):
        return self.slave.task_executor(self["id"])

    @property
    def framework(self):
        return framework.Framework(self.master.framework(self["framework_id"]))

    @util.CachedProperty()
    def directory(self):
        try:
            return self.executor["directory"]
        except exceptions.MissingExecutor:
            return ""

    @util.CachedProperty()
    def slave(self):
        return self.master.slave(self["slave_id"])

    def file(self, path):
        return mesos_file.File(self.slave, self, path)

    def file_list(self, path):
        return self.slave.file_list(os.path.join(self.directory, path))

    @property
    def stats(self):
        try:
            return self.slave.task_stats(self["id"])
        except exceptions.MissingExecutor:
            return {}

    @property
    def cpu_time(self):
        st = self.stats
        secs = st.get("cpus_user_time_secs", 0) + \
            st.get("cpus_system_time_secs", 0)
        # timedelta has a resolution of .000000 while mesos only keeps .00
        return str(datetime.timedelta(seconds=secs)).rsplit(".", 1)[0]

    @property
    def cpu_limit(self):
        return self.stats.get("cpus_limit", 0)

    @property
    def mem_limit(self):
        return self.stats.get("mem_limit_bytes", 0)

    @property
    def rss(self):
        return self.stats.get("mem_rss_bytes", 0)

    @property
    def command(self):
        try:
            result = self.cmd_re.search(self.executor["name"])
        except exceptions.MissingExecutor:
            result = None
        if not result:
            return "none"
        return result.group(1)

    @property
    def user(self):
        return self.framework.user
