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
import datetime
import os
import re

import a_sync

from . import exceptions
from . import framework
from . import mesos_file
from paasta_tools.async_utils import async_ttl_cache


class Task:

    cmd_re = re.compile("\(Command: (.+)\)")

    def __init__(self, master, items):
        self.master = master
        self.__items = items

    def __str__(self):
        return "{}:{}".format(a_sync.block(self.slave), self["id"])

    def __getitem__(self, name):
        return self.__items[name]

    async def executor(self):
        return await (await self.slave()).task_executor(self["id"])

    async def framework(self):
        return framework.Framework(await self.master.framework(self["framework_id"]))

    @async_ttl_cache()
    async def directory(self):
        try:
            return (await self.executor())["directory"]
        except exceptions.MissingExecutor:
            return ""

    @async_ttl_cache()
    async def slave(self):
        return await self.master.slave(self["slave_id"])

    async def file(self, path):
        return mesos_file.File(await self.slave(), self, path)

    async def file_list(self, path):
        return await (await self.slave()).file_list(os.path.join(self.directory, path))

    async def stats(self):
        try:
            return await (await self.slave()).task_stats(self["id"])
        except exceptions.MissingExecutor:
            return {}

    async def cpu_time(self):
        st = await self.stats()
        secs = st.get("cpus_user_time_secs", 0) + \
            st.get("cpus_system_time_secs", 0)
        # timedelta has a resolution of .000000 while mesos only keeps .00
        return str(datetime.timedelta(seconds=secs)).rsplit(".", 1)[0]

    async def cpu_limit(self):
        return (await self.stats()).get("cpus_limit", 0)

    async def mem_limit(self):
        return (await self.stats()).get("mem_limit_bytes", 0)

    async def rss(self):
        return (await self.stats()).get("mem_rss_bytes", 0)

    async def command(self):
        try:
            result = self.cmd_re.search((await self.executor())["name"])
        except exceptions.MissingExecutor:
            result = None
        if not result:
            return "none"
        return result.group(1)

    async def user(self):
        return (await self.framework()).user
