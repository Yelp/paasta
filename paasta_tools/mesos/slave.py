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
from urllib.parse import urljoin

import aiohttp

from . import exceptions
from . import mesos_file
from . import util
from paasta_tools.async_utils import async_ttl_cache
from paasta_tools.utils import get_user_agent


class MesosSlave:

    def __init__(self, config, items):
        self.config = config
        self.__items = items

    def __getitem__(self, name):
        return self.__items[name]

    def __str__(self):
        return self.key()

    def key(self):
        return self["pid"].split('@')[-1]

    @property
    def host(self):
        return "{}://{}:{}".format(
            self.config["scheme"],
            self["hostname"],
            self["pid"].split(":")[-1],
        )

    async def fetch(self, url, **kwargs) -> aiohttp.ClientResponse:
        headers = {'User-Agent': get_user_agent()}
        async with aiohttp.ClientSession(
            conn_timeout=self.config["response_timeout"],
            read_timeout=self.config["response_timeout"],
        ) as session:
            try:
                async with session.get(
                    urljoin(self.host, url),
                    headers=headers,
                    **kwargs,
                ) as response:
                    await response.text()
                    return response
            except aiohttp.ClientConnectionError:
                raise exceptions.SlaveDoesNotExist(
                    f"Unable to connect to the slave at {self.host}",
                )

    @async_ttl_cache(ttl=5, cleanup_self=True)
    async def state(self):
        return await (await self.fetch("/slave(1)/state.json")).json()

    async def frameworks(self):
        return util.merge(await self.state(), "frameworks", "completed_frameworks")

    async def task_executor(self, task_id):
        for fw in await self.frameworks():
            for exc in util.merge(fw, "executors", "completed_executors"):
                if task_id in list(map(
                        lambda x: x["id"],
                        util.merge(
                            exc, "completed_tasks", "tasks", "queued_tasks",
                        ),
                )):
                    return exc
        raise exceptions.MissingExecutor("No executor has a task by that id")

    async def file_list(self, path):
        # The sandbox does not exist on the slave.
        if path == "":
            return []

        resp = self.fetch("/files/browse.json", params={"path": path})
        if resp.status_code == 404:
            return []
        return await resp.json()

    def file(self, task, path):
        return mesos_file.File(self, task, path)

    @async_ttl_cache(ttl=30, cleanup_self=True)
    async def stats(self):
        return await (await self.fetch("/monitor/statistics.json")).json()

    def executor_stats(self, _id):
        return list(filter(lambda x: x["executor_id"]))

    async def task_stats(self, _id):
        stats = list(filter(
            lambda x: x["executor_id"] == _id,
            await self.stats(),
        ))

        # Tasks that are not yet in a RUNNING state have no stats.
        if len(stats) == 0:
            return {}
        else:
            return stats[0]["statistics"]

    @property  # type: ignore
    @util.memoize
    def log(self):
        return mesos_file.File(self, path="/slave/log")
