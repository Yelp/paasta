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
import fnmatch
import itertools
import json
import logging
import os
import re
from typing import List
from urllib.parse import urljoin
from urllib.parse import urlparse

import aiohttp
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.retry import KazooRetry
from mypy_extensions import TypedDict
from retry import retry

from . import exceptions
from . import framework
from . import log
from . import mesos_file
from . import slave
from . import task
from . import util
from . import zookeeper
from paasta_tools.async_utils import async_ttl_cache
from paasta_tools.utils import get_user_agent

ZOOKEEPER_TIMEOUT = 1

INVALID_PATH = "{0} does not have a valid path. Did you forget /mesos?"

MISSING_MASTER = """unable to connect to a master at {0}.

Try running `mesos config master zk://localhost:2181/mesos`. See the README for
more examples."""

MULTIPLE_SLAVES = "There are multiple slaves with that id. Please choose one: "

logger = logging.getLogger(__name__)


class MesosState(TypedDict):
    slaves: List
    frameworks: List
    orphan_tasks: List


MesosMetrics = TypedDict(
    "MesosMetrics",
    {
        "master/cpus_total": int,
        "master/cpus_used": int,
        "master/disk_total": int,
        "master/disk_used": int,
        "master/gpus_total": int,
        "master/gpus_used": int,
        "master/mem_total": int,
        "master/mem_used": int,
        "master/tasks_running": int,
        "master/tasks_staging": int,
        "master/tasks_starting": int,
        "master/slaves_active": int,
        "master/slaves_inactive": int,
    },
)


class MesosMaster:
    def __init__(self, config):
        self.config = config

    def __str__(self):
        return "<master: {}>".format(self.key())

    def key(self):
        return self.config["master"]

    @util.CachedProperty(ttl=5)
    def host(self):
        return "{}://{}".format(
            self.config["scheme"], self.resolve(self.config["master"])
        )

    @util.CachedProperty(ttl=5)
    def cache_host(self):
        host_url = urlparse(self.host)
        replaced = host_url._replace(netloc=host_url.hostname + ":5055")
        return replaced.geturl()

    async def _request(
        self, url: str, method: str = "GET", cached: bool = False, **kwargs
    ) -> aiohttp.ClientResponse:
        headers = {"User-Agent": get_user_agent()}

        if cached and self.config.get("use_mesos_cache", False):
            # TODO: fall back to original host if this fails?
            host = self.cache_host
        else:
            host = self.host

        try:
            async with aiohttp.ClientSession(
                conn_timeout=self.config["response_timeout"],
                read_timeout=self.config["response_timeout"],
            ) as session:
                async with session.request(
                    method=method, url=urljoin(host, url), headers=headers, **kwargs
                ) as resp:
                    # if nobody awaits resp.text() or resp.json() before we exit the session context manager, then the
                    # http connection gets closed before we read the response; then later calls to resp.text/json will
                    # fail.
                    await resp.text()
                    return resp

        except aiohttp.client_exceptions.ClientConnectionError:
            raise exceptions.MasterNotAvailableException(MISSING_MASTER.format(host))
        except aiohttp.client_exceptions.TooManyRedirects:
            raise exceptions.MasterTemporarilyNotAvailableException(
                (
                    "Unable to connect to master at %s, likely due to "
                    "an ongoing leader election"
                )
                % host
            )

    async def fetch(self, url, **kwargs):
        return await self._request(url, **kwargs)

    async def post(self, url, **kwargs):
        return await self._request(url, method="POST", **kwargs)

    def _file_resolver(self, cfg):
        return self.resolve(open(cfg[6:], "r+").read().strip())

    @retry(KazooTimeoutError, tries=5, delay=0.5, logger=logger)
    def _zookeeper_resolver(self, cfg):
        hosts, path = cfg[5:].split("/", 1)
        path = "/" + path

        retry = KazooRetry(max_tries=10)
        with zookeeper.client(
            hosts=hosts, read_only=True, connection_retry=retry, command_retry=retry
        ) as zk:

            def master_id(key):
                return int(key.split("_")[-1])

            def get_masters():
                return [x for x in zk.get_children(path) if re.search(r"\d+", x)]

            leader = sorted(get_masters(), key=lambda x: master_id(x))

            if len(leader) == 0:
                raise exceptions.MasterNotAvailableException(
                    f"cannot find any masters at {cfg}"
                )
            data, stat = zk.get(os.path.join(path, leader[0]))

            if not data:
                exceptions.MasterNotAvailableException(
                    "Cannot retrieve valid MasterInfo data from ZooKeeper"
                )
            else:
                data = data.decode("utf8")

            try:
                parsed = json.loads(data)
                if parsed and "address" in parsed:
                    ip = parsed["address"].get("ip")
                    port = parsed["address"].get("port")
                    if ip and port:
                        return f"{ip}:{port}"
            except ValueError as parse_error:
                log.debug(
                    "[WARN] No JSON content, probably connecting to older "
                    "Mesos version. Reason: {}".format(parse_error)
                )
                raise exceptions.MasterNotAvailableException(
                    "Failed to parse mesos master ip from ZK"
                )

    @log.duration
    def resolve(self, cfg):
        """Resolve the URL to the mesos master.

        The value of cfg should be one of:
            - host:port
            - zk://host1:port1,host2:port2/path
            - zk://username:password@host1:port1/path
            - file:///path/to/file (where file contains one of the above)
        """
        if cfg.startswith("zk:"):
            return self._zookeeper_resolver(cfg)
        elif cfg.startswith("file:"):
            return self._file_resolver(cfg)
        else:
            return cfg

    @async_ttl_cache(ttl=15, cleanup_self=True)
    async def state(self) -> MesosState:
        return await (await self.fetch("/master/state.json", cached=True)).json()

    async def state_summary(self) -> MesosState:
        return await (await self.fetch("/master/state-summary")).json()

    @async_ttl_cache(ttl=None, cleanup_self=True)
    async def slave(self, fltr):
        lst = await self.slaves(fltr)

        log.debug(f"master.slave({fltr})")

        if len(lst) == 0:
            raise exceptions.SlaveDoesNotExist(f"Slave {fltr} no longer exists.")

        elif len(lst) > 1:
            raise exceptions.MultipleSlavesForIDError(
                "Multiple slaves matching filter {}. {}".format(
                    fltr, ",".join([slave.id for slave in lst])
                )
            )

        return lst[0]

    async def slaves(self, fltr=""):
        return [
            slave.MesosSlave(self.config, x)
            for x in (await self.state())["slaves"]
            if fltr == x["id"]
        ]

    async def _task_list(self, active_only=False):
        keys = ["tasks"]
        if not active_only:
            keys.append("completed_tasks")
        return itertools.chain(
            *[util.merge(x, *keys) for x in await self._framework_list(active_only)]
        )

    async def task(self, fltr):
        lst = await self.tasks(fltr)

        if len(lst) == 0:
            raise exceptions.TaskNotFoundException(
                "Cannot find a task with filter %s" % fltr
            )

        elif len(lst) > 1:
            raise exceptions.MultipleTasksForIDError(
                "Multiple tasks matching filter {}. {}".format(
                    fltr, ",".join([task.id for task in lst])
                )
            )
        return lst[0]

    async def orphan_tasks(self):
        return (await self.state())["orphan_tasks"]

    # XXX - need to filter on task state as well as id
    async def tasks(self, fltr="", active_only=False):
        return [
            task.Task(self, x)
            for x in await self._task_list(active_only)
            if fltr in x["id"] or fnmatch.fnmatch(x["id"], fltr)
        ]

    async def framework(self, fwid):
        return list(filter(lambda x: x.id == fwid, await self.frameworks()))[0]

    async def _framework_list(self, active_only=False):
        keys = ["frameworks"]
        if not active_only:
            keys.append("completed_frameworks")
        return util.merge(await self._frameworks(), *keys)

    @async_ttl_cache(ttl=15, cleanup_self=True)
    async def _frameworks(self):
        return await (await self.fetch("/master/frameworks", cached=True)).json()

    async def frameworks(self, active_only=False):
        return [framework.Framework(f) for f in await self._framework_list(active_only)]

    async def teardown(self, framework_id):
        return await self.post("/master/teardown", data="frameworkId=%s" % framework_id)

    async def metrics_snapshot(self) -> MesosMetrics:
        return await (await self.fetch("/metrics/snapshot")).json()

    @property  # type: ignore
    @util.memoize
    def log(self):
        return mesos_file.File(self, path="/master/log")
