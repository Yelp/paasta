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
from __future__ import unicode_literals

import fnmatch
import itertools
import json
import logging
import os
import re

import requests
import requests.exceptions
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.retry import KazooRetry
from retry import retry
from six.moves.urllib_parse import urljoin

from . import exceptions
from . import framework
from . import log
from . import mesos_file
from . import slave
from . import task
from . import util
from . import zookeeper

ZOOKEEPER_TIMEOUT = 1

INVALID_PATH = "{0} does not have a valid path. Did you forget /mesos?"

MISSING_MASTER = """unable to connect to a master at {0}.

Try running `mesos config master zk://localhost:2181/mesos`. See the README for
more examples."""

MULTIPLE_SLAVES = "There are multiple slaves with that id. Please choose one: "

logger = logging.getLogger(__name__)


class MesosMaster(object):

    def __init__(self, config):
        self.config = config

    def __str__(self):
        return "<master: {}>".format(self.key())

    def key(self):
        return self.config["master"]

    @util.CachedProperty(ttl=5)
    def host(self):
        return "{}://{}".format(self.config["scheme"], self.resolve(self.config["master"]))

    @log.duration
    def _request(self, url, method=requests.get, **kwargs):
        try:
            return method(
                urljoin(self.host, url),
                timeout=self.config["response_timeout"],
                **kwargs)
        except requests.exceptions.ConnectionError:
            raise exceptions.MasterNotAvailableException(MISSING_MASTER.format(self.host))

    def fetch(self, url, **kwargs):
        return self._request(url, **kwargs)

    def post(self, url, **kwargs):
        return self._request(url, method=requests.post, **kwargs)

    def _file_resolver(self, cfg):
        return self.resolve(open(cfg[6:], "r+").read().strip())

    @retry(KazooTimeoutError, tries=5, delay=0.5, logger=logger)
    def _zookeeper_resolver(self, cfg):
        hosts, path = cfg[5:].split("/", 1)
        path = "/" + path

        retry = KazooRetry(max_tries=10)
        with zookeeper.client(hosts=hosts, read_only=True, connection_retry=retry, command_retry=retry) as zk:
            def master_id(key):
                return int(key.split("_")[-1])

            def get_masters():
                return [x for x in zk.get_children(path)
                        if re.search("\d+", x)]

            leader = sorted(get_masters(), key=lambda x: master_id(x))

            if len(leader) == 0:
                raise exceptions.MasterNotAvailableException("cannot find any masters at {}".format(cfg,))
            data, stat = zk.get(os.path.join(path, leader[0]))

            if not data:
                exceptions.MasterNotAvailableException("Cannot retrieve valid MasterInfo data from ZooKeeper")

            try:
                parsed = json.loads(data)
                if parsed and "address" in parsed:
                    ip = parsed["address"].get("ip")
                    port = parsed["address"].get("port")
                    if ip and port:
                        return "{ip}:{port}".format(ip=ip, port=port)
            except ValueError as parse_error:
                log.debug("[WARN] No JSON content, probably connecting to older Mesos version. "
                          "Reason: {}".format(parse_error))
                raise exceptions.MasterNotAvailableException("Failed to parse mesos master ip from ZK")

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

    @util.CachedProperty(ttl=15)
    def state(self):
        return self.fetch("/master/state.json").json()

    def state_summary(self):
        return self.fetch("/master/state-summary").json()

    @util.memoize
    def slave(self, fltr):
        lst = self.slaves(fltr)

        log.debug("master.slave({})".format(fltr))

        if len(lst) == 0:
            raise exceptions.SlaveDoesNotExist(
                "Slave {} no longer exists.".format(fltr))

        elif len(lst) > 1:
            raise exceptions.MultipleSlavesForIDError(
                "Multiple slaves matching filter %s. %s" % (
                    fltr,
                    ",".join([slave.id for slave in lst])
                )
            )

        return lst[0]

    def slaves(self, fltr=""):
        return [
            slave.MesosSlave(self.config, x)
            for x in self.state['slaves']
            if fltr == x['id']
        ]

    def _task_list(self, active_only=False):
        keys = ["tasks"]
        if not active_only:
            keys.append("completed_tasks")
        return itertools.chain(
            *[util.merge(x, *keys) for x in self._framework_list(active_only)])

    def task(self, fltr):
        lst = self.tasks(fltr)

        if len(lst) == 0:
            raise exceptions.TaskNotFoundException("Cannot find a task with filter %s" % fltr)

        elif len(lst) > 1:
            raise exceptions.MultipleTasksForIDError(
                "Multiple tasks matching filter %s. %s" % (
                    fltr,
                    ",".join([task.id for task in lst])
                )
            )
        return lst[0]

    def orphan_tasks(self):
        return self.state["orphan_tasks"]

    # XXX - need to filter on task state as well as id
    def tasks(self, fltr="", active_only=False):
        return [
            task.Task(self, x)
            for x in self._task_list(active_only)
            if fltr in x['id'] or fnmatch.fnmatch(x['id'], fltr)
        ]

    def framework(self, fwid):
        return list(filter(
            lambda x: x.id == fwid,
            self.frameworks()))[0]

    def _framework_list(self, active_only=False):
        keys = ["frameworks"]
        if not active_only:
            keys.append("completed_frameworks")
        return util.merge(self._frameworks, *keys)

    @util.CachedProperty(ttl=15)
    def _frameworks(self):
        return self.fetch("/master/frameworks").json()

    def frameworks(self, active_only=False):
        return [framework.Framework(f) for f in self._framework_list(active_only)]

    def teardown(self, framework_id):
        return self.post("/master/teardown", data="frameworkId=%s" % framework_id)

    def metrics_snapshot(self):
        return self.fetch("/metrics/snapshot").json()

    @property
    @util.memoize
    def log(self):
        return mesos_file.File(self, path="/master/log")
