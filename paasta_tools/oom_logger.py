#!/usr/bin/env python
# Copyright 2015-2017 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
paasta_oom_logger is supposed to be used as a syslog-ng destination.
It looks for OOM events in the log, adds PaaSTA service and instance names
and send JSON-encoded messages the stream 'tmp_paasta_oom_events'.

syslog-ng.conf:

destination paasta_oom_logger {
  program("exec /usr/bin/paasta_oom_logger" template("${UNIXTIME} ${HOST} ${MESSAGE}\n") );
};

filter f_cgroup_oom {
  match(" killed as a result of limit of ") or match(" invoked oom-killer: ");
};

log {
  source(s_all);
  filter(f_cgroup_oom);
  destination(paasta_oom_logger);
};
"""
import re
import sys
from collections import namedtuple

from docker.errors import APIError

from paasta_tools.cli.utils import get_instance_config
from paasta_tools.utils import _log
from paasta_tools.utils import DEFAULT_LOGLEVEL
from paasta_tools.utils import get_docker_client
from paasta_tools.utils import load_system_paasta_config


# Sorry to any non-yelpers but this won't
# do much as our metrics and logging libs
# are not open source
try:
    import yelp_meteorite
except ImportError:
    yelp_meteorite = None

try:
    import clog
except ImportError:
    clog = None


LogLine = namedtuple(
    "LogLine",
    [
        "timestamp",
        "hostname",
        "container_id",
        "cluster",
        "service",
        "instance",
        "process_name",
        "mesos_container_id",
        "mem_limit",
    ],
)


def capture_oom_events_from_stdin():
    process_name_regex = re.compile(
        r"^\d+\s[a-zA-Z0-9\-]+\s.*\]\s(.+)\sinvoked\soom-killer:"
    )
    oom_regex_docker = re.compile(
        r"^(\d+)\s([a-zA-Z0-9\-]+)\s.*Task in /docker/(\w{12})\w+ killed as a"
    )
    oom_regex_kubernetes = re.compile(
        r"""
        ^(\d+)\s # timestamp
        ([a-zA-Z0-9\-]+) # hostname
        \s.*Task\sin\s/kubepods/(?:[a-zA-Z]+/)? # start of message; non capturing, optional group for the qos cgroup
        pod[-\w]+/(\w{12})\w+\s # containerid
        killed\sas\sa*  # eom
        """,
        re.VERBOSE,
    )
    oom_regex_kubernetes_structured = re.compile(
        r"""
        ^(\d+)\s # timestamp
        ([a-zA-Z0-9\-]+) # hostname
        \s.*oom-kill:.*task_memcg=/kubepods/(?:[a-zA-Z]+/)? # start of message; non-capturing, optional group for the qos cgroup
        pod[-\w]+/(\w{12})\w+,.*$ # containerid
        """,
        re.VERBOSE,
    )
    process_name = ""

    while True:
        try:
            syslog = sys.stdin.readline()
        except StopIteration:
            break
        if not syslog:
            break
        r = process_name_regex.search(syslog)
        if r:
            process_name = r.group(1)
        r = oom_regex_docker.search(syslog)
        if r:
            yield (int(r.group(1)), r.group(2), r.group(3), process_name)
            process_name = ""
        r = oom_regex_kubernetes.search(syslog)
        if r:
            yield (int(r.group(1)), r.group(2), r.group(3), process_name)
            process_name = ""
        r = oom_regex_kubernetes_structured.search(syslog)
        if r:
            yield (int(r.group(1)), r.group(2), r.group(3), process_name)
            process_name = ""


def get_container_env_as_dict(docker_inspect):
    env_vars = {}
    config = docker_inspect.get("Config")
    if config is not None:
        env = config.get("Env", [])
        for i in env:
            name, _, value = i.partition("=")
            env_vars[name] = value
    return env_vars


def log_to_clog(log_line):
    """Send the event to 'tmp_paasta_oom_events'."""
    line = (
        '{"timestamp": %d, "hostname": "%s", "container_id": "%s", "cluster": "%s", '
        '"service": "%s", "instance": "%s", "process_name": "%s", '
        '"mesos_container_id": "%s", "mem_limit": "%s"}'
        % (
            log_line.timestamp,
            log_line.hostname,
            log_line.container_id,
            log_line.cluster,
            log_line.service,
            log_line.instance,
            log_line.process_name,
            log_line.mesos_container_id,
            log_line.mem_limit,
        )
    )
    clog.log_line("tmp_paasta_oom_events", line)


def log_to_paasta(log_line):
    """Add the event to the standard PaaSTA logging backend."""
    line = "oom-killer killed {} on {} (container_id: {}).".format(
        "a %s process" % log_line.process_name
        if log_line.process_name
        else "a process",
        log_line.hostname,
        log_line.container_id,
    )
    _log(
        service=log_line.service,
        instance=log_line.instance,
        component="oom",
        cluster=log_line.cluster,
        level=DEFAULT_LOGLEVEL,
        line=line,
    )


def send_sfx_event(service, instance, cluster):
    if yelp_meteorite:
        service_instance_config = get_instance_config(
            service=service, instance=instance, cluster=cluster
        )
        dimensions = {
            "paasta_cluster": cluster,
            "paasta_instance": instance,
            "paasta_service": service,
            "paasta_pool": service_instance_config.get_pool(),
        }
        yelp_meteorite.events.emit_event(
            "paasta.service.oom_events", dimensions=dimensions,
        )
        counter = yelp_meteorite.create_counter(
            "paasta.service.oom_count", default_dimensions=dimensions,
        )
        counter.count()


def main():
    if clog is None:
        print("CLog logger unavailable, exiting.", file=sys.stderr)
        sys.exit(1)

    clog.config.configure(
        scribe_host="169.254.255.254",
        scribe_port=1463,
        monk_disable=False,
        scribe_disable=False,
    )

    cluster = load_system_paasta_config().get_cluster()
    client = get_docker_client()
    for (
        timestamp,
        hostname,
        container_id,
        process_name,
    ) in capture_oom_events_from_stdin():
        try:
            docker_inspect = client.inspect_container(resource_id=container_id)
        except (APIError):
            continue
        env_vars = get_container_env_as_dict(docker_inspect)
        service = env_vars.get("PAASTA_SERVICE", "unknown")
        instance = env_vars.get("PAASTA_INSTANCE", "unknown")
        mesos_container_id = env_vars.get("MESOS_CONTAINER_NAME", "mesos-null")
        mem_limit = env_vars.get("PAASTA_RESOURCE_MEM", "unknown")
        log_line = LogLine(
            timestamp=timestamp,
            hostname=hostname,
            container_id=container_id,
            cluster=cluster,
            service=service,
            instance=instance,
            process_name=process_name,
            mesos_container_id=mesos_container_id,
            mem_limit=mem_limit,
        )
        log_to_clog(log_line)
        log_to_paasta(log_line)
        send_sfx_event(service, instance, cluster)


if __name__ == "__main__":
    main()
