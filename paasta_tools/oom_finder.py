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
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import re
from tempfile import gettempdir

from docker.errors import APIError

from paasta_tools.utils import get_docker_client


def position_file_name():
    """Return an absolute path of the position file.

    See tail_log_file() for details.
    """
    name = '{}_position'.format(os.path.splitext(os.path.basename(__file__))[0])
    return os.path.join(gettempdir(), name)


def tail_log_file(log_file='/var/log/messages'):
    """Iterate new records from the log_file.

    To avoid reading the same records twice this function uses
    an auxiliary file to store the current position with log_file.
    """
    position_file = position_file_name()
    try:
        with open(position_file, 'r') as p:
            last_position = int(p.read())
        log_size = os.path.getsize(log_file)
        if last_position > log_size:
            last_position = 0
    except (IOError, ValueError):
        last_position = 0

    with open(log_file) as f:
        f.seek(last_position)
        for line in f:
            yield line.rstrip()
        last_position = f.tell()

    with open(position_file, 'w') as p:
        p.write(str(last_position))


def oom_killings():
    """Iterate over all OOM events in docker containers"""
    oom_regex = re.compile('^(.*)\skernel:\s.*Task in /docker/(\w{12})\w+ killed as a result of limit of (.*)')

    for line in tail_log_file():
        r = oom_regex.search(line)
        if r:
            yield (r.group(1), r.group(2))


def get_container_env_as_dict(docker_inspect):
    env_vars = {}
    config = docker_inspect.get('Config')
    if config is not None:
        env = config.get('Env', [])
        for i in env:
            name, _, value = i.partition('=')
            env_vars[name] = value
    return env_vars


def get_service_instance(env_vars):
    return '{}.{}'.format(
        env_vars.get('PAASTA_SERVICE', ''),
        env_vars.get('PAASTA_INSTANCE', ''),
    )


def main():
    client = get_docker_client()
    for time_hostname, container_id in oom_killings():
        try:
            inspect = client.inspect_container(resource_id=container_id)
        except (APIError):
            continue
        env = get_container_env_as_dict(inspect)
        service_instance = get_service_instance(env)
        print("{} {} {}".format(time_hostname, container_id, service_instance))


if __name__ == "__main__":
    main()
