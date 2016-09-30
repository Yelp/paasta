#!/usr/bin/env python
# Copyright 2015-2016 Yelp Inc.
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
import subprocess

from paasta_tools.cli.utils import get_status_for_instance
from paasta_tools.cli.utils import get_subparser
from paasta_tools.cli.utils import pick_slave_from_status
from paasta_tools.mesos_tools import get_container_name


def add_subparser(subparsers):
    get_subparser(description="'paasta docker_inspect' works by picking a container running your service "
                              "at random. It then runs docker docker_inspect <container_id> ",
                  help_text="Docker inspect against a container running your service",
                  command='docker_inspect',
                  function=paasta_docker_inspect,
                  subparsers=subparsers)


def paasta_docker_inspect(args):
    status = get_status_for_instance(cluster=args.cluster,
                                     service=args.service,
                                     instance=args.instance)
    slave = pick_slave_from_status(status=status,
                                   host=args.host)
    container = get_container_name(status.marathon.app_id, slave_hostname=slave, task_id=args.mesos_id)
    command = "sudo docker inspect {0}".format(container)
    subprocess.call(["ssh", "-o", "LogLevel=QUIET", "-tA", slave, command])
