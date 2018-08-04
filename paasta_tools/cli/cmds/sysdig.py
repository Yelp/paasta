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
import shlex
import subprocess
import sys
from urllib.parse import urlparse

from paasta_tools.cli.utils import calculate_remote_masters
from paasta_tools.cli.utils import find_connectable_master
from paasta_tools.cli.utils import get_status_for_instance
from paasta_tools.cli.utils import get_subparser
from paasta_tools.cli.utils import pick_slave_from_status
from paasta_tools.marathon_tools import get_marathon_clients
from paasta_tools.marathon_tools import get_marathon_servers
from paasta_tools.marathon_tools import load_marathon_service_config
from paasta_tools.mesos_tools import get_mesos_master
from paasta_tools.utils import _run
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print


def add_subparser(subparsers):
    new_parser = get_subparser(
        description="'paasta sysdig' works by SSH'ing to remote PaaSTA masters and "
                    "running sysdig with the necessary filters",
        help_text="Run sysdig on a remote host and filter to a service and instance",
        command='sysdig',
        function=paasta_sysdig,
        subparsers=subparsers,
    )
    new_parser.add_argument(
        '-l', '--local',
        help="Run the script here rather than SSHing to a PaaSTA master",
        default=False,
        action='store_true',
    )


def get_any_mesos_master(cluster, system_paasta_config):
    masters, output = calculate_remote_masters(cluster, system_paasta_config)
    if not masters:
        paasta_print('ERROR: %s' % output)
        sys.exit(1)
    mesos_master, output = find_connectable_master(masters)
    if not mesos_master:
        paasta_print(f'ERROR: could not find connectable master in cluster {cluster}\nOutput: {output}')
        sys.exit(1)
    return mesos_master


def paasta_sysdig(args):
    system_paasta_config = load_system_paasta_config()

    if not args.local:
        mesos_master = get_any_mesos_master(cluster=args.cluster, system_paasta_config=system_paasta_config)
        ssh_cmd = (
            'ssh -At -o StrictHostKeyChecking=no -o LogLevel=QUIET {0} '
            '"sudo paasta {1} --local"'
        ).format(mesos_master, ' '.join(sys.argv[1:]))
        return_code, output = _run(ssh_cmd)
        if return_code != 0:
            paasta_print(output)
            sys.exit(return_code)
        slave, command = output.split(':', 1)
        subprocess.call(shlex.split("ssh -tA {} '{}'".format(slave, command.strip())))
        return
    status = get_status_for_instance(
        cluster=args.cluster,
        service=args.service,
        instance=args.instance,
    )
    slave = pick_slave_from_status(
        status=status,
        host=args.host,
    )

    job_config = load_marathon_service_config(
        service=args.service,
        instance=args.instance,
        cluster=args.cluster,
    )

    marathon_servers = get_marathon_servers(system_paasta_config)
    marathon_clients = get_marathon_clients(marathon_servers)

    # Unfortunately, sysdig seems to only be able to take one marathon URL, so hopefully the service in question is not
    # currently moving between shards.
    client = marathon_clients.get_current_client_for_service(
        job_config=job_config,
    )
    marathon_url = client.servers[0]
    marathon_user, marathon_pass = client.auth

    mesos_url = get_mesos_master().host
    marathon_parsed_url = urlparse(marathon_url)
    marathon_creds_url = marathon_parsed_url._replace(netloc="{}:{}@{}".format(
        marathon_user, marathon_pass,
        marathon_parsed_url.netloc,
    ))
    paasta_print(format_mesos_command(slave, status.marathon.app_id, mesos_url, marathon_creds_url.geturl()))


def format_mesos_command(slave, app_id, mesos_url, marathon_url):
    sysdig_mesos = f'{mesos_url},{marathon_url}'
    command = f'sudo csysdig -m {sysdig_mesos} marathon.app.id="/{app_id}" -v mesos_tasks'
    return slave + ":" + command
