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
from urlparse import urlparse

from paasta_tools.cli.utils import calculate_remote_masters
from paasta_tools.cli.utils import find_connectable_master
from paasta_tools.cli.utils import get_status_for_instance
from paasta_tools.cli.utils import get_subparser
from paasta_tools.cli.utils import pick_slave_from_status
from paasta_tools.marathon_tools import load_marathon_config
from paasta_tools.mesos_tools import get_mesos_master
from paasta_tools.utils import _run
from paasta_tools.utils import load_system_paasta_config


def add_subparser(subparsers):
    new_parser = get_subparser(description="'paasta sysdig' works by SSH'ing to remote PaaSTA masters and "
                                           "running sysdig with the neccessary filters",
                               help_text="Run sysdig on a remote host and filter to a service and instance",
                               command='sysdig',
                               function=paasta_sysdig,
                               subparsers=subparsers)
    new_parser.add_argument(
        '-l', '--local',
        help="Run the script here rather than SSHing to a PaaSTA master",
        default=False,
        action='store_true'
    )


def get_any_mesos_master(cluster):
    system_paasta_config = load_system_paasta_config()
    masters, output = calculate_remote_masters(cluster, system_paasta_config)
    if not masters:
        print 'ERROR: %s' % output
        sys.exit(1)
    mesos_master, output = find_connectable_master(masters)
    if not mesos_master:
        print 'ERROR: could not find connectable master in cluster %s\nOutput: %s' % (cluster, output)
        sys.exit(1)
    return mesos_master


def paasta_sysdig(args):
    if not args.local:
        mesos_master = get_any_mesos_master(cluster=args.cluster)
        ssh_cmd = 'ssh -At -o LogLevel=QUIET {0} "sudo paasta {1} --local"'.format(mesos_master, ' '.join(sys.argv[1:]))
        return_code, output = _run(ssh_cmd)
        if return_code != 0:
            print output
            sys.exit(return_code)
        slave, command = output.split(':', 1)
        subprocess.call(shlex.split("ssh -tA {0} '{1}'".format(slave, command.strip())))
        return
    status = get_status_for_instance(cluster=args.cluster,
                                     service=args.service,
                                     instance=args.instance)
    slave = pick_slave_from_status(status=status,
                                   host=args.host)
    marathon_config = load_marathon_config()
    marathon_url = marathon_config.get_url()[0]
    marathon_user = marathon_config.get_username()
    marathon_pass = marathon_config.get_password()
    mesos_url = get_mesos_master().host
    marathon_parsed_url = urlparse(marathon_url)
    marathon_creds_url = marathon_parsed_url._replace(netloc="{0}:{1}@{2}".format(marathon_user, marathon_pass,
                                                                                  marathon_parsed_url.netloc))
    print format_mesos_command(slave, status.marathon.app_id, mesos_url, marathon_creds_url.geturl())


def format_mesos_command(slave, app_id, mesos_url, marathon_url):
    sysdig_mesos = '{0},{1}'.format(mesos_url, marathon_url)
    command = 'sudo csysdig -m {0} marathon.app.id="/{1}" -v mesos_tasks'.format(sysdig_mesos, app_id)
    return slave + ":" + command
