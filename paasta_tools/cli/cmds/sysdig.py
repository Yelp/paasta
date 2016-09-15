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

from paasta_tools.api import client
from paasta_tools.cli.utils import calculate_remote_masters
from paasta_tools.cli.utils import find_connectable_master
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_services
from paasta_tools.marathon_tools import load_marathon_config
from paasta_tools.mesos_tools import get_mesos_master
from paasta_tools.mesos_tools import get_running_tasks_from_active_frameworks
from paasta_tools.utils import _run
from paasta_tools.utils import list_clusters
from paasta_tools.utils import load_system_paasta_config


def add_subparser(subparsers):
    sysdig_parser = subparsers.add_parser(
        'sysdig',
        help="Run sysdig on a remote host and filter to a service and instance",
        description=(
            "'paasta sysdig' works by SSH'ing to remote PaaSTA masters and "
            "running sysdig with the neccessary filters"
        ),
        epilog=(
            "Note: This command requires SSH and sudo privileges on the remote PaaSTA "
            "nodes."
        ),
    )
    sysdig_parser.add_argument(
        '-s', '--service',
        help='The name of the service you wish to inspect',
        required=True
    ).completer = lazy_choices_completer(list_services)
    sysdig_parser.add_argument(
        '-c', '--cluster',
        help="Cluster on which the service is running"
             "For example: --cluster norcal-prod",
        required=True
    ).completer = lazy_choices_completer(list_clusters)
    sysdig_parser.add_argument(
        '-i', '--instance',
        help="The instance that you wish to inspect with sysdig"
             "For example: --instance main",
        required=True,
        default='main'
    )  # No completer because we need to know service first and we can't until some other stuff has happened
    sysdig_parser.add_argument(
        '-l', '--local',
        help="Run the script here rather than SSHing to a PaaSTA master",
        default=False,
        action='store_true'
    )
    sysdig_parser.add_argument(
        '-H', '--host',
        dest="host",
        default=None,
        help="Specify a specific host on which to run sysdig. Defaults to"
             " one that is running the service chosen at random"
    )
    sysdig_parser.set_defaults(command=paasta_sysdig)


def paasta_sysdig(args):
    if not args.local:
        system_paasta_config = load_system_paasta_config()
        masters, output = calculate_remote_masters(args.cluster, system_paasta_config)
        if masters == []:
            print 'ERROR: %s' % output
        mesos_master, output = find_connectable_master(masters)
        if not mesos_master:
            print 'ERROR: could not find connectable master in cluster %s\nOutput: %s' % (args.cluster, output)
        ssh_cmd = 'ssh -At -o LogLevel=QUIET {0} "sudo paasta {1} --local"'.format(mesos_master, ' '.join(sys.argv[1:]))
        return_code, output = _run(ssh_cmd)
        if return_code != 0:
            print output
            sys.exit(return_code)
        slave, command = output.split(':', 1)
        print command
        subprocess.call(shlex.split("ssh -tA {0} '{1}'".format(slave, command.strip())))
        sys.exit(0)
    api = client.get_paasta_api_client(cluster=args.cluster)
    if not api:
        sys.exit(1)
    status = api.service.status_instance(service=args.service, instance=args.instance).result()
    app_id = status.marathon.app_id
    if args.host:
        slave = args.host
    else:
        slaves = [task.slave['hostname'] for task in get_running_tasks_from_active_frameworks(status.marathon.app_id)]
        slave = slaves[0]
    marathon_config = load_marathon_config()
    marathon_url = marathon_config.get_url()[0]
    marathon_user = marathon_config.get_username()
    marathon_pass = marathon_config.get_password()
    mesos_url = get_mesos_master().host
    marathon_parsed_url = urlparse(marathon_url)
    marathon_creds_url = marathon_parsed_url._replace(netloc="{0}:{1}@{2}".format(marathon_user, marathon_pass,
                                                                                  marathon_parsed_url.netloc))
    sysdig_mesos = '{0},{1}'.format(mesos_url, marathon_creds_url.geturl())
    command = 'sudo csysdig -m {0} marathon.app.id="{1}" -v mesos_tasks'.format(sysdig_mesos, app_id)
    print slave + ":" + command
