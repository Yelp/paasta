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

import re

from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_clusters
from paasta_tools.cli.utils import list_instances
from paasta_tools.cli.utils import list_services
from paasta_tools.cli.utils import run_on_master
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import SystemPaastaConfig


def add_remote_run_args(parser):
    parser.add_argument(
        '-s', '--service',
        help='The name of the service you wish to inspect',
    ).completer = lazy_choices_completer(list_services)
    parser.add_argument(
        '-c', '--cluster',
        help=("The name of the cluster you wish to run your task on. "
              "If omitted, uses the default cluster defined in the paasta remote-run configs"),
    ).completer = lazy_choices_completer(list_clusters)
    parser.add_argument(
        '-y', '--yelpsoa-config-root',
        dest='yelpsoa_config_root',
        help='A directory from which yelpsoa-configs should be read from',
        default=DEFAULT_SOA_DIR,
    )
    parser.add_argument(
        '--json-dict',
        help='When running dry run, output the arguments as a json dict',
        action='store_true',
        dest='dry_run_json_dict',
    )
    parser.add_argument(
        '-C', '--cmd',
        help=('Run Docker container with particular command, '
              'for example: "bash". By default will use the command or args specified by the '
              'soa-configs or what was specified in the Dockerfile'),
        required=False,
        default=None,
    )
    parser.add_argument(
        '-i', '--instance',
        help=("Simulate a docker run for a particular instance of the service, like 'main' or 'canary'"),
        required=False,
        default=None,
    ).completer = lazy_choices_completer(list_instances)
    parser.add_argument(
        '-v', '--verbose',
        help='Show Docker commands output',
        action='store_true',
        required=False,
        default=True,
    )
    parser.add_argument(
        '-d', '--dry-run',
        help='Don\'t launch the task',
        action='store_true',
        required=False,
        default=False,
    )


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'remote-run',
        help="Schedule Mesos to run adhoc command in context of a service",
        description=(
            "'paasta remote-run' is useful for running adhoc commands in context "
            "of a service's Docker image. The command will be scheduled on a "
            "Mesos cluster and stdout/stderr printed after execution is finished."
        ),
        epilog=(
            "Note: 'paasta remote-run' Mesos API that may require authentication."
        ),
    )
    add_remote_run_args(list_parser)
    list_parser.add_argument(
        '-D', '--very-dry-run',
        help='Don\'t ssh into mesos-master',
        action='store_true',
        required=False,
        default=False,
    )
    list_parser.set_defaults(command=paasta_remote_run)


def paasta_remote_run(args):
    try:
        system_paasta_config = load_system_paasta_config()
    except PaastaNotConfiguredError:
        paasta_print(
            PaastaColors.yellow(
                "Warning: Couldn't load config files from '/etc/paasta'. This indicates"
                "PaaSTA is not configured locally on this host, and remote-run may not behave"
                "the same way it would behave on a server configured for PaaSTA."
            ),
            sep='\n',
        )
        system_paasta_config = SystemPaastaConfig({"volumes": []}, '/etc/paasta')

    cmd_parts = ['/usr/bin/paasta_remote_run']
    args_vars = vars(args)
    args_keys = {
        'service': None,
        'cluster': None,
        'yelpsoa_config_root': DEFAULT_SOA_DIR,
        'json_dict': False,
        'cmd': None,
        'verbose': True,
        'dry_run': False
    }
    for key in args_vars:
        # skip args we don't know about
        if key not in args_keys:
            continue

        value = args_vars[key]

        # skip args that have default value
        if value == args_keys[key]:
            continue

        arg_key = re.sub(r'_', '-', key)

        if isinstance(value, bool) and value:
            cmd_parts.append('--%s' % arg_key)
        elif not isinstance(value, bool):
            cmd_parts.extend(['--%s' % arg_key, value])

    paasta_print('Running on master: %s' % cmd_parts)
    return_code, status = run_on_master(
        args.cluster, system_paasta_config, cmd_parts, dry=args.very_dry_run)

    # Status results are streamed. This print is for possible error messages.
    if status is not None:
        for line in status.rstrip().split('\n'):
            paasta_print('    %s' % line)

    return return_code
