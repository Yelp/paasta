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

import sys
import os
import shlex
import service_configuration_lib
import re
import uuid
from datetime import datetime

from paasta_tools.cli.utils import run_on_master
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import validate_service_instance
from paasta_tools.paasta_remote_run import add_remote_run_args


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

    cmd_parts = ['/usr/bin/paasta_remote_run.py']
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
        if not key in args_keys:
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
    if args.very_dry_run:
        # TODO: maybe print which cluster we'd be connecting to?
        status = None
        try:
            master = connectable_master(args.cluster, system_paasta_config)
            paasta_print('Very dry run: would have ssh-ed into %s' % master)
            return_code = 0
        except NoMasterError as e:
            paasta_print('Very dry run: could\'t find connectable master %s' % str(e))
            return_code = err_code
    else:
        return_code, status = run_on_master(args.cluster, system_paasta_config, cmd_parts)

    # Status results are streamed. This print is for possible error messages.
    if status is not None:
        for line in status.rstrip().split('\n'):
            paasta_print('    %s' % line)

    return return_code
