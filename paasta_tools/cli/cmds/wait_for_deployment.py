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
"""Contains methods used by the paasta client to wait for deployment
of a docker image to a cluster.instance.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from paasta_tools.cli.cmds.mark_for_deployment import NoInstancesFound
from paasta_tools.cli.cmds.mark_for_deployment import wait_for_deployment
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_deploy_groups
from paasta_tools.cli.utils import list_services
from paasta_tools.cli.utils import NoSuchService
from paasta_tools.cli.utils import validate_full_git_sha
from paasta_tools.cli.utils import validate_given_deploy_groups
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.utils import _log
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import TimeoutError


DEFAULT_DEPLOYMENT_TIMEOUT = 3600  # seconds


log = logging.getLogger(__name__)


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'wait-for-deployment',
        help='Wait a service to be deployed to deploy_group',
        description=(
            "'paasta wait-for-deployment' waits for a previously marked for "
            "deployment service to be deployed to deploy_group."
        ),
        epilog=(
            "Note: Access and credentials to the Git repo of a service "
            "are required for this command to work."
        )
    )
    list_parser.add_argument(
        '-c', '-k', '--commit',
        help='Git sha to wait for deployment',
        required=True,
        type=validate_full_git_sha,
    )
    list_parser.add_argument(
        '-l', '--deploy-group',
        help='deploy group (e.g. cluster1.canary, cluster2.main).',
        required=True,
    ).completer = lazy_choices_completer(list_deploy_groups)
    list_parser.add_argument(
        '-s', '--service',
        help='Name of the service which you wish to wait for deployment. '
        'Leading "services-" will be stripped.',
        required=True,
    ).completer = lazy_choices_completer(list_services)
    list_parser.add_argument(
        '-t', '--timeout',
        dest="timeout",
        type=int,
        default=DEFAULT_DEPLOYMENT_TIMEOUT,
        help=(
            "Time in seconds to wait for paasta to deploy the service. "
            "If the timeout is exceeded we return 1. "
            "Default is %(default)s seconds."
        ),
    )
    list_parser.add_argument(
        '-d', '--soa-dir',
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    list_parser.add_argument(
        '-v', '--verbose',
        action='count',
        dest="verbose",
        default=0,
        help="Print out more output."
    )

    list_parser.set_defaults(command=paasta_wait_for_deployment)


def paasta_wait_for_deployment(args):
    """Wrapping wait_for_deployment"""
    if args.verbose:
        log.setLevel(level=logging.DEBUG)
    else:
        log.setLevel(level=logging.INFO)

    service = args.service
    if service and service.startswith('services-'):
        service = service.split('services-', 1)[1]
    try:
        validate_service_name(service, soa_dir=args.soa_dir)
    except NoSuchService as e:
        paasta_print(PaastaColors.red('%s' % e))
        return 1

    in_use_deploy_groups = list_deploy_groups(service=service,
                                              soa_dir=args.soa_dir)
    _, invalid_deploy_groups = \
        validate_given_deploy_groups(in_use_deploy_groups, [args.deploy_group])

    if len(invalid_deploy_groups) == 1:
        paasta_print(PaastaColors.red("ERROR: These deploy groups are not "
                                      "currently used anywhere: %s.\n" %
                                      (",").join(invalid_deploy_groups)))
        paasta_print(PaastaColors.red("You probably need one of these in-use "
                                      "deploy groups?:\n   %s" %
                                      (",").join(in_use_deploy_groups)))
        return 1

    try:
        wait_for_deployment(service=service,
                            deploy_group=args.deploy_group,
                            git_sha=args.commit,
                            soa_dir=args.soa_dir,
                            timeout=args.timeout)
        _log(service=service,
             component='deploy',
             line=("Deployment of {0} for {1} complete".
                   format(args.commit, args.deploy_group)),
             level='event')

    except (KeyboardInterrupt, TimeoutError):
        paasta_print("Waiting for deployment aborted.")
        return 1
    except NoInstancesFound:
        return 1

    return 0
