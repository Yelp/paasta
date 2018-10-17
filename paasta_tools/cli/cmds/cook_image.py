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
"""Contains methods used by the paasta client to build a docker image."""
import os
import sys

from paasta_tools.cli.cmds.check import makefile_responds_to
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.utils import _log
from paasta_tools.utils import _run
from paasta_tools.utils import get_username
from paasta_tools.utils import paasta_print


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'cook-image',
        description="Calls 'make cook-image' as part of the PaaSTA contract",
        help=(
            "'paasta cook-image' calls 'make cook-image' as part of the PaaSTA contract.\n\n"
            "The PaaSTA contract specifies that a service MUST respond to 'cook-image' and produce "
            "a docker image as a result. This command is often run as part of the normal build pipeline "
            "('paasta itest'), or via a 'paasta local-run --build'."
        ),
        epilog="This command assumes that the Makefile is in the current working directory.",
    )
    list_parser.add_argument(
        '-s', '--service',
        help=(
            'Build docker image for this service. Leading '
            '"services-", as included in a Jenkins job name, '
            'will be stripped.'
        ),
        required=True,
    )
    list_parser.set_defaults(command=paasta_cook_image)


def paasta_cook_image(args, service=None, soa_dir=None):
    """Build a docker image"""
    if not service:
        service = args.service
    if service.startswith('services-'):
        service = service.split('services-', 1)[1]
    validate_service_name(service, soa_dir)

    run_env = os.environ.copy()
    default_tag = 'paasta-cook-image-{}-{}'.format(service, get_username())
    tag = run_env.get('DOCKER_TAG', default_tag)
    run_env['DOCKER_TAG'] = tag

    if not makefile_responds_to('cook-image'):
        paasta_print(
            'ERROR: local-run now requires a cook-image target to be present in the Makefile. See'
            'http://paasta.readthedocs.io/en/latest/about/contract.html', file=sys.stderr,
        )
        return 1

    try:
        cmd = 'make cook-image'
        returncode, output = _run(
            cmd,
            env=run_env,
            log=True,
            component='build',
            service=service,
            loglevel='debug',
        )
        if returncode != 0:
            _log(
                service=service,
                line='ERROR: make cook-image failed for %s.' % service,
                component='build',
                level='event',
            )
        return returncode

    except KeyboardInterrupt:
        paasta_print('\nProcess interrupted by the user. Cancelling.', file=sys.stderr)
        return 2
