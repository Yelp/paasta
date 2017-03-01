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
from __future__ import absolute_import
from __future__ import unicode_literals

import os

from paasta_tools.cli.utils import get_jenkins_build_output_url
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_services
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.utils import _log
from paasta_tools.utils import _run
from paasta_tools.utils import build_docker_tag
from paasta_tools.utils import check_docker_image
from paasta_tools.utils import DEFAULT_SOA_DIR


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'itest',
        help="Runs 'make itest' as part of the PaaSTA contract.",
        description=(
            "'paasta itest' runs 'make itest' in the root of a service directory. "
            "It is designed to be used in conjection with the 'Jenkins' workflow: "
            "http://paasta.readthedocs.io/en/latest/about/contract.html#jenkins-pipeline-recommended"
        )
    )
    list_parser.add_argument(
        '-s', '--service',
        help='Test and build docker image for this service. Leading '
             '"services-", as included in a Jenkins job name, '
             'will be stripped.',
        required=True,
    )
    list_parser.add_argument(
        '-c', '--commit',
        help='Git sha used to construct tag for built image',
        required=True,
    )
    list_parser.add_argument(
        '-d', '--soa-dir',
        dest='soa_dir',
        help='A directory from which soa-configs should be read from',
        default=DEFAULT_SOA_DIR,
    ).completer = lazy_choices_completer(list_services)
    list_parser.set_defaults(command=paasta_itest)


def paasta_itest(args):
    """Build and test a docker image"""
    service = args.service
    soa_dir = args.soa_dir
    if service and service.startswith('services-'):
        service = service.split('services-', 1)[1]
    validate_service_name(service, soa_dir=soa_dir)

    tag = build_docker_tag(service, args.commit)
    run_env = os.environ.copy()
    run_env['DOCKER_TAG'] = tag
    cmd = "make itest"
    loglines = []

    _log(
        service=service,
        line='starting itest for %s.' % args.commit,
        component='build',
        level='event'
    )
    returncode, output = _run(
        cmd,
        env=run_env,
        timeout=3600,
        log=True,
        component='build',
        service=service,
        loglevel='debug',
    )
    if returncode != 0:
        loglines.append(
            'ERROR: itest failed for %s.' % args.commit
        )
        output = get_jenkins_build_output_url()
        if output:
            loglines.append('See output: %s' % output)
    else:
        loglines.append('itest passed for %s.' % args.commit)
        if not check_docker_image(service, args.commit):
            loglines.append('ERROR: itest has not created %s' % tag)
            returncode = 1
    for logline in loglines:
        _log(
            service=service,
            line=logline,
            component='build',
            level='event',
        )
    return returncode
