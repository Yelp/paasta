#!/usr/bin/env python
"""Contains methods used by the paasta client to build and test a docker image."""

import os
import sys

from paasta_tools.paasta_cli.utils import validate_service_name
from paasta_tools.utils import _log
from paasta_tools.utils import _run


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'itest',
        description='Builds and tests a docker image',
        help='Builds and tests a docker image')

    list_parser.add_argument('-s', '--service',
                             help='Test and build docker image for this service. Leading '
                                  '"services-", as included in a Jenkins job name, '
                                  'will be stripped.',
                             required=True,
                             )
    list_parser.add_argument('-c', '--commit',
                             help='Git sha used to construct tag for built image',
                             required=True,
                             )

    list_parser.set_defaults(command=paasta_itest)


def build_docker_tag(upstream_job_name, upstream_git_commit):
    """docker-paasta.yelpcorp.com:443 is the URL for the Registry where PaaSTA
    will look for your images.

    upstream_job_name is a sanitized-for-Jenkins (s,/,-,g) version of the
    service's path in git. E.g. For git.yelpcorp.com:services/foo the
    upstream_job_name is services-foo.

    upstream_git_commit is the SHA that we're building. Usually this is the
    tip of origin/master.
    """
    tag = 'docker-paasta.yelpcorp.com:443/services-%s:paasta-%s' % (
        upstream_job_name,
        upstream_git_commit,
    )
    return tag


def paasta_itest(args):
    """Build and test a docker image"""
    service_name = args.service
    if service_name and service_name.startswith('services-'):
        service_name = service_name.split('services-', 1)[1]
    validate_service_name(service_name)

    tag = build_docker_tag(service_name, args.commit)
    run_env = os.environ.copy()
    run_env['DOCKER_TAG'] = tag
    cmd = "make itest"

    returncode, output = _run(
        cmd,
        env=run_env,
        timeout=3600,
        log=True,
        component='build',
        service_name=service_name,
        loglevel='debug'
    )
    if returncode != 0:
        _log(
            service_name=service_name,
            line='ERROR: itest failed for %s. See output: %s' %
            (args.commit, os.environ.get('BUILD_URL', '') + 'console'),
            component='build',
            level='event',
        )
        sys.exit(returncode)
    _log(
        service_name=service_name,
        line='itest passed for %s.' % args.commit,
        component='build',
        level='event',
    )
