#!/usr/bin/env python
"""Contains methods used by the paasta client to build and test a docker image."""

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
                             help='Test and build docker image for this service. Leading "services-", as included in a Jenkins job name, will be stripped.',
                             required=True,
                             )
    list_parser.add_argument('-c', '--commit',
                             help='Git sha used to construct tag for built image',
                             required=True,
                             )

    list_parser.set_defaults(command=paasta_itest)


def build_command(upstream_job_name, upstream_git_commit):
    """docker-paasta.yelpcorp.com:443 is the URL for the Registry where PaaSTA
    will look for your images.

    upstream_job_name is a sanitized-for-Jenkins (s,/,-,g) version of the
    service's path in git. E.g. For git.yelpcorp.com:services/foo the
    upstream_job_name is services-foo.

    upstream_git_commit is the SHA that we're building. Usually this is the
    tip of origin/master.
    """
    cmd = 'DOCKER_TAG="docker-paasta.yelpcorp.com:443/services-%s:paasta-%s" make itest' % (
        upstream_job_name,
        upstream_git_commit,
    )
    return cmd


def paasta_itest(args):
    """Build and test a docker image"""
    component = 'jenkins'
    service_name = args.service
    if service_name and service_name.startswith('services-'):
        service_name = service_name.split('services-', 1)[1]
    validate_service_name(service_name)

    cmd = build_command(service_name, args.commit)
    _log(service_name, 'Executing command "%s"' % cmd, component=component)
    returncode, output = _run(cmd)
    if returncode != 0:
        _log(service_name, 'Execution failed. Return code: %s' % str(returncode), component=component, level='ERROR')
        for line in output.split('\n'):
            _log(service_name, line, component=component, level='DEBUG')
        sys.exit(returncode)
    _log(service_name, 'Execution successful.', component=component)
    for line in output.split('\n'):
        _log(service_name, line, component=component, level='DEBUG')
