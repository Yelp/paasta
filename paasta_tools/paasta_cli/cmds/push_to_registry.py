#!/usr/bin/env python
"""Contains methods used by the paasta client to upload a docker
image to a registry.
"""

import os
import sys

from paasta_tools.paasta_cli.utils import validate_service_name
from paasta_tools.utils import _log
from paasta_tools.utils import _run


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'push-to-registry',
        description='Uploads a docker image to a registry',
        help='Uploads a docker image to a registry')

    list_parser.add_argument('-s', '--service',
                             help='Name of service for which you wish to upload a docker image. Leading "services-", '
                                  'as included in a Jenkins job name, will be stripped.',
                             required=True,
                             )
    list_parser.add_argument('-c', '--commit',
                             help='Git sha after which to name the remote image',
                             required=True,
                             )

    list_parser.set_defaults(command=paasta_push_to_registry)


def build_command(upstream_job_name, upstream_git_commit):
    # This is kinda dumb since we just cleaned the 'services-' off of the
    # service so we could validate it, but the Docker image will have the full
    # name with 'services-' so add it back.
    cmd = 'docker push docker-paasta.yelpcorp.com:443/services-%s:paasta-%s' % (
        upstream_job_name,
        upstream_git_commit,
    )
    return cmd


def paasta_push_to_registry(args):
    """Upload a docker image to a registry"""
    service_name = args.service
    if service_name and service_name.startswith('services-'):
        service_name = service_name.split('services-', 1)[1]
    validate_service_name(service_name)

    cmd = build_command(service_name, args.commit)
    returncode, output = _run(
        cmd,
        timeout=1800,
        log=True,
        component='build',
        service_name=service_name,
        loglevel='debug'
    )
    if returncode != 0:
        logline = 'ERROR: Failed to promote image for %s.\nDetailed output: %s' % \
            (args.commit, os.environ.get('BUILD_URL', '') + 'console')
    else:
        logline = 'Successfully pushed image for %s to registry' % (args.commit,)
    _log(
        service_name=service_name,
        line=logline,
        component='build',
        level='event',
    )
    sys.exit(returncode)
