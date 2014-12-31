#!/usr/bin/env python
"""Contains methods used by the paasta client to upload a docker
image to a registry.
"""

import subprocess
import sys

from paasta_tools.paasta_cli.utils import validate_service_name


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'promote-to-registry',
        description='Uploads a docker image to a registry',
        help='Uploads a docker image to a registry')

    list_parser.add_argument('-s', '--service',
                             help='Name of service for which you wish to '
                                  'generate a Jenkins pipeline',
                             required=True,
                             )
    list_parser.add_argument('-c', '--commit',
                             help='Git sha to name the remote image',
                             required=True,
                             )

    list_parser.set_defaults(command=paasta_promote_to_registry)


def build_promote_command(upstream_job_name, upstream_git_commit):
    return "docker push docker-paasta.yelpcorp.com:443/%s:paasta-%s" % (
        upstream_job_name,
        upstream_git_commit,
    )


def paasta_promote_to_registry(args):
    """Upload a docker image to a registry"""
    service_name = args.service
    validate_service_name(service_name)

    cmd = build_promote_command(service_name, args.commit)
    try:
        print "INFO: Executing promote command '%s'" % cmd
        subprocess.check_call([cmd])
    except subprocess.CalledProcessError as subprocess_error:
        print "ERROR: Failed to promote image:\n%s" % subprocess_error
        sys.exit(1)
