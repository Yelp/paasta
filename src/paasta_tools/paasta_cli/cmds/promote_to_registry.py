#!/usr/bin/env python
"""Contains methods used by the paasta client to upload a docker
image to a registry.
"""

import subprocess
import sys

from paasta_tools.paasta_cli.utils import NoSuchService
from paasta_tools.paasta_cli.utils import validate_service_name


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'promote-to-registry',
        description='Uploads a docker image to a registry',
        help='Uploads a docker image to a registry')

    list_parser.add_argument('-s', '--service',
                             help='Name of service for which you wish to '
                                  'generate a Jenkins pipeline',
                             )
    list_parser.add_argument('--sha',
                             help='Git sha to name the remote image',
                             )

    list_parser.set_defaults(command=paasta_promote_to_registry)


def build_promote_command():
    # docker push docker-paasta.yelpcorp.com:443/$upstream_job_name:paasta-$upstream_git_commit
    return ""


def paasta_promote_to_registry(args):
    """Upload a docker image to a registry"""
    service_name = args.service
    try:
        validate_service_name(service_name)
    except NoSuchService as service_not_found:
        print service_not_found
        sys.exit(1)

    cmd = build_promote_command()
    try:
        print "INFO: Executing fab_repo %s" % cmd
        subprocess.check_call([cmd])
    except subprocess.CalledProcessError as subprocess_error:
        print "%s\nFailed to promote image" % subprocess_error
        sys.exit(1)
