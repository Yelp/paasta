#!/usr/bin/env python
"""Contains methods used by the paasta client to build and test a docker image."""

import subprocess
import sys

from paasta_tools.paasta_cli.utils import validate_service_name


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
    service’s path in git. E.g. For git.yelpcorp.com:services/foo the
    upstream_job_name is services-foo.

    upstream_git_commit is the SHA that we’re building. Usually this is the
    tip of origin/master.
    """
    cmd = 'DOCKER_TAG="docker-paasta.yelpcorp.com:443/services-%s:paasta-%s" make itest' % (
        upstream_job_name,
        upstream_git_commit,
    )
    return cmd


def paasta_itest(args):
    """Build and test a docker image"""
    service_name = args.service
    if service_name and service_name.startswith('services-'):
        service_name = service_name.split('services-', 1)[1]
    validate_service_name(service_name)

    cmd = build_command(service_name, args.commit)
    print 'INFO: Executing command "%s"' % cmd
    try:
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
    except subprocess.CalledProcessError as exc:
        print 'ERROR: Failed to run itest. Output:\n%sReturn code was: %d' % (exc.output, exc.returncode)
        sys.exit(exc.returncode)
