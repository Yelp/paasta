#!/usr/bin/env python
"""
Contains methods used by the paasta client to generate a Jenkins build pipeline.
"""
import subprocess
import sys

from service_deployment_tools.paasta_cli.utils import \
    guess_service_name, NoSuchService


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'generate-pipeline',
        description='Run `paasta generate-pipeline` in root of your service or '
                    'with -s $SERVICENAME. Uses a deploy.yaml in '
                    '/nail/etc/services/$SERVICENAME/deploy.yaml',
        help='Configure a Jenkins build pipeline.')

    list_parser.add_argument('-s', '--service',
                             help='Name of service for which you wish to '
                                  'generate a Jenkins pipeline',
                             )

    list_parser.set_defaults(command=paasta_generate_pipeline)


def paasta_generate_pipeline(args):
    """
    Generate a Jenkins build pipeline
    """

    # Get the service name
    try:
        service_name = args.service or guess_service_name()
    except NoSuchService as service_not_found:
        print service_not_found
        sys.exit(1)

    # Build pipeline
    try:
        args1 = 'setup_jenkins:services/%s,' \
                'profile=paasta,job_disabled=False' % service_name

        print "INFO: Executing fab_repo %s" % args1
        subprocess.check_call(['fab_repo', args1])

        args2 = 'setup_jenkins:services/%s,' \
                'profile=paasta_boilerplate' % service_name

        print "INFO: Executing fab_repo %s" % args2
        subprocess.check_call(['fab_repo', args2])

    except subprocess.CalledProcessError as subprocess_error:
        print "%s\nFailed to generate Jenkins pipeline" % subprocess_error
        sys.exit(1)
