#!/usr/bin/env python
"""Contains methods used by the paasta client to generate a Jenkins build
pipeline."""
import sys

from paasta_tools.paasta_cli.utils import lazy_choices_completer
from paasta_tools.paasta_cli.utils import list_services
from paasta_tools.paasta_cli.utils import \
    guess_service_name, NoSuchService, validate_service_name
from paasta_tools.utils import _run
from paasta_tools.monitoring_tools import get_team_email_address
from paasta_tools.monitoring_tools import get_team


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'generate-pipeline',
        description='Run `paasta generate-pipeline` in root of your service or '
                    'with -s $SERVICENAME. Uses a deploy.yaml in '
                    '/nail/etc/services/$SERVICENAME/deploy.yaml',
        help='Configure a Jenkins build pipeline.')

    list_parser.add_argument('-s', '--service',
                             help='Name of service for which you wish to generate a Jenkins pipeline',
                             ).completer = lazy_choices_completer(list_services)

    list_parser.set_defaults(command=paasta_generate_pipeline)


def paasta_generate_pipeline(args):
    """Generate a Jenkins build pipeline.
    :param args: argparse.Namespace obj created from sys.args by paasta_cli"""
    service_name = args.service or guess_service_name()
    try:
        validate_service_name(service_name)
    except NoSuchService as service_not_found:
        print service_not_found
        sys.exit(1)

    generate_pipeline(service=service_name)


def generate_pipeline(service):
    email_address = get_team_email_address(service=service)
    if email_address is None:
        email_address = get_team(overrides={}, service_name=service)
    cmds = [
        'fab_repo setup_jenkins:services/%s,'
        'profile=paasta,job_disabled=False,owner=%s' % (service, email_address),
        'fab_repo setup_jenkins:services/%s,'
        'profile=paasta_boilerplate,owner=%s' % (service, email_address),
    ]
    for cmd in cmds:
        print "INFO: Executing %s" % cmd
        returncode, output = _run(cmd, timeout=90)
        if returncode != 0:
            print "ERROR: Failed to generate Jenkins pipeline"
            print output
            sys.exit(returncode)
