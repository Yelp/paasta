#!/usr/bin/env python
"""
Blah blah blah
"""
import subprocess
import sys

from service_deployment_tools.paasta_cli.utils import \
    guess_service_name, NoSuchService


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'generate-pipeline',
        description='foo',
        help='bar')
    list_parser.set_defaults(command=paasta_generate_pipeline)


def paasta_generate_pipeline(args):
    """
    Shmuf
    """
    try:
        service_name = guess_service_name()
        try:
            args1 = 'setup_jenkins:services/' \
                    '%s,profile=paasta,job_disabled=False' % service_name

            subprocess.check_call(['fab_repo', args1], shell=True)

            args2 = 'setup_jenkins:services/' \
                    '%s,profile=paasta_boilerplate' % service_name

            subprocess.check_call(['fab_repo', args2], shell=True)
        except subprocess.CalledProcessError:
            print "generate-pipeline failed"
            sys.exit(1)
    except NoSuchService:
        print "SERVICE NOT FOUND"
        sys.exit(1)
