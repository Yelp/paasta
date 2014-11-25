#!/usr/bin/env python
"""
Contains methods used by the paasta client to check whether Yelp service
passes all the markers required to be considered paasta ready.
"""
from service_deployment_tools.paasta_cli.cmds import checks
from service_deployment_tools.paasta_cli.utils import \
    file_names_in_dir as checks_dir, load_method


def add_subparser(subparsers):
    check_parser = subparsers.add_parser(
        'check', description="Determine whether service in pwd is paasta ready.")
    check_parser.set_defaults(command=paasta_check)


def paasta_check(args):
    """
    Analyze the service in the PWD to determine if it is paasta ready
    """
    for check_task in checks_dir(checks):

        # Path to the file containing the check to perform
        check_task = "service_deployment_tools.paasta_cli.cmds.checks.%s"\
                     % check_task

        # Determine whether the check passes
        if load_method(check_task, 'check')():
            # Execute success behaviour
            load_method(check_task, 'success')()
        else:
            # Execute failure behaviour
            load_method(check_task, 'fail')()
