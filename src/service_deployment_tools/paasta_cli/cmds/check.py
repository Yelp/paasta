#!/usr/bin/env python
"""
Contains methods used by the paasta client to check whether Yelp service
passes all the markers required to be considered paasta ready.
"""


def add_subparser(subparsers):
    check_parser = subparsers.add_parser(
        'check', description="Determine whether service in pwd is paasta ready.")
    check_parser.set_defaults(command=paasta_check)


def paasta_check():
    """
    Analyze the service in the PWD to determine if it is paasta ready
    """
    # TODO: Write this method on next ticket

    print "Executing check"
