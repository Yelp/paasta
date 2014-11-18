#!/usr/bin/env python

# To enable autocompletion, run:
# eval "$(register-python-argcomplete paasta-cli.py)"
# TODO: Move the above command to the build process
"""Usage: paasta [options]

A paasta client to interface with the Yelp paasta stack.

Command line options:
[check | list] : name of paasta command to execute

"""
import argcomplete
import argparse

from cmds.list import paasta_list
from cmds.check import paasta_check


def commands():
    """
    :return: A list of commands that paasta_cli can execute
    """
    return ['check', 'list']


def parse_args():
    """
    Initialize autocompletion and configure the argument parser
    :return: a namespace mapping parameter names to the inputs from sys.argv
    """
    parser = argparse.ArgumentParser(description="Yelp PAASTA client")

    parser.add_argument('cmd', help='paasta client command to execute',
                        choices=commands())

    argcomplete.autocomplete(parser)

    return parser.parse_args()


def main():
    """
    Performs a paasta cli call
    """

    args = parse_args()

    cmd = args.cmd

    if cmd == 'list':
        paasta_list()
    elif cmd == 'check':
        paasta_check()


if __name__ == '__main__':
    main()