#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK

# To enable autocompletion, run:
# eval "$(register-python-argcomplete paasta_cli.py)"
# TODO: Move the above command to the build process
"""Usage: paasta [options]

A paasta client to interface with the Yelp paasta stack.

Command line options:
[check | list] : name of paasta command to execute

NOTE: make paasta_cli executable for tab complete to function

"""


import argcomplete
import argparse

from utils.arg_utils import add_subparser
from utils.cmd_utils import paasta_commands


def parse_args():
    """
    Initialize autocompletion and configure the argument parser
    :return: a namespace mapping parameter names to the inputs from sys.argv
    """
    parser = argparse.ArgumentParser(description="Yelp PAASTA client")

    subparsers = parser.add_subparsers(help="[-h, --help] for subcommand help")

    for command in paasta_commands():
        add_subparser(command, subparsers)

    argcomplete.autocomplete(parser)

    return parser.parse_args()


def main():
    """
    Performs a paasta cli call
    """

    args = parse_args()
    args.command(args)

if __name__ == '__main__':
    main()