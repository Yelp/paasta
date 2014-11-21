#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK

# To enable autocompletion, run:
# eval "$(register-python-argcomplete paasta_cli.py)"
# TODO: Move the above command to the build process
"""
A command line tool for viewing information from the PaaSTA stack.

NOTE: make paasta_cli executable for tab completion to function properly

"""
import glob
import os

import argcomplete
import argparse


def paasta_commands():
    """
    Read the files names in the cmds directory to determine the various commands
    the paasta client is able to execute
    :return: a list of string such as ['list','check'] that correspond to a
    file in cmds
    """
    my_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(my_dir, 'cmds', '*.py')
    for file_name in glob.glob(path):
        basename = os.path.basename(file_name)
        root, _ = os.path.splitext(basename)
        if root == '__init__':
            continue
        yield root


def load_method(module_name, method_name):
    """
    Return a function given a module and method name
    :param module_name: a string
    :param method_name: a string
    :return: a function
    """
    module = __import__(module_name, fromlist=[method_name])
    method = getattr(module, method_name)
    return method


def add_subparser(command, subparsers):
    """
    Given a command name, paasta_cmd, execute the add_subparser method
    implemented in paasta_cmd.py

    Each paasta client command must implement a method called add_subparser.
    This allows the client to dynamically add subparsers to its subparser, which
    provides the benefits of argcomplete/argparse but gets it done in a modular
    fashion.

    :param command: a simple string - e.g. 'list'
    :param subparsers: an ArgumentParser object
    """
    module_name = 'service_deployment_tools.paasta_cli.cmds.%s' % command
    add_subparser_fn = load_method(module_name, 'add_subparser')
    add_subparser_fn(subparsers)


def parse_args():
    """
    Initialize autocompletion and configure the argument parser
    :return: a namespace mapping parameter names to the inputs from sys.argv
    """
    parser = argparse.ArgumentParser(description="Yelp PaaSTA client")

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
