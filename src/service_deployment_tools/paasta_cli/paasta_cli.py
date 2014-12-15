#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
"""A command line tool for viewing information from the PaaSTA stack."""
import argcomplete
import argparse

from service_deployment_tools.paasta_cli import cmds
from service_deployment_tools.paasta_cli.utils \
    import file_names_in_dir as paasta_commands_dir, load_method


def add_subparser(command, subparsers):
    """Given a command name, paasta_cmd, execute the add_subparser method
    implemented in paasta_cmd.py.

    Each paasta client command must implement a method called add_subparser.
    This allows the client to dynamically add subparsers to its subparser, which
    provides the benefits of argcomplete/argparse but gets it done in a modular
    fashion.

    :param command: a simple string - e.g. 'list'
    :param subparsers: an ArgumentParser object"""
    module_name = 'service_deployment_tools.paasta_cli.cmds.%s' % command
    add_subparser_fn = load_method(module_name, 'add_subparser')
    add_subparser_fn(subparsers)


def parse_args():
    """Initialize autocompletion and configure the argument parser.

    :return: an argparse.Namespace object mapping parameter names to the inputs
    from sys.argv"""
    parser = argparse.ArgumentParser(description="Yelp PaaSTA client")

    subparsers = parser.add_subparsers(help="[-h, --help] for subcommand help")

    for command in paasta_commands_dir(cmds):
        add_subparser(command, subparsers)

    argcomplete.autocomplete(parser)

    return parser.parse_args()


def main():
    """Perform a paasta call.  Read args from sys.argv and pass parsed args onto
    appropriate command in paata_cli/cmds directory"""

    args = parse_args()
    args.command(args)


if __name__ == '__main__':
    main()
