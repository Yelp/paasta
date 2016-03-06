#!/usr/bin/env python
# Copyright 2015 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# PYTHON_ARGCOMPLETE_OK
"""A command line tool for viewing information from the PaaSTA stack."""
import argparse
import sys

import argcomplete
import pkg_resources

from paasta_tools.cli import cmds
from paasta_tools.cli.utils import load_method
from paasta_tools.cli.utils import modules_in_pkg as paasta_commands_dir
from paasta_tools.utils import configure_log


def add_subparser(command, subparsers):
    """Given a command name, paasta_cmd, execute the add_subparser method
    implemented in paasta_cmd.py.

    Each paasta client command must implement a method called add_subparser.
    This allows the client to dynamically add subparsers to its subparser, which
    provides the benefits of argcomplete/argparse but gets it done in a modular
    fashion.

    :param command: a simple string - e.g. 'list'
    :param subparsers: an ArgumentParser object"""
    module_name = 'paasta_tools.cli.cmds.%s' % command
    add_subparser_fn = load_method(module_name, 'add_subparser')
    add_subparser_fn(subparsers)


def get_argparser():
    parser = argparse.ArgumentParser(
        description=(
            "The PaaSTA command line tool. The 'paasta' command is the entry point "
            "to multiple subcommands, see below.\n\n"
            "You can see more help for individual commands by appending them with '--help', "
            "for example, 'paasta status --help' or see the man page with 'man paasta status'."
        ),
        epilog=(
            "The 'paasta' command line tool is designed to be used by humans, and therefore has "
            "command line completion for almost all options and uses pretty formatting when "
            "possible."
        ),
    )

    # http://stackoverflow.com/a/8521644/812183
    parser.add_argument(
        '-V', '--version',
        action='version',
        version='paasta-tools {0}'.format(
            pkg_resources.get_distribution('paasta-tools').version
        )
    )

    subparsers = parser.add_subparsers(help="[-h, --help] for subcommand help")

    for command in sorted(paasta_commands_dir(cmds)):
        add_subparser(command, subparsers)

    return parser


def parse_args(argv):
    """Initialize autocompletion and configure the argument parser.

    :return: an argparse.Namespace object mapping parameter names to the inputs
             from sys.argv
    """
    parser = get_argparser()
    argcomplete.autocomplete(parser)

    return parser.parse_args(argv)


def main(argv=None):
    """Perform a paasta call. Read args from sys.argv and pass parsed args onto
    appropriate command in paata_cli/cmds directory.

    Ensure we kill any child pids before we quit
    """
    configure_log()
    args = parse_args(argv)
    return_code = args.command(args)
    sys.exit(return_code)

if __name__ == '__main__':
    main()
