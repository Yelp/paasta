#!/usr/bin/env python
# Copyright 2015-2016 Yelp Inc.
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
import logging
import os
import pkgutil
import subprocess
import sys
import warnings
from typing import Any
from typing import List
from typing import Tuple

import argcomplete

import paasta_tools
from paasta_tools.cli import cmds


def load_method(module_name, method_name):
    """Return a function given a module and method name.

    :param module_name: a string
    :param method_name: a string
    :return: a function
    """
    module = __import__(module_name, fromlist=[method_name])
    method = getattr(module, method_name)
    return method


def modules_in_pkg(pkg):
    """Return the list of modules in a python package (a module with a
    __init__.py file.)

    :return: a list of strings such as `['list', 'check']` that correspond to
             the module names in the package.
    """
    for _, module_name, _ in pkgutil.walk_packages(pkg.__path__):
        yield module_name


class PrintsHelpOnErrorArgumentParser(argparse.ArgumentParser):
    """Overriding the error method allows us to print the whole help page,
    otherwise the python arg parser prints a not-so-useful usage message that
    is way too terse"""

    def error(self, message):
        print(f"Argument parse error: {message}\n")
        self.print_help()
        sys.exit(1)


def list_external_commands():
    p = subprocess.check_output(["/bin/bash", "-p", "-c", "compgen -A command paasta-"])
    lines = p.decode("utf-8").strip().split("\n")
    return {l.replace("paasta-", "", 1) for l in lines}


def calling_external_command():
    if len(sys.argv) > 1:
        return sys.argv[1] in list_external_commands()
    else:
        return False


def exec_subcommand(argv):
    command = sys.argv[1]
    os.execlp(f"paasta-{command}", *argv[1:])


def add_subparser(command, subparsers):
    """Given a command name, paasta_cmd, execute the add_subparser method
    implemented in paasta_cmd.py.

    Each paasta client command must implement a method called add_subparser.
    This allows the client to dynamically add subparsers to its subparser, which
    provides the benefits of argcomplete/argparse but gets it done in a modular
    fashion.

    :param command: a simple string - e.g. 'list'
    :param subparsers: an ArgumentParser object"""
    module_name = "paasta_tools.cli.cmds.%s" % command
    add_subparser_fn = load_method(module_name, "add_subparser")
    add_subparser_fn(subparsers)


PAASTA_SUBCOMMANDS = {
    "autoscale": "autoscale",
    "boost": "boost",
    "check": "check",
    "cook-image": "cook_image",
    "get-docker-image": "get_docker_image",
    "get-image-version": "get_image_version",
    "get-latest-deployment": "get_latest_deployment",
    "info": "info",
    "itest": "itest",
    "list-clusters": "list_clusters",
    "list-deploy-queue": "list_deploy_queue",
    "list": "list",
    "local-run": "local_run",
    "logs": "logs",
    "mark-for-deployment": "mark_for_deployment",
    "mesh-status": "mesh_status",
    "metastatus": "metastatus",
    "pause_service_autoscaler": "pause_service_autoscaler",
    "performance-check": "performance_check",
    "push-to-registry": "push_to_registry",
    "remote-run": "remote_run",
    "rollback": "rollback",
    "secret": "secret",
    "security-check": "security_check",
    "spark-run": "spark_run",
    "start": "start_stop_restart",
    "stop": "start_stop_restart",
    "restart": "start_stop_restart",
    "status": "status",
    "sysdig": "sysdig",
    "validate": "validate",
    "wait-for-deployment": "wait_for_deployment",
}


def get_argparser(commands=None):
    """Create and return argument parser for a set of subcommands.

    :param commands: Union[None, List[str]] If `commands` argument is `None`,
    add full parsers for all subcommands, if `commands` is empty list -
    add thin parsers for all subcommands, otherwise - add full parsers for
    subcommands in the argument.
    """

    parser = PrintsHelpOnErrorArgumentParser(
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
        # Suppressing usage prevents it from being printed twice upon print_help
        usage=argparse.SUPPRESS,
    )

    # http://stackoverflow.com/a/8521644/812183
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"paasta-tools {paasta_tools.__version__}",
    )

    subparsers = parser.add_subparsers(dest="command", metavar="")
    subparsers.required = True

    # Adding a separate help subparser allows us to respond to "help" without --help
    help_parser = subparsers.add_parser(
        "help", help=f"run `paasta <subcommand> -h` for help"
    )
    help_parser.set_defaults(command=None)

    # Build a list of subcommands to add them in alphabetical order later
    command_choices: List[Tuple[str, Any]] = []
    if commands is None:
        for command in sorted(modules_in_pkg(cmds)):
            command_choices.append(
                (command, (add_subparser, [command, subparsers], {}))
            )
    elif commands:
        for command in commands:
            if command not in PAASTA_SUBCOMMANDS:
                # could be external subcommand
                continue
            command_choices.append(
                (
                    command,
                    (add_subparser, [PAASTA_SUBCOMMANDS[command], subparsers], {}),
                )
            )
    else:
        for command in PAASTA_SUBCOMMANDS.keys():
            command_choices.append(
                (
                    command,
                    (subparsers.add_parser, [command], dict(help="", add_help=False)),
                )
            )

    for command in list_external_commands():
        command_choices.append(
            (command, (subparsers.add_parser, [command], dict(help="")))
        )

    for (_, (fn, args, kwds)) in sorted(command_choices, key=lambda e: e[0]):
        fn(*args, **kwds)

    return parser


def parse_args(argv):
    """Initialize autocompletion and configure the argument parser.

    :return: an argparse.Namespace object mapping parameter names to the inputs
             from sys.argv
    """
    parser = get_argparser(commands=[])
    argcomplete.autocomplete(parser)

    args, _ = parser.parse_known_args(argv)
    if args.command:
        parser = get_argparser(commands=[args.command])

    argcomplete.autocomplete(parser)
    return parser.parse_args(argv), parser


def main(argv=None):
    """Perform a paasta call. Read args from sys.argv and pass parsed args onto
    appropriate command in paasta_cli/cmds directory.

    Ensure we kill any child pids before we quit
    """
    logging.basicConfig()
    warnings.filterwarnings("ignore", category=DeprecationWarning)

    # if we are an external command, we need to exec out early.
    # The reason we exec out early is so we don't bother trying to parse
    # "foreign" arguments, which would cause a stack trace.
    if calling_external_command():
        exec_subcommand(sys.argv)

    try:
        args, parser = parse_args(argv)
        if args.command is None:
            parser.print_help()
            return_code = 0
        else:
            return_code = args.command(args)
    except KeyboardInterrupt:
        return_code = 1
    sys.exit(return_code)


if __name__ == "__main__":
    main()
