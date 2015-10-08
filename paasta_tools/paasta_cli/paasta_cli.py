#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
"""A command line tool for viewing information from the PaaSTA stack."""
import argcomplete
import argparse
import psutil
import os
import signal
import contextlib

from paasta_tools.paasta_cli import cmds
from paasta_tools.paasta_cli.utils \
    import file_names_in_dir as paasta_commands_dir, load_method
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
    module_name = 'paasta_tools.paasta_cli.cmds.%s' % command
    add_subparser_fn = load_method(module_name, 'add_subparser')
    add_subparser_fn(subparsers)


def parse_args():
    """Initialize autocompletion and configure the argument parser.

    :return: an argparse.Namespace object mapping parameter names to the inputs
             from sys.argv
    """
    parser = argparse.ArgumentParser(description="Yelp PaaSTA client")

    subparsers = parser.add_subparsers(help="[-h, --help] for subcommand help")

    for command in sorted(paasta_commands_dir(cmds)):
        add_subparser(command, subparsers)

    argcomplete.autocomplete(parser)

    return parser.parse_args()


@contextlib.contextmanager
def set_pgrp_and_cleanup_procs_on_exit():
    """
        Set the pgrp of the process and all children.
        After the task completes, cleanup any other processes in the
        same pgrp.
    """
    try:
        os.setpgrp()
        kill_pgrp_when_finished = True
    except OSError:
        # Per http://linux.die.net/man/2/setpgid
        # if we could not set our process group, that means we are not
        # in charge of this session and we should not kill everything
        # in our process group. This might happen if our parent process
        # already ran setpgrp, setpgid, or execve.
        kill_pgrp_when_finished = False
        pass

    try:
        yield
    finally:
        if kill_pgrp_when_finished:
            try:
                pgrp = os.getpgrp()
                pids_in_pgrp = [proc for proc in psutil.process_iter() if os.getpgid(proc.pid) == pgrp
                                and proc.pid != os.getpid()]
                for proc in pids_in_pgrp:
                    os.kill(proc.pid, signal.SIGTERM)
            except OSError:
                pass


def main():
    """Perform a paasta call. Read args from sys.argv and pass parsed args onto
    appropriate command in paata_cli/cmds directory.

    Ensure we kill any child pids before we quit
    """
    configure_log()
    args = parse_args()
    with set_pgrp_and_cleanup_procs_on_exit():
        args.command(args)

if __name__ == '__main__':
    main()
