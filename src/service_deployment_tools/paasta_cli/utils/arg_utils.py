#!/usr/bin/env python
"""
Contains helper functions for argparse and argcomplete
"""


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
    module_name = 'cmds.%s' % command
    add_subparser_fn = load_method(module_name, 'add_subparser')
    add_subparser_fn(subparsers)
