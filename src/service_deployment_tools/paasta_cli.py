#!/usr/bin/env python

# To enable autocompletion, run:
# eval "$(register-python-argcomplete paasta-cli.py)"
"""Usage: ./paasta-cli.py [options]

A paasta client to interface with the Yelp paasta stack.

Command line options:
[check | list] : name of paasta command to execute

"""
import argcomplete
import argparse


def commands():
    """
    :return: A list of commands that paasta-cli can execute
    """
    return ['check', 'list']


def get_services():
    """
    :return: a list of Yelp services that are currently running
    """
    # TODO: Replace with actual call that gets services
    return ["mock_service_1", "mock_service_2"]


def paasta_check():
    """
    Analyze the service in the PWD to determine if it is paasta ready
    """
    # TODO: Write this method
    print "Executing check"


def paasta_list():
    """
    Print a list of Yelp services currently running
    """
    for service in get_services():
        print service


def parse_args():
    parser = argparse.ArgumentParser(description="Yelp PAASTA client")

    parser.add_argument('cmd', help='paasta client command to execute',
                        choices=commands())

    argcomplete.autocomplete(parser)

    return parser.parse_args()


def main():

    args = parse_args()

    cmd = args.cmd

    if cmd == 'list':
        paasta_list()
    elif cmd == 'check':
        paasta_check()


if __name__ == '__main__':
    main()