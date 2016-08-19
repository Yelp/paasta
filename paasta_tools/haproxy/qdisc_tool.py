# -*- coding: utf8 -*-
""" Command line interface for working with qdiscs """
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import os
import subprocess
import sys
from pwd import getpwnam

from paasta_tools.haproxy.qdisc_util import check_setup
from paasta_tools.haproxy.qdisc_util import clear
from paasta_tools.haproxy.qdisc_util import manage_plug
from paasta_tools.haproxy.qdisc_util import needs_setup
from paasta_tools.haproxy.qdisc_util import setup
from paasta_tools.haproxy.qdisc_util import stat


log = logging.getLogger(__name__)

# We run haproxy on localhost (yocalhost share's the lo interface)
INTERFACE_NAME = 'lo'
# Traffic comes from the yocalhost IP
SOURCE_IP = '169.254.255.254'

# Log format for logging to console
CONSOLE_FORMAT = '%(asctime)s - %(name)-12s: %(levelname)-8s %(message)s'


def stat_cmd(args):
    return stat(INTERFACE_NAME)


def check_setup_cmd(args):
    return check_setup(INTERFACE_NAME)


def manage_plug_cmd(args):
    if args.action == 'plug':
        manage_plug(INTERFACE_NAME, enable_plug=True)
    elif args.action == 'unplug':
        manage_plug(INTERFACE_NAME, enable_plug=False)
    else:
        return 1
    return 0


def needs_setup_cmd(args):
    return needs_setup(INTERFACE_NAME)


def setup_cmd(args):
    return setup(INTERFACE_NAME, SOURCE_IP)


def clear_cmd(args):
    return clear(INTERFACE_NAME, SOURCE_IP)


def drop_perms():
    user = getpwnam(os.environ.get('SUDO_USER', 'nobody'))
    uid = user.pw_uid
    gid = user.pw_gid

    os.setgroups([])
    os.setgid(gid)
    os.setuid(uid)


def protect_call_cmd(args):
    if os.getuid() != 0:
        print('Only root can execute protected binaries')
        return 1

    try:
        try:
            manage_plug(INTERFACE_NAME, enable_plug=True)
        except:
            # If we fail to plug, it is no big deal, we might
            # drop some traffic but let's not fail to run the
            # command
            log.exception('Failed to enable plug')
        subprocess.check_call(
            [args.cmd] + args.args,
            preexec_fn=drop_perms
        )
    finally:
        # Netlink comms can be unreliable according to the manpage,
        # so do some retries to ensure we really turn off the plug
        # It would be really bad if we do not turn off the plug
        for i in range(3):
            try:
                manage_plug(INTERFACE_NAME, enable_plug=False)
                break
            except:
                log.exception('Failed to disable plug, try #%d' % i)


def parse_options():
    parser = argparse.ArgumentParser(epilog=(
        'Setup QoS queueing disciplines for haproxy'
    ))
    parser.add_argument('--verbose', '-v', action='store_true')
    subparsers = parser.add_subparsers()

    stat_parser = subparsers.add_parser(
        'stat', help='Show current qdisc and iptables setup')
    stat_parser.set_defaults(func=stat_cmd)

    check_parser = subparsers.add_parser(
        'check', help='Check qdisc and iptables are as expected')
    check_parser.set_defaults(func=check_setup_cmd)

    needs_setup_parser = subparsers.add_parser(
        'needs_setup', help='Check if qdisc and iptables need setup')
    needs_setup_parser.set_defaults(func=needs_setup_cmd)

    setup_parser = subparsers.add_parser(
        'setup', help='Setup the qdisc')
    setup_parser.set_defaults(func=setup_cmd)

    clear_parser = subparsers.add_parser(
        'clear', help='Clear the qdisc and iptables')
    clear_parser.set_defaults(func=clear_cmd)

    plug_parser = subparsers.add_parser(
        'manage_plug', help='Manage the plug lane')
    plug_parser.add_argument(
        'action', choices=('plug', 'unplug'),
        help='Plug or unplug traffic on the plug qdisc')
    plug_parser.set_defaults(func=manage_plug_cmd)

    protect_parser = subparsers.add_parser(
        'protect', help='Run a command while network traffic is blocked')
    protect_parser.add_argument(
        dest='cmd', help='Command to run while traffic is blocked')
    protect_parser.add_argument(
        'args', nargs=argparse.REMAINDER)
    protect_parser.set_defaults(func=protect_call_cmd)

    return parser.parse_args()


def setup_logging(args):
    if args.verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(level=level, format=CONSOLE_FORMAT)


def main():
    args = parse_options()
    setup_logging(args)
    sys.exit(args.func(args))


if __name__ == '__main__':
    main()
