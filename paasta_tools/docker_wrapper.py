#!/usr/bin/env python
""" Meant to be used by mesos-slave instead of the /usr/bin/docker executable
directly This will parse the CLI arguments intended for docker, extract
environment variable settings related to the actual node hostname and mesos
task ID, and use those as an additional --hostname argument when calling the
underlying docker command.

If the environment variables are unspecified, or if --hostname is already
specified, this does not change any arguments and just directly calls docker
as-is.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import re
import socket
import sys


ENV_MATCH_RE = re.compile('^(-\w*e\w*|--env)(=(\S.*))?$')
MAX_HOSTNAME_LENGTH = 63


def parse_env_args(args):
    result = {}
    in_env = False
    for arg in args:
        if not in_env:
            match = ENV_MATCH_RE.match(arg)
            if not match:
                continue
            arg = match.group(3) or ''
            if not arg:
                in_env = True
                continue

        in_env = False
        if '=' not in arg:
            continue

        k, _, v = arg.partition('=')
        result[k] = v

    return result


def can_add_hostname(args):
    # return False if --hostname is already specified or if --network=host
    for index, arg in enumerate(args):

        # Check for --hostname and variants
        if arg == '-h':
            return False
        if arg.startswith('--hostname'):
            return False
        if len(arg) > 1 and arg[0] == '-' and arg[1] != '-':
            # several short args
            arg = arg.partition('=')[0]
            if 'h' in arg:
                return False

        # Check for --network=host and variants
        if arg in ('--net=host', '--network=host'):
            return False
        try:
            if arg in ('--net', '--network') and args[index + 1] == 'host':
                return False
        except IndexError:
            pass

    return True


def generate_hostname(fqdn, mesos_task_id):
    host_hostname = fqdn.partition('.')[0]
    task_id = mesos_task_id.rpartition('.')[2]

    hostname = host_hostname + '-' + task_id

    # hostnames can only contain alphanumerics and dashes and must be no more
    # than 63 characters
    hostname = re.sub('[^a-zA-Z0-9-]+', '-', hostname)[:MAX_HOSTNAME_LENGTH]
    return hostname


def add_hostname(args, hostname):
    # Add --hostname argument immediately after 'run' command if it exists
    args = list(args)

    try:
        run_index = args.index('run')
    except ValueError:
        pass
    else:
        args.insert(run_index + 1, '--hostname=' + hostname)

    return args


def main(argv=None):
    argv = argv if argv is not None else sys.argv

    env_args = parse_env_args(argv)
    fqdn = socket.getfqdn()

    # Marathon sets MESOS_TASK_ID whereas Chronos sets mesos_task_id
    mesos_task_id = env_args.get('MESOS_TASK_ID') or env_args.get('mesos_task_id')

    if mesos_task_id and can_add_hostname(argv):
        hostname = generate_hostname(fqdn, mesos_task_id)
        argv = add_hostname(argv, hostname)

    os.execlp('docker', 'docker', *argv[1:])
