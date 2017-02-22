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
MAX_HOSTNAME_LENGTH = 64


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


def already_has_hostname(args):
    for arg in args:
        if arg == '-h':
            return True
        if arg.startswith('--hostname'):
            return True
        if len(arg) > 1 and arg[0] == '-' and arg[1] != '-':
            # several short args
            arg = arg.partition('=')[0]
            if 'h' in arg:
                return True
    return False


def generate_hostname(fqdn, mesos_task_id):
    host_hostname = fqdn.partition('.')[0]
    task_id_no_spaces = mesos_task_id.partition(' ')[0]  # Chronos has spaces in MESOS_TASK_ID
    task_id = task_id_no_spaces.rpartition('.')[2]

    hostname = host_hostname + '-' + task_id
    return hostname[:MAX_HOSTNAME_LENGTH]


def add_hostname(args, hostname):
    try:
        run_index = args.index('run')
    except ValueError:
        return

    args.insert(run_index + 1, '--hostname=' + hostname)


def main():
    env_args = parse_env_args(sys.argv)
    fqdn = socket.getfqdn()
    mesos_task_id = env_args.get('MESOS_TASK_ID')

    if mesos_task_id and not already_has_hostname(sys.argv[1:]):
        hostname = generate_hostname(fqdn, mesos_task_id)
        add_hostname(sys.argv, hostname)

    os.execlp('docker', 'docker', *sys.argv[1:])
