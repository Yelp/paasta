#!/usr/bin/env python
""" Meant to be used by mesos-slave instead of the /usr/bin/docker executable
directly This will parse the CLI arguments intended for docker, extract
environment variable settings related to the actual node hostname and mesos
task ID, and use those as an additional --hostname argument when calling the
underlying docker command.

If the environment variables are unspecified, or if --hostname is already
specified, this does not change any arguments and just directly calls docker
as-is.

Additionally this wrapper will look for the environment variable
PIN_TO_NUMA_NODE which contains the physical CPU and memory to restrict the
container to. If the system is NUMA enabled, docker will be called with the
arguments cpuset-cpus and cpuset-mems.
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


def add_argument(args, argument):
    # Add an argument immediately after 'run' command if it exists
    args = list(args)
    try:
        run_index = args.index('run')
    except ValueError:
        pass
    else:
        args.insert(run_index + 1, argument)
    return args


def get_cpumap():
    # Return a dict containing the core numbers per physical CPU
    core = 0
    cpumap = {}
    try:
        with open('/proc/cpuinfo', 'r') as f:
            for line in f:
                m = re.match('physical\sid.*(\d)', line)
                if m:
                    cpuid = int(m.group(1))
                    if cpuid not in cpumap:
                        cpumap[cpuid] = []
                    cpumap[cpuid].append(core)
                    core += 1
    except IOError:
        pass
    return cpumap


def is_numa_enabled():
    return os.path.exists('/proc/1/numa_maps')


def arg_collision(new_args, current_args):
    # Returns True if one of the new arguments is already in the
    # current argument list.
    cur_arg_keys = []
    for c in current_args:
        cur_arg_keys.append(c.split('=')[0])
    return bool(set(new_args).intersection(set(cur_arg_keys)))


def validate_int(i):
    try:
        return int(i)
    except (ValueError, TypeError):
        return None


def validate_float(f):
    # Ensure we return a float. If input is invalid we return 0.0
    try:
        return float(f)
    except (ValueError, TypeError):
        return 0.0


def generate_cpuset_args(env_args, pinned_numa_node):
    required_cores = validate_float(env_args.get('MARATHON_APP_RESOURCE_CPUS'))
    cpumap = get_cpumap()
    result = []

    # No need to set up docker cpuset options if one of the above fails:
    if (
        # NUMA is supported by the system
        is_numa_enabled() and
        # Machine has multiple CPUs
        len(cpumap) > 1 and
        # Requested numa node exists
        pinned_numa_node in cpumap and
        # The numa node has more cores than the container requires
        len(cpumap[pinned_numa_node]) >= required_cores
    ):
        result.append('--cpuset-cpus=' + ','.join(str(c) for c in cpumap[pinned_numa_node]))
        result.append('--cpuset-mems=' + str(pinned_numa_node))

    return result


def main(argv=None):
    argv = argv if argv is not None else sys.argv

    # Get Docker env variables
    env_args = parse_env_args(argv)

    # Marathon sets MESOS_TASK_ID whereas Chronos sets mesos_task_id
    mesos_task_id = env_args.get('MESOS_TASK_ID') or env_args.get('mesos_task_id')

    # Check if we require this container to stick to a given NUMA node
    pinned_numa_node = validate_int(env_args.get('PIN_TO_NUMA_NODE'))

    if pinned_numa_node and not arg_collision(['--cpuset-cpus', '--cpuset-mems'], argv):
        for c in generate_cpuset_args(env_args, pinned_numa_node):
            argv = add_argument(argv, c)

    if mesos_task_id and not already_has_hostname(argv):
        hostname = generate_hostname(socket.getfqdn(), mesos_task_id)
        argv = add_argument(argv, '--hostname=' + hostname)

    os.execlp('docker', 'docker', *argv[1:])
