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
from __future__ import print_function
from __future__ import unicode_literals

import logging
import os
import re
import socket
import sys

from paasta_tools.firewall import DEFAULT_SYNAPSE_SERVICE_DIR
from paasta_tools.firewall import prepare_new_container
from paasta_tools.mac_address import reserve_unique_mac_address
from paasta_tools.utils import DEFAULT_SOA_DIR


LOCK_DIRECTORY = '/var/run/paasta/mac-address'
ENV_MATCH_RE = re.compile('^(-\w*e\w*|--env(?P<file>-file)?)(=(?P<arg>\S.*))?$')
MAX_HOSTNAME_LENGTH = 63


def parse_env_args(args):
    result = {}
    in_env = False
    in_file = False
    for arg in args:
        if not in_env:
            match = ENV_MATCH_RE.match(arg)
            if not match:
                continue
            arg = match.group('arg') or ''
            in_file = bool(match.group('file'))
            if not arg:
                in_env = True
                continue

        in_env = False

        if in_file:
            result.update(read_env_file(arg))
            in_file = False
            continue

        try:
            k, v = arg.split('=', 1)
        except ValueError:
            continue

        result[k] = v

    return result


def read_env_file(filename):
    # Parse a file where each line is KEY=VALUE
    # return contents in dictionary form
    result = {}
    with open(filename) as f:
        for line in f:
            try:
                k, v = line.split('=', 1)
            except ValueError:
                continue
            result[k] = v.strip()
    return result


def can_add_hostname(args):
    # return False if --hostname is already specified or if --network=host
    if is_network_host(args):
        return False

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

    return True


def is_network_host(args):
    for index, arg in enumerate(args):
        # Check for --network=host and variants
        if arg in ('--net=host', '--network=host'):
            return True
        try:
            if arg in ('--net', '--network') and args[index + 1] == 'host':
                return True
        except IndexError:
            pass

    return False


def is_run(args):
    try:
        list(args).index('run')
        return True
    except ValueError:
        return False


def can_add_mac_address(args):
    # return False if --mac-address is already specified or if --network=host
    if is_network_host(args) or not is_run(args):
        return False

    for index, arg in enumerate(args):
        # Check for --mac-address
        if arg.startswith('--mac-address'):
            return False

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
        logging.warning('Error while trying to read cpuinfo')
        pass
    return cpumap


def get_numa_memsize(nb_nodes):
    # Return memory size in mB per NUMA node assuming memory is split evenly
    # TODO: calculate and return real memory map
    try:
        with open('/proc/meminfo', 'r') as f:
            for line in f:
                m = re.match('MemTotal:\s*(\d+)\skB', line)
                if m:
                    return int(m.group(1)) / 1024 / int(nb_nodes)
    except IOError:
        logging.warning('Error while trying to read meminfo')
        pass
    return 0


def arg_collision(new_args, current_args):
    # Returns True if one of the new arguments is already in the
    # current argument list.
    cur_arg_keys = []
    for c in current_args:
        cur_arg_keys.append(c.split('=')[0])
    return bool(set(new_args).intersection(set(cur_arg_keys)))


def is_numa_enabled():
    if os.path.exists('/proc/1/numa_maps'):
        return True
    else:
        logging.warning('The system does not support NUMA')
        return False


def get_cpu_requierements(env_args):
    # Ensure we return a float. If no requierements we return 0.0
    try:
        return float(env_args.get('MARATHON_APP_RESOURCE_CPUS'))
    except (ValueError, TypeError):
        logging.warning('Could not read {} as a float'.format(env_args.get('MARATHON_APP_RESOURCE_CPUS')))
        return 0.0


def get_mem_requierements(env_args):
    # Ensure we return a float. If no requierements we return 0.0
    try:
        return float(env_args.get('MARATHON_APP_RESOURCE_MEM'))
    except (ValueError, TypeError):
        logging.warning('Could not read {} as a float'.format(env_args.get('MARATHON_APP_RESOURCE_MEM')))
        return 0.0


def append_cpuset_args(argv, env_args):
    # Enable log messages to stderr
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)

    try:
        pinned_numa_node = int(env_args.get('PIN_TO_NUMA_NODE'))
    except (ValueError, TypeError):
        logging.error('Could not read PIN_TO_NUMA_NODE value as an int: {}'.format(
            env_args.get('PIN_TO_NUMA_NODE')))
        return argv

    cpumap = get_cpumap()

    if len(cpumap) < 1:
        logging.error('Less than 2 physical CPU detected')
        return argv
    if pinned_numa_node not in cpumap:
        logging.error('Specified NUMA node: {} does not exist on this system'.format(
            pinned_numa_node))
        return argv
    if arg_collision(['--cpuset-cpus', '--cpuset-mems'], argv):
        logging.error('--cpuset options are already set. Not overriding')
        return argv
    if not is_numa_enabled():
        logging.error('Could not detect NUMA on the system')
        return argv
    if len(cpumap[pinned_numa_node]) < get_cpu_requierements(env_args):
        logging.error('The NUMA node has less cores than requested')
        return argv
    if get_numa_memsize(len(cpumap)) <= get_mem_requierements(env_args):
        logging.error('Requested memory:{} MB does not fit in one NUMA node: {} MB'.format(
            get_mem_requierements(env_args), get_numa_memsize(len(cpumap))))
        return argv

    logging.info('Binding container to NUMA node {}'.format(pinned_numa_node))
    argv = add_argument(argv, ('--cpuset-cpus=' + ','.join(
        str(c) for c in cpumap[pinned_numa_node])))
    argv = add_argument(argv, ('--cpuset-mems=' + str(pinned_numa_node)))
    return argv


def main(argv=None):
    argv = argv if argv is not None else sys.argv

    env_args = parse_env_args(argv)

    if env_args.get('PIN_TO_NUMA_NODE'):
        argv = append_cpuset_args(argv, env_args)

    # Marathon sets MESOS_TASK_ID whereas Chronos sets mesos_task_id
    mesos_task_id = env_args.get('MESOS_TASK_ID') or env_args.get('mesos_task_id')

    if mesos_task_id and can_add_hostname(argv):
        hostname = generate_hostname(socket.getfqdn(), mesos_task_id)
        argv = add_argument(argv, '--hostname={}'.format(hostname))

    paasta_firewall = env_args.get('PAASTA_FIREWALL')
    if paasta_firewall and can_add_mac_address(argv):
        try:
            mac_address, lockfile = reserve_unique_mac_address(LOCK_DIRECTORY)
        except Exception as e:
            print('Unable to add mac address: {}'.format(e), file=sys.stderr)
        else:
            argv = add_argument(argv, '--mac-address={}'.format(mac_address))

            prepare_new_container(
                DEFAULT_SOA_DIR,
                DEFAULT_SYNAPSE_SERVICE_DIR,
                env_args['PAASTA_SERVICE'],
                env_args['PAASTA_INSTANCE'],
                mac_address)

    os.execlp('docker', 'docker', *argv[1:])
