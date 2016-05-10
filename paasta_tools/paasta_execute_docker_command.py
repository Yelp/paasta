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
"""
Usage: ./paasta_execute_docker_command.py [options]

This script will attempt to find a running container that contains an environment variable matching
the specified Mesos task ID and then execute the given command with the specified timeout. The script will
print the output of the command and exit with the same return code as the command.

Command line options:

- -i <MESOS_TASK_ID>, --mesos-id <MESOS_TASK_ID>: Specify a Mesos task ID to search for
- -c <command>, --cmd <command>: Shell command to execute in container
- -t <timeout>, --timeout <timeout>: Timeout for command
"""
import argparse
import signal
import sys
from contextlib import contextmanager

from paasta_tools.mesos_tools import get_container_id_for_mesos_id
from paasta_tools.utils import get_docker_client


def parse_args():
    parser = argparse.ArgumentParser(description='Executes given command in Docker container for given Mesos task ID')
    parser.add_argument('-i', '--mesos-id', required=True, help="Mesos task ID")
    parser.add_argument('-c', '--cmd', required=True, help="command to execute in container")
    parser.add_argument('-t', '--timeout', default=45, type=int, help="timeout for command")
    args = parser.parse_args()
    return args


class TimeoutException(Exception):
    pass


@contextmanager
def time_limit(seconds):  # From http://stackoverflow.com/a/601168/1576438
    def signal_handler(signum, frame):
        raise TimeoutException('Timed out!')
    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)


def execute_in_container(docker_client, container_id, cmd, timeout):
    container_info = docker_client.inspect_container(container_id)
    if container_info['ExecIDs'] and len(container_info['ExecIDs']) > 0:
        for possible_exec_id in container_info['ExecIDs']:
            exec_info = docker_client.exec_inspect(possible_exec_id)['ProcessConfig']
            if exec_info['entrypoint'] == '/bin/sh' and exec_info['arguments'] == ['-c', cmd]:
                exec_id = possible_exec_id
                break
    else:
        exec_id = docker_client.exec_create(container_id, ['/bin/sh', '-c', cmd])['Id']
    output = docker_client.exec_start(exec_id, stream=False)
    return_code = docker_client.exec_inspect(exec_id)['ExitCode']
    return (output, return_code)


def main():
    args = parse_args()

    if not args.mesos_id:
        sys.stdout.write(
            "The Mesos task id you supplied seems to be an empty string! Please provide a valid task id.\n")
        sys.exit(2)

    docker_client = get_docker_client()

    container_id = get_container_id_for_mesos_id(docker_client, args.mesos_id)

    if container_id:
        try:
            with time_limit(args.timeout):
                output, return_code = execute_in_container(docker_client, container_id, args.cmd, args.timeout)
            sys.stdout.write(output)
        except TimeoutException:
            sys.stdout.write("Command timed out!\n")
            return_code = 1
        finally:
            sys.exit(return_code)
    else:
        sys.stdout.write("Could not find container with MESOS_TASK_ID '%s'.\n" % args.mesos_id)
        sys.exit(1)

if __name__ == "__main__":
    main()
