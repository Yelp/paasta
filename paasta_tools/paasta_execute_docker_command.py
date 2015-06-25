#!/usr/bin/env python
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
from contextlib import contextmanager
import signal
import sys

from docker import Client

from paasta_tools.utils import get_docker_host


def parse_args():
    parser = argparse.ArgumentParser(description='Executes given command in Docker container for given Mesos task ID')
    parser.add_argument('-i', '--mesos-id', required=True, help="Mesos task ID")
    parser.add_argument('-c', '--cmd', required=True, help="command to execute in container")
    parser.add_argument('-t', '--timeout', default=45, type=int, help="timeout for command")
    args = parser.parse_args()
    return args


def get_container_id_for_mesos_id(client, mesos_task_id):
    running_containers = client.containers()

    container_id = None
    for container in running_containers:
        info = client.inspect_container(container)
        for env_var in info['Config']['Env']:
            if ('MESOS_TASK_ID=%s' % mesos_task_id) in env_var:
                container_id = info['Id']
                break

    return container_id


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
    exec_id = docker_client.exec_create(container_id, "/bin/sh -c '%s'" % cmd)['Id']
    output = docker_client.exec_start(exec_id, stream=False)
    return_code = docker_client.exec_inspect(exec_id)['ExitCode']
    return (output, return_code)


def main():
    args = parse_args()

    base_docker_url = get_docker_host()
    docker_client = Client(base_url=base_docker_url)

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
