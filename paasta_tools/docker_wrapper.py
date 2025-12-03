#!/usr/bin/env python
# flake8: noqa: E402
""" Meant to be used by mesos-slave instead of the /usr/bin/docker executable
directly This will parse the CLI arguments intended for docker, extract
environment variable settings related to the actual node hostname and mesos
task ID, and use those as an additional --hostname argument when calling the
underlying docker command.

If the environment variables are unspecified, or if --hostname is already
specified, this does not change any arguments and just directly calls docker
as-is.
"""
import logging
import os
import re
import socket
import sys


if "PATH" not in os.environ:
    # This command is sometimes executed in a sanitized environment
    # which does not have the path, which causes the following imports
    # to fail.
    # To compensate, we set a minimal path to get off the ground.
    os.environ["PATH"] = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"


LOCK_DIRECTORY = "/var/lib/paasta/mac-address"
ENV_MATCH_RE = re.compile(r"^(-\w*e\w*|--env(?P<file>-file)?)(=(?P<arg>\S.*))?$")
MAX_HOSTNAME_LENGTH = 60


def parse_env_args(args):
    result = dict(os.environ.items())
    in_env = False
    in_file = False
    for arg in args:
        if not in_env:
            match = ENV_MATCH_RE.match(arg)
            if not match:
                continue
            arg = match.group("arg") or ""
            in_file = bool(match.group("file"))
            if not arg:
                in_env = True
                continue

        in_env = False

        if in_file:
            result.update(read_env_file(arg))
            in_file = False
            continue

        try:
            k, v = arg.split("=", 1)
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
                k, v = line.split("=", 1)
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
        if arg == "-h":
            return False
        if arg.startswith("--hostname"):
            return False
        if len(arg) > 1 and arg[0] == "-" and arg[1] != "-":
            # several short args
            arg = arg.partition("=")[0]
            if "h" in arg:
                return False

    return True


def is_network_host(args):
    for index, arg in enumerate(args):
        # Check for --network=host and variants
        if arg in ("--net=host", "--network=host"):
            return True
        try:
            if arg in ("--net", "--network") and args[index + 1] == "host":
                return True
        except IndexError:
            pass

    return False


def is_run(args):
    try:
        list(args).index("run")
        return True
    except ValueError:
        return False


def can_add_mac_address(args):
    # return False if --mac-address is already specified or if --network=host
    if is_network_host(args) or not is_run(args):
        return False

    for index, arg in enumerate(args):
        # Check for --mac-address
        if arg.startswith("--mac-address"):
            return False

    return True


def generate_hostname_task_id(hostname, mesos_task_id):
    task_id = mesos_task_id.rpartition(".")[2]

    hostname_task_id = hostname + "-" + task_id

    # hostnames can only contain alphanumerics and dashes and must be no more
    # than 60 characters
    hostname_task_id = re.sub("[^a-zA-Z0-9-]+", "-", hostname_task_id)[
        :MAX_HOSTNAME_LENGTH
    ]

    # hostnames can also not end with dashes as per RFC952
    hostname_task_id = hostname_task_id.rstrip("-")

    return hostname_task_id


def add_argument(args, argument):
    # Add an argument immediately after 'run' command if it exists
    args = list(args)
    try:
        run_index = args.index("run")
    except ValueError:
        pass
    else:
        args.insert(run_index + 1, argument)
    return args


def arg_collision(new_args, current_args):
    # Returns True if one of the new arguments is already in the
    # current argument list.
    cur_arg_keys = []
    for c in current_args:
        cur_arg_keys.append(c.split("=")[0])
    return bool(set(new_args).intersection(set(cur_arg_keys)))


def add_firewall(argv, service, instance):
    # Delayed import to improve performance when add_firewall is not used
    from paasta_tools.docker_wrapper_imports import DEFAULT_SYNAPSE_SERVICE_DIR
    from paasta_tools.docker_wrapper_imports import firewall_flock
    from paasta_tools.docker_wrapper_imports import prepare_new_container
    from paasta_tools.docker_wrapper_imports import reserve_unique_mac_address
    from paasta_tools.docker_wrapper_imports import DEFAULT_SOA_DIR

    output = ""
    try:
        mac_address, lockfile = reserve_unique_mac_address(LOCK_DIRECTORY)
    except Exception as e:
        output = f"Unable to add mac address: {e}"
    else:
        argv = add_argument(argv, f"--mac-address={mac_address}")
        try:

            with firewall_flock():
                prepare_new_container(
                    DEFAULT_SOA_DIR,
                    DEFAULT_SYNAPSE_SERVICE_DIR,
                    service,
                    instance,
                    mac_address,
                )
        except Exception as e:
            output = f"Unable to add firewall rules: {e}"

    if output:
        print(output, file=sys.stderr)

    return argv


def main(argv=None):
    argv = argv if argv is not None else sys.argv

    env_args = parse_env_args(argv)

    # Marathon sets MESOS_TASK_ID
    mesos_task_id = env_args.get("MESOS_TASK_ID")

    fqdn = socket.getfqdn()
    hostname = fqdn.partition(".")[0]
    if mesos_task_id and can_add_hostname(argv):
        argv = add_argument(argv, f"-e=PAASTA_HOST={fqdn}")
        hostname_task_id = generate_hostname_task_id(hostname, mesos_task_id)
        argv = add_argument(argv, f"--hostname={hostname_task_id }")
    elif can_add_hostname(argv):
        argv = add_argument(argv, f"-e=PAASTA_HOST={fqdn}")
        argv = add_argument(argv, f"--hostname={hostname}")

    paasta_firewall = env_args.get("PAASTA_FIREWALL")
    service = env_args.get("PAASTA_SERVICE")
    instance = env_args.get("PAASTA_INSTANCE")
    if paasta_firewall and service and instance and can_add_mac_address(argv):
        try:
            argv = add_firewall(argv, service, instance)
        except Exception as e:
            print(f"Unhandled exception in add_firewall: {e}", file=sys.stderr)

    os.execlp("docker", "docker", *argv[1:])
