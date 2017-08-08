#!/usr/bin/env python
import argparse
import os
import socket

from paasta_tools.utils import get_docker_client

HAPROXY_STATS_SOCKET = '/var/run/synapse/haproxy.sock'


def send_to_haproxy(command):
    s = socket.socket(socket.AF_UNIX)
    # 1 seconds should be more than enough of a timeout since HAProxy is local
    s.settimeout(1)
    s.connect(HAPROXY_STATS_SOCKET)
    print("Sending {} to HAProxy stats socket".format(command))
    s.send((command + '\n').encode())
    file_handle = s.makefile()
    response = file_handle.read().splitlines()
    print("Response: {}".format(response))
    s.close()


def main():
    parser = argparse.ArgumentParser(
        description=(
            'Script to dump a HAProxy map between container IPs and task IDs.'
        ),
    )
    parser.add_argument(
        'map_file', nargs='?', default='/var/run/synapse/maps/ip_to_service.map',
        help='Where to write the output map file',
    )
    args = parser.parse_args()

    client = get_docker_client()

    file_exists = os.path.isfile(args.map_file)
    if file_exists:
        mode = 'r+'
    else:
        mode = 'w'

    with open(args.map_file, mode) as f:
        if file_exists:
            prev_lines = [line.strip().split(' ') for line in f.readlines()]
            prev_ip_to_task_id = {line[0]: line[1] for line in prev_lines}
        else:
            prev_ip_to_task_id = {}

        print("Previous file: {}".format(prev_ip_to_task_id))

        new_lines = []
        ip_addrs = []
        for container in client.containers():
            networks = container['NetworkSettings']['Networks']
            labels = container['Labels']

            # Only add containers that are using bridged networking and are
            # running in Mesos
            if 'bridge' in networks and 'MESOS_TASK_ID' in labels:
                ip_addr = networks['bridge']['IPAddress']
                task_id = labels['MESOS_TASK_ID']
                ip_addrs.append(ip_addr)

                # Check if this IP was in the file previously, if so, we want
                # to send an update to the HAProxy map instead of adding a new
                # entry (new additions to the map don't overwrite old entries
                # and instead create duplicate keys with different values).
                if ip_addr in prev_ip_to_task_id:
                    if prev_ip_to_task_id[ip_addr] != task_id:
                        method = 'set'
                    else:
                        method = None
                else:
                    # The IP was not added previously, add it as a new entry
                    method = 'add'

                if method:
                    send_to_haproxy('{} map {} {} {}'.format(
                        method,
                        args.map_file,
                        ip_addr,
                        task_id,
                    ))

                new_lines.append('{} {}'.format(ip_addr, task_id))

        for ip_addr in prev_ip_to_task_id.keys():
            # Remove any keys for containers that are no longer running
            if ip_addr not in ip_addrs:
                send_to_haproxy('del map {} {}'.format(
                    args.map_file,
                    ip_addr,
                ))

        # Replace the file contents with the new map
        if file_exists:
            f.seek(0)
            f.truncate()
        f.write('\n'.join(new_lines))


if __name__ == "__main__":
    main()
