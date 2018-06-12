#!/usr/bin/env python
import argparse
import os
import socket

from paasta_tools.utils import atomic_file_write
from paasta_tools.utils import get_docker_client

HAPROXY_STATS_SOCKET = '/var/run/synapse/haproxy.sock'


def get_prev_file_contents(filename):
    if os.path.isfile(filename):
        with open(filename, 'r') as fp:
            prev_lines = [
                # Remove any empty strings, since split could leave empty
                # strings if there is any extra whitespace in the file
                list(filter(None, line.strip().split(' ')))
                for line
                in fp.readlines()
            ]
            return {line[0]: line[1] for line in prev_lines}
    return {}


def extract_taskid_and_ip(docker_client):
    service_ips_and_ids = []
    for container in docker_client.containers():
        networks = container['NetworkSettings']['Networks']
        labels = container['Labels']

        # Only add containers that are using bridged networking and are
        # running as Mesos tasks
        if 'bridge' in networks:
            ip_addr = networks['bridge']['IPAddress']
            if 'MESOS_TASK_ID' in labels:
                task_id = labels['MESOS_TASK_ID']
                service_ips_and_ids.append((ip_addr, task_id))
            # For compatibility with tron/batch services.
            elif 'paasta_instance' in labels and 'paasta_service' in labels:
                task_id = '{}.{}'.format(
                    labels['paasta_service'],
                    labels['paasta_instance'],
                )
                # For compatibility with MESOS_TASK_ID format.
                task_id = task_id.replace('_', '--')
                service_ips_and_ids.append((ip_addr, task_id))
    return service_ips_and_ids


def send_to_haproxy(command):
    s = socket.socket(socket.AF_UNIX)
    # 1 seconds should be more than enough of a timeout since HAProxy is local
    s.settimeout(1)
    s.connect(HAPROXY_STATS_SOCKET)
    s.send((command + '\n').encode())
    s.close()


def update_haproxy_mapping(ip_addr, task_id, prev_ip_to_task_id, filename):
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
            filename,
            ip_addr,
            task_id,
        ))


def remove_stopped_container_entries(prev_ips, curr_ips, filename):
    for ip in prev_ips:
        if ip not in curr_ips:
            send_to_haproxy('del map {} {}'.format(
                filename,
                ip,
            ))


def main():
    parser = argparse.ArgumentParser(
        description=(
            'Script to dump a HAProxy map between container IPs and task IDs.'
        ),
    )
    parser.add_argument(
        'map_file',
        nargs='?',
        default='/var/run/synapse/maps/ip_to_service.map',
        help='Where to write the output map file',
    )
    args = parser.parse_args()

    prev_ip_to_task_id = get_prev_file_contents(args.map_file)

    new_lines = []
    ip_addrs = []
    service_ips_and_ids = extract_taskid_and_ip(get_docker_client())

    for ip_addr, task_id in service_ips_and_ids:
        ip_addrs.append(ip_addr)
        update_haproxy_mapping(
            ip_addr,
            task_id,
            prev_ip_to_task_id,
            args.map_file,
        )
        new_lines.append(f'{ip_addr} {task_id}')

    remove_stopped_container_entries(
        prev_ip_to_task_id.keys(),
        ip_addrs,
        args.map_file,
    )

    # Replace the file contents with the new map
    with atomic_file_write(args.map_file) as fp:
        fp.write('\n'.join(new_lines))


if __name__ == "__main__":
    main()
