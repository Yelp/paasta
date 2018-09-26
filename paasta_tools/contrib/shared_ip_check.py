#!/usr/bin/env python3.6
import sys
from collections import defaultdict

import iptc

from paasta_tools import iptables
from paasta_tools.utils import get_docker_client


def list_docker_nat_rules():
    chain_name = 'DOCKER'
    table = iptc.Table(iptc.Table.NAT)
    chain = iptc.Chain(table, chain_name)
    for rule in chain.rules:
        yield iptables.Rule.from_iptc(rule)


def main():
    docker_client = get_docker_client()
    ip_to_containers = defaultdict(list)

    for container in docker_client.containers():
        networks = container['NetworkSettings']['Networks']
        if 'bridge' in networks:
            ip = networks['bridge']['IPAddress']
            if ip:
                ip_to_containers[ip].append(container)

    output = []
    for ip, containers in ip_to_containers.items():
        if len(containers) > 1:
            output.append(f'{ip} shared by the following containers:')
            for container in containers:
                output.append('    Image: {}'.format(container['Image']))
                output.append('        ID: {}'.format(container['Id']))
                output.append('        State: {}'.format(container['State']))
                output.append('        Status: {}'.format(container['Status']))
            output.append('')

    if output:
        print('CRITICAL - There are multiple Docker containers assigned to the same IP.')
        print('There should only be one per IP. Choose one to keep and try stopping the others.')
        print('\n'.join(output))
        return 2
    else:
        print('OK - No Docker containers sharing an IP on this host.')

    targets_seen = {}
    duplicates_found = False
    for rule in list_docker_nat_rules():
        target = rule.target_parameters
        if target not in targets_seen:
            targets_seen[target] = rule
        else:
            print("This is the second time we've seen a rule with the same target_parameters!")
            print(rule)
            print("The other rule with that target is:")
            print(targets_seen[target])
            duplicates_found = True
    if duplicates_found is True:
        print("CRITICAL - Duplicate iptables rules found! This will route traffic to the wrong service!")
        return 2
    else:
        print("OK - No duplicate Docker iptables rules detected")


if __name__ == '__main__':
    sys.exit(main())
