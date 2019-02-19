#!/opt/venvs/paasta-tools/bin/python3
import sys

import iptc

from paasta_tools import iptables
from paasta_tools.utils import get_docker_client


def list_docker_nat_rules():
    chain_name = 'DOCKER'
    table = iptc.Table(iptc.Table.NAT)
    chain = iptc.Chain(table, chain_name)
    for rule in chain.rules:
        yield iptables.Rule.from_iptc(rule)


def get_container_from_dport(dport, docker_client):
    for container in docker_client.containers():
        try:
            ports = container['Ports']
            for port in ports:
                if "PublicPort" in port:
                    if port["PublicPort"] == int(dport):
                        return container
        except KeyError:
            print(ports)
            pass


def target_rule_to_dport(rule):
    try:
        # (('tcp', (('dport', ('31493',)),)),)
        return rule.matches[0][1][0][1][0]
    except IndexError:
        return None


def kill_containers_with_duplicate_iptables_rules(docker_client):
    targets_seen = {}
    for rule in list_docker_nat_rules():
        target = rule.target_parameters
        if target not in targets_seen:
            targets_seen[target] = rule
        else:
            print("This is the second time we've seen a rule with the same target_parameters!")
            print(rule)
            dport = target_rule_to_dport(rule)
            container1 = get_container_from_dport(dport, docker_client)
            print("The other rule with that target is:")
            print(targets_seen[target])
            dport = target_rule_to_dport(rule)
            container2 = get_container_from_dport(dport, docker_client)
            if container1["Id"] == container2["Id"]:
                print("The same container is getting traffic for both ports!")
                print(container1)
                print("Killing the container")
                docker_client.kill(container1["Id"])
            else:
                print("These are two different containers, which means we have duplicate ips:")
                print(container1)
                print(container2)
                print("Not sure which to kill, picking the first one")
                docker_client.kill(container1["Id"])


def main():
    docker_client = get_docker_client()
    kill_containers_with_duplicate_iptables_rules(docker_client)


if __name__ == '__main__':
    sys.exit(main())
