#!/opt/venvs/paasta-tools/bin/python3
import sys

import iptc

from paasta_tools import iptables
from paasta_tools.utils import get_docker_client


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
    chain_name = 'DOCKER'
    table = iptc.Table(iptc.Table.NAT)
    chain = iptc.Chain(table, chain_name)

    targets_seen = {}
    raw_rules_seen = {}

    for iptables_rule in chain.rules:
        rule = iptables.Rule.from_iptc(iptables_rule)
        target = rule.target_parameters
        if target not in targets_seen:
            targets_seen[target] = rule
            raw_rules_seen[target] = iptables_rule
        else:
            print("This is the second time we've seen a rule with the same target_parameters!")
            print(rule)
            dport = target_rule_to_dport(rule)
            container1 = get_container_from_dport(dport, docker_client)
            print("The other rule with that target is:")
            print(targets_seen[target])
            dport2 = target_rule_to_dport(targets_seen[target])
            container2 = get_container_from_dport(dport2, docker_client)
            if container1 is None or container2 is None:
                print("Error: there is only one container here and we couldn't determine the other:")
                print(f"container1: {container1}")
                print(f"container2: {container2}")
                print("This script currently doesn't understand this situation and manual intervention is required")
                return 1
            if container1["Id"] == container2["Id"]:
                print("The same container is getting traffic for both ports!")
                print(container1)
                print("Killing the container")
                docker_client.kill(container1["Id"])
                print("Deleting both iptables rules")
                chain.delete_rule(iptables_rule)
                chain.delete_rule(raw_rules_seen[target])
            else:
                print("These are two different containers, which means we have duplicate ips:")
                print(container1)
                print(container2)
                print("Not sure which to kill, killing both")
                docker_client.kill(container1["Id"])
                docker_client.kill(container2["Id"])
                print("Deleting the both iptables rules for good measure")
                chain.delete_rule(iptables_rule)
                chain.delete_rule(raw_rules_seen[target])


def main():
    docker_client = get_docker_client()
    kill_containers_with_duplicate_iptables_rules(docker_client)


if __name__ == '__main__':
    sys.exit(main())
