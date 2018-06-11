import sys
from collections import defaultdict

from paasta_tools.utils import get_docker_client


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
        return 0


if __name__ == '__main__':
    sys.exit(main())
