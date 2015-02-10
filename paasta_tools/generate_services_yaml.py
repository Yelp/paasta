#!/usr/bin/env python

# CEP 324: Generation of services.yaml file

import sys

import yaml
from paasta_tools.marathon_tools import get_all_namespaces
from paasta_tools.utils import atomic_file_write


# CEP 337 address for accessing services
YOCALHOST = '169.254.255.254'


def generate_configuration():
    service_data = get_all_namespaces()

    config = {}
    for (name, data) in service_data:
        proxy_port = data.get('proxy_port')
        if proxy_port is None:
            continue
        config[name] = {
            'host': YOCALHOST,
            'port': int(proxy_port),
        }

    return config


def main():
    if len(sys.argv) != 2:
        print >>sys.stderr, "Usage: %s <output_path>"
        sys.exit(1)

    output_path = sys.argv[1]
    configuration = generate_configuration()

    with atomic_file_write(output_path) as fp:
        yaml.dump(configuration,
                  fp,
                  indent=2,
                  explicit_start=True,
                  default_flow_style=False)


if __name__ == '__main__':
    main()
