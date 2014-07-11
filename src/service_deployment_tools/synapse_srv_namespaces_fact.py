#!/usr/bin/env python
import sys
from service_deployment_tools import marathon_tools


def main():
    """A simple script to enumerate all namespaces as a sorted comma separated
    string to stdout, with each entry in the form of full_name:proxy_port.

    If a proxy_port isn't defined for a namespace, it's skipped.

    Example output: mumble.canary:5019,mumble.main:111,zookeeper.devc:4921"""
    strings = []
    for full_name, config in marathon_tools.get_all_namespaces():
        if 'proxy_port' in config:
            strings.append('%s:%s' % (full_name, config['proxy_port']))
    strings = sorted(strings)
    print ','.join(strings)
    sys.exit(0)


if __name__ == "__main__":
    main()