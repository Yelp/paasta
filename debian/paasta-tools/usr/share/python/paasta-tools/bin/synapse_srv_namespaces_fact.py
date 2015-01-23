#!/usr/share/python/paasta-tools/bin/python
"""Usage: ./synapse_srv_namespaces_fact.py

A simple script to enumerate all namespaces as a sorted comma separated
string to stdout, with each entry in the form of full_name:proxy_port.

If a proxy_port isn't defined for a namespace, that namespace is skipped.

Example output: mumble.canary:5019,mumble.main:111,zookeeper.devc:4921

This is nice to use as a facter fact for Synapse stuff!
"""
import sys
from paasta_tools import marathon_tools


def main():
    strings = []
    for full_name, config in marathon_tools.get_all_namespaces():
        if 'proxy_port' in config:
            strings.append('%s:%s' % (full_name, config['proxy_port']))
    strings = sorted(strings)
    print "synapse_srv_namespaces=" + ','.join(strings)
    sys.exit(0)


if __name__ == "__main__":
    main()
