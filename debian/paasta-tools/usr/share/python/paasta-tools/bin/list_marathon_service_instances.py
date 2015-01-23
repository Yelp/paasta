#!/usr/share/python/paasta-tools/bin/python
"""Usage: ./list_marathon_service_instances.py [options]

Enumerates all marathon instances for services in the soa directory that
are for the current cluster (defined by the marathon configuration file).

Outputs (to stdout) a space separated list of service_name.instance_name
for each instance found in marathon-<CLUSTER>.yaml for every folder
in the SOA Configuration directory.

Command line options:

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
- -c <CLUSTER>, --cluster <CLUSTER>: Specify which cluster of services to read
"""
import argparse
import sys

import service_configuration_lib
from paasta_tools import marathon_tools


def parse_args():
    parser = argparse.ArgumentParser(description='Lists marathon instances for a service.')
    parser.add_argument('-c', '--cluster', dest="cluster", metavar="CLUSTER",
                        default=None,
                        help="define a specific cluster to read from")
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
                        default=service_configuration_lib.DEFAULT_SOA_DIR,
                        help="define a different soa config directory")
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    instances = marathon_tools.get_marathon_services_for_cluster(cluster=args.cluster,
                                                                 soa_dir=args.soa_dir)
    composed = []
    for name, instance in instances:
        composed.append('%s%s%s' % (name, marathon_tools.ID_SPACER, instance))
    print ' '.join(composed)
    sys.exit(0)


if __name__ == "__main__":
    main()
