#!/usr/bin/env python

import argparse
import sys

import service_configuration_lib
from service_deployment_tools import marathon_tools


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
    """Enumerates all marathon instances for services in a given cluster and soa directory.

    This uses the cluster defined in the marathon configuration file for
    service_deployment_tools, and will attempt to read from the default soa
    directory defined in service_configuration_lib. You can specify a different
    soa directory with -d SOA_DIR, and a cluster with -c CLUSTER."""
    args = parse_args()
    instances = marathon_tools.get_marathon_services_for_cluster(cluster=args.cluster,
                                                                 soa_dir=args.soa_dir,
                                                                 include_iteration=False)
    print ' '.join(instances)
    sys.exit(0)


if __name__ == "__main__":
    main()