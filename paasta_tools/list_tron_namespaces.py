"""
Usage: ./list_tron_namespaces [options]

Enumerates all Tron namespaces defined in the SOA directory for the current Tron cluster.

Command line options:

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
- -t <TRON_CLUSTER>, --tron-cluster <TRON_CLUSTER>: Specify which Tron cluster to read for
"""
import argparse

from paasta_tools import tron_tools
from paasta_tools.utils import paasta_print


def parse_args():
    parser = argparse.ArgumentParser(description='Lists Tron namespaces for a cluster.')
    parser.add_argument(
        '-t', '--tron-cluster', dest="tron_cluster", metavar="TRON_CLUSTER",
        default=None,
        help="Use a different Tron cluster",
    )
    parser.add_argument(
        '-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
        default=tron_tools.DEFAULT_SOA_DIR,
        help="Use a different soa config directory",
    )
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    namespaces = tron_tools.get_tron_namespaces_for_cluster(cluster=args.tron_cluster, soa_dir=args.soa_dir)
    paasta_print('\n'.join(namespaces))


if __name__ == "__main__":
    main()
