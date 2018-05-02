#!/usr/bin/env python
"""
Usage: ./cleanup_tron_namespaces.py [options]

Delete namespaces that aren't configured in SOA configs for a Tron cluster.

Gets the list of namespaces from Tron, then compares to the namespaces
defined in SOA configs.

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
"""
import argparse
import sys

from paasta_tools import tron_tools
from paasta_tools.utils import paasta_print


def parse_args():
    parser = argparse.ArgumentParser(description='Cleans up stale Tron namespaces.')
    parser.add_argument(
        '-d', '--soa-dir', dest='soa_dir', metavar='SOA_DIR',
        default=tron_tools.DEFAULT_SOA_DIR,
        help='Use a different soa config directory',
    )
    args = parser.parse_args()
    return args


def main():
    args = parse_args()

    client = tron_tools.get_tron_client()
    namespaces = client.list_namespaces()
    expected_namespaces = tron_tools.get_tron_namespaces_for_cluster(soa_dir=args.soa_dir)
    to_delete = set(namespaces) - set(expected_namespaces)

    if not to_delete:
        paasta_print('No Tron namespaces to remove')
        sys.exit(0)

    successes = []
    errors = []
    for namespace in to_delete:
        try:
            client.update_namespace(namespace, '')
            successes.append(namespace)
        except Exception as e:
            errors.append((namespace, e))

    if successes:
        paasta_print('Successfully removed namespaces:\n', '\n  '.join(successes))

    if errors:
        paasta_print(
            'Failed to remove namespaces:\n  ' + '\n  '.join(
                ['{namespace}: {error}'.format(namespace=namespace, error=str(error)) for namespace, error in errors],
            ),
        )
        sys.exit(1)


if __name__ == '__main__':
    main()
