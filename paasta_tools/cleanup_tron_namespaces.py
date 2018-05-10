#!/usr/bin/env python
# Copyright 2015-2018 Yelp Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Usage: ./cleanup_tron_namespaces.py [options]

Delete namespaces that aren't configured in SOA configs for a Tron cluster.

Gets the list of namespaces from Tron, then compares to the namespaces
defined in SOA configs.

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
- --dry-run: Print namespaces to be deleted instead of deleting them
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
    parser.add_argument(
        '--dry-run', dest='dry_run', action='store_true',
        help='Print namespaces to be deleted, instead of deleting them',
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

    if args.dry_run:
        paasta_print('Dry run, would have removed namespaces:\n  ' + '\n  '.join(to_delete))
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
