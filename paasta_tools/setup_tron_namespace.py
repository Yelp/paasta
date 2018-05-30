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
Usage: ./setup_tron_namespace.py service [service...] | --all

Deploy a namespace to the local Tron master from a service configuration file.
Reads from the soa_dir /nail/etc/services by default.

The script will load the service configuration file, generate a Tron configuration
file for it, and send the updated file to Tron.
"""
import argparse
import logging
import sys

from paasta_tools import tron_tools

log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description='Update the Tron namespace configuration for a service.',
    )
    parser.add_argument(
        'services',
        nargs='*',
        help='Services to update.',
    )
    parser.add_argument(
        '-a',
        '--all',
        dest='all_namepsaces',
        action='store_true',
        help='Update all available Tron namespaces.',
    )
    parser.add_argument(
        '-d',
        '--soa-dir',
        dest="soa_dir",
        metavar="SOA_DIR",
        default=tron_tools.DEFAULT_SOA_DIR,
        help="Use a different soa config directory",
    )
    parser.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        default=False,
    )
    args = parser.parse_args()
    return args


def main():
    args = parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    if args.all_namespaces:
        try:
            services = tron_tools.get_tron_namespaces_for_cluster()
        except Exception as e:
            log.error('Failed to list tron namespaces: {error}'.format(
                error=str(e),
            ))
            sys.exit(1)
    else:
        services = args.services

    if not services:
        log.warning("No namespaces found")
        sys.exit(0)

    client = tron_tools.get_tron_client()

    updated = []
    failed = []

    for service in services:
        try:
            new_config = tron_tools.create_complete_config(
                service=args.service,
                soa_dir=args.soa_dir,
            )
            client.update_namespace(args.service, new_config)
            updated.append(service)
        except Exception as e:
            log.error('Update for {namespace} failed: {error}'.format(
                namespace=args.service, error=str(e),
            ))
            failed.append(service)

    log.info(
        'Updated following namespaces: {updated}, failed: {failed}'.format(
            updated=updated,
            failed=failed,
        ),
    )

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
