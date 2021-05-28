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

import ruamel.yaml as yaml

from paasta_tools import tron_tools
from paasta_tools.tron_tools import MASTER_NAMESPACE

log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Update the Tron namespace configuration for a service."
    )
    parser.add_argument("services", nargs="*", help="Services to update.")
    parser.add_argument(
        "-a",
        "--all",
        dest="all_namespaces",
        action="store_true",
        help="Update all available Tron namespaces.",
    )
    parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=tron_tools.DEFAULT_SOA_DIR,
        help="Use a different soa config directory",
    )
    parser.add_argument("-v", "--verbose", action="store_true", default=False)
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument(
        "--cluster",
        help="Cluster to read configs for. Defaults to the configuration in /etc/paasta",
        default=None,
    )
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level)

    if not args.cluster:
        args.cluster = tron_tools.load_tron_config().get_cluster_name()

    if args.all_namespaces:
        if args.services:
            log.error("Do not pass service names with --all flag")
            sys.exit(1)

        try:
            services = tron_tools.get_tron_namespaces(
                cluster=args.cluster, soa_dir=args.soa_dir
            )
        except Exception as e:
            log.error("Failed to list tron namespaces: {error}".format(error=str(e)))
            sys.exit(1)
    else:
        services = args.services

    if not services:
        log.warning("No namespaces found")
        sys.exit(0)

    if not args.dry_run:
        client = tron_tools.get_tron_client()

    updated = []
    failed = []
    skipped = []

    master_config = tron_tools.create_complete_master_config(
        cluster=args.cluster, soa_dir=args.soa_dir
    )
    if args.dry_run:
        log.info(f"Would update {MASTER_NAMESPACE} to:")
        log.info(f"{master_config}")
        updated.append(MASTER_NAMESPACE)
    else:
        if client.update_namespace(MASTER_NAMESPACE, master_config):
            updated.append(MASTER_NAMESPACE)
            log.debug(f"Updated {MASTER_NAMESPACE}")
        else:
            skipped.append(MASTER_NAMESPACE)
            log.debug(f"Skipped {MASTER_NAMESPACE}")

    k8s_enabled_for_cluster = (
        yaml.safe_load(master_config).get("k8s_options", {}).get("enabled", False)
    )
    for service in sorted(services):
        try:
            new_config = tron_tools.create_complete_config(
                cluster=args.cluster,
                service=service,
                soa_dir=args.soa_dir,
                k8s_enabled=k8s_enabled_for_cluster,
            )
            if args.dry_run:
                log.info(f"Would update {service} to:")
                log.info(f"{new_config}")
                updated.append(service)
            else:
                if client.update_namespace(service, new_config):
                    updated.append(service)
                    log.debug(f"Updated {service}")
                else:
                    skipped.append(service)
                    log.debug(f"Skipped {service}")
        except Exception as e:
            log.error(f"Update for {service} failed: {str(e)}")
            log.debug(f"Exception while updating {service}", exc_info=1)
            failed.append(service)

    skipped_report = skipped if args.verbose else len(skipped)
    log.info(
        f"Updated following namespaces: {updated}, "
        f"failed: {failed}, skipped: {skipped_report}"
    )

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
