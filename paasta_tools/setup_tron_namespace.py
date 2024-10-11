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
from typing import Dict
from typing import List

import ruamel.yaml as yaml

from paasta_tools import spark_tools
from paasta_tools import tron_tools
from paasta_tools.kubernetes_tools import ensure_service_account
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.tron_tools import KUBERNETES_NAMESPACE
from paasta_tools.tron_tools import load_tron_service_config
from paasta_tools.tron_tools import MASTER_NAMESPACE
from paasta_tools.tron_tools import TronJobConfig
from paasta_tools.utils import load_system_paasta_config

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
        "--bulk-config-fetch",
        dest="bulk_config_fetch",
        action="store_true",
        default=False,
        help="Attempt to fetch all configs in bulk rather than one by one",
    )
    parser.add_argument(
        "--cluster",
        help="Cluster to read configs for. Defaults to the configuration in /etc/paasta",
        default=None,
    )
    args = parser.parse_args()
    return args


def ensure_service_accounts(job_configs: List[TronJobConfig]) -> None:
    # NOTE: these are lru_cache'd so it should be fine to call these for every service
    system_paasta_config = load_system_paasta_config()
    kube_client = KubeClient()

    for job in job_configs:
        for action in job.get_actions():
            if action.get_iam_role():
                ensure_service_account(
                    action.get_iam_role(),
                    namespace=KUBERNETES_NAMESPACE,
                    kube_client=kube_client,
                )
                # spark executors are special in that we want the SA to exist in two namespaces:
                # the tron namespace - for the spark driver (which will be created by the ensure_service_account() above)
                # and the spark namespace - for the spark executor (which we'll create below)
                if (
                    action.get_executor() == "spark"
                    # this should always be truthy, but let's be safe since this comes from SystemPaastaConfig
                    and action.get_spark_executor_iam_role()
                ):
                    # this kubeclient creation is lru_cache'd so it should be fine to call this for every spark action
                    spark_kube_client = KubeClient(
                        config_file=system_paasta_config.get_spark_kubeconfig()
                    )
                    # this will look quite similar to the above, but we're ensuring that a potentially different SA exists:
                    # this one is for the actual spark executors to use. if an iam_role is set, we'll use that, otherwise
                    # there's an executor-specifc default role just like there is for the drivers :)
                    ensure_service_account(
                        action.get_spark_executor_iam_role(),
                        namespace=spark_tools.SPARK_EXECUTOR_NAMESPACE,
                        kube_client=spark_kube_client,
                    )


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
        except Exception:
            log.exception("Failed to list tron namespaces:")
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
        try:
            if client.update_namespace(MASTER_NAMESPACE, master_config):
                updated.append(MASTER_NAMESPACE)
                log.debug(f"Updated {MASTER_NAMESPACE}")
            else:
                skipped.append(MASTER_NAMESPACE)
                log.debug(f"Skipped {MASTER_NAMESPACE}")
        except Exception:
            failed.append(MASTER_NAMESPACE)
            log.exception(f"Error while updating {MASTER_NAMESPACE}:")

    k8s_enabled_for_cluster = (
        yaml.safe_load(master_config).get("k8s_options", {}).get("enabled", False)
    )
    new_configs: Dict[str, str] = {}  # service -> new_config
    for service in sorted(services):
        try:
            new_config = tron_tools.create_complete_config(
                cluster=args.cluster,
                service=service,
                soa_dir=args.soa_dir,
                k8s_enabled=k8s_enabled_for_cluster,
                dry_run=args.dry_run,
            )
            new_configs[service] = new_config
            if args.dry_run:
                log.info(f"Would update {service} to:")
                log.info(f"{new_config}")
                updated.append(service)
            else:
                # PaaSTA will not necessarily have created the SAs we want to use
                # ...so let's go ahead and create them!
                job_configs = load_tron_service_config(
                    service=service,
                    cluster=args.cluster,
                    load_deployments=False,
                    soa_dir=args.soa_dir,
                    # XXX: we can remove for_validation now that we've refactored how service account stuff works
                    for_validation=False,
                )
                ensure_service_accounts(job_configs)
                if not args.bulk_config_fetch:
                    if client.update_namespace(service, new_config):
                        updated.append(service)
                        log.debug(f"Updated {service}")
                    else:
                        skipped.append(service)
                        log.debug(f"Skipped {service}")

        except Exception:
            if args.bulk_config_fetch:
                # service account creation should be the only action that can throw if this flag is true,
                # so we can safely assume that's what happened here in the log message
                log.exception(
                    f"Failed to create service account for {service} (will skip reconfiguring):"
                )

                # since service account creation failed, we want to skip reconfiguring this service
                # as the new config will likely fail due to the missing service account - even though
                # the rest of the config is valid
                new_configs.pop(service, None)
            else:
                log.exception(f"Update for {service} failed:")

            # NOTE: this happens for both ways of updating (bulk fetch and JIT fetch)
            # since we need to print out what failed in either case
            failed.append(service)

    if args.bulk_config_fetch:
        updated_namespaces = client.update_namespaces(new_configs)

        if updated_namespaces:
            updated = list(updated_namespaces.keys())
            log.debug(f"Updated {updated}")

        if updated_namespaces != new_configs.keys():
            skipped = set(new_configs.keys()) - set(updated_namespaces.keys())
            log.debug(f"Skipped {skipped}")

    skipped_report = skipped if args.verbose else len(skipped)
    log.info(
        f"Updated following namespaces: {updated}, "
        f"failed: {failed}, skipped: {skipped_report}"
    )

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
