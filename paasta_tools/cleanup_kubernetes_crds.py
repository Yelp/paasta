#!/usr/bin/env python
# Copyright 2015-2019 Yelp Inc.
#
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
Usage: ./cleanup_kubernetes_crds.py [options]

Command line options:

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
- -c <cluster>, --cluster <cluster>: Specify a kubernetes cluster name
- -v, --verbose: Verbose output
- -n, --dry-run: Only report what would have been deleted
"""
import argparse
import logging
import sys

import service_configuration_lib
from kubernetes.client import V1DeleteOptions
from kubernetes.client.rest import ApiException

from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import load_system_paasta_config

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Removes stale kubernetes CRDs.')
    parser.add_argument(
        '-c', '--cluster', dest="cluster", metavar="CLUSTER",
        default=None,
        help="Kubernetes cluster name",
    )
    parser.add_argument(
        '-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        dest="verbose", default=False,
    )
    parser.add_argument(
        '-n', '--dry-run', action='store_true',
        dest="dry_run", default=False,
    )
    args = parser.parse_args()
    return args


def main() -> None:
    args = parse_args()
    soa_dir = args.soa_dir
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    if args.cluster:
        cluster = args.cluster
    else:
        system_paasta_config = load_system_paasta_config()
        cluster = system_paasta_config.get_cluster()

    kube_client = KubeClient()

    success = cleanup_kube_crd(
        kube_client=kube_client,
        cluster=cluster,
        soa_dir=soa_dir,
        dry_run=args.dry_run,
    )
    sys.exit(0 if success else 1)


def cleanup_kube_crd(
        kube_client: KubeClient,
        cluster: str,
        soa_dir: str = DEFAULT_SOA_DIR,
        dry_run: bool = False,
) -> bool:
    existing_crds = kube_client.apiextensions.list_custom_resource_definition(
        label_selector="yelp.com/paasta_service",
    )

    success = True
    for crd in existing_crds.items:
        service = crd.metadata.labels['yelp.com/paasta_service']
        if not service:
            log.error(
                f"CRD {crd.metadata.name} has empty paasta_service label",
            )
            continue

        crd_config = service_configuration_lib.read_extra_service_information(
            service, f'crd-{cluster}', soa_dir=soa_dir,
        )
        if crd_config:
            log.debug(f"CRD {crd.metadata.name} declaration found in {service}")
            continue

        log.info(f"CRD {crd.metadata.name} not found in {service} service")
        if dry_run:
            log.info("not deleting in dry-run mode")
            continue

        try:
            kube_client.apiextensions.delete_custom_resource_definition(
                name=crd.metadata.name,
                body=V1DeleteOptions(),
            )
            log.info(f"deleted {crd.metadata.name} for {cluster}:{service}")
        except ApiException as exc:
            log.error(
                f"error deploying crd for {cluster}:{service}, "
                f"status: {exc.status}, reason: {exc.reason}",
            )
            log.debug(exc.body)
            success = False

    return success


if __name__ == "__main__":
    main()
