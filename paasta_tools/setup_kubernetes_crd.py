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
Usage: ./setup_kubernetes_crd.py <service.crd> [options]

Command line options:

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
- -c <cluster>, --cluster <cluster>: Specify a kubernetes cluster name
- -v, --verbose: Verbose output
"""
import argparse
import logging
import sys
from typing import Sequence

import service_configuration_lib
from kubernetes.client import V1beta1CustomResourceDefinition
from kubernetes.client.rest import ApiException

from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import paasta_prefixed
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import load_system_paasta_config

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Creates/updates kubernetes CRDs.")
    parser.add_argument(
        "service_list",
        nargs="+",
        help="The list of services to create or update CRDs for",
        metavar="SERVICE",
    )
    parser.add_argument(
        "-c",
        "--cluster",
        dest="cluster",
        metavar="CLUSTER",
        default=None,
        help="Kubernetes cluster name",
    )
    parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", dest="verbose", default=False
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

    success = setup_kube_crd(
        kube_client=kube_client,
        cluster=cluster,
        services=args.service_list,
        soa_dir=soa_dir,
    )
    sys.exit(0 if success else 1)


def setup_kube_crd(
    kube_client: KubeClient,
    cluster: str,
    services: Sequence[str],
    soa_dir: str = DEFAULT_SOA_DIR,
) -> bool:
    existing_crds = kube_client.apiextensions.list_custom_resource_definition(
        label_selector=paasta_prefixed("service")
    )

    success = True
    for service in services:
        crd_config = service_configuration_lib.read_extra_service_information(
            service, f"crd-{cluster}", soa_dir=soa_dir
        )
        if not crd_config:
            log.info("nothing to deploy")
            continue

        metadata = crd_config.get("metadata", {})
        if "labels" not in metadata:
            metadata["labels"] = {}
        metadata["labels"]["yelp.com/paasta_service"] = service
        metadata["labels"][paasta_prefixed("service")] = service
        desired_crd = V1beta1CustomResourceDefinition(
            api_version=crd_config.get("apiVersion"),
            kind=crd_config.get("kind"),
            metadata=metadata,
            spec=crd_config.get("spec"),
        )

        existing_crd = None
        for crd in existing_crds.items:
            if crd.metadata.name == desired_crd.metadata["name"]:
                existing_crd = crd
                break

        try:
            if existing_crd:
                desired_crd.metadata[
                    "resourceVersion"
                ] = existing_crd.metadata.resource_version
                kube_client.apiextensions.replace_custom_resource_definition(
                    name=desired_crd.metadata["name"], body=desired_crd
                )
            else:
                try:
                    kube_client.apiextensions.create_custom_resource_definition(
                        body=desired_crd
                    )
                except ValueError as err:
                    # TODO: kubernetes server will sometimes reply with conditions:null,
                    # figure out how to deal with this correctly, for more details:
                    # https://github.com/kubernetes/kubernetes/pull/64996
                    if "`conditions`, must not be `None`" in str(err):
                        pass
                    else:
                        raise err
            log.info(f"deployed {desired_crd.metadata['name']} for {cluster}:{service}")
        except ApiException as exc:
            log.error(
                f"error deploying crd for {cluster}:{service}, "
                f"status: {exc.status}, reason: {exc.reason}"
            )
            log.debug(exc.body)
            success = False

    return success


if __name__ == "__main__":
    main()
