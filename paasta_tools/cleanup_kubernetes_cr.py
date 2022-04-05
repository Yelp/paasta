#!/usr/bin/env python
# Copyright 2015-2018 Yelp Inc.
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
Usage: ./cleanup_kubernetes_cr.py [options]

Command line options:

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
- -v, --verbose: Verbose output
"""
import argparse
import logging
import sys
from typing import Sequence

from paasta_tools.kubernetes_tools import CustomResourceDefinition
from paasta_tools.kubernetes_tools import delete_custom_resource
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import list_custom_resources
from paasta_tools.kubernetes_tools import load_custom_resource_definitions
from paasta_tools.kubernetes_tools import paasta_prefixed
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import load_all_configs
from paasta_tools.utils import load_system_paasta_config

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cleanup custom_resources.")
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
    parser.add_argument(
        "-c", "--cluster", default=None, help="Cluster to cleanup CRs for"
    )
    args = parser.parse_args()
    return args


def main() -> None:
    args = parse_args()
    soa_dir = args.soa_dir
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    kube_client = KubeClient()

    system_paasta_config = load_system_paasta_config()
    cluster = args.cluster or system_paasta_config.get_cluster()
    custom_resource_definitions = load_custom_resource_definitions(system_paasta_config)
    cleanup_kube_succeeded = cleanup_all_custom_resources(
        kube_client=kube_client,
        soa_dir=soa_dir,
        cluster=cluster,
        custom_resource_definitions=custom_resource_definitions,
    )
    sys.exit(0 if cleanup_kube_succeeded else 1)


def cleanup_all_custom_resources(
    kube_client: KubeClient,
    soa_dir: str,
    cluster: str,
    custom_resource_definitions: Sequence[CustomResourceDefinition],
) -> bool:
    cluster_crds = {
        crd.spec.names.kind
        for crd in kube_client.apiextensions.list_custom_resource_definition(
            label_selector=paasta_prefixed("service")
        ).items
    }
    log.debug(f"CRDs found: {cluster_crds}")
    results = []
    for crd in custom_resource_definitions:
        if crd.kube_kind.singular not in cluster_crds:
            # TODO: kube_kind.singular seems to correspond to `crd.names.kind`
            # and not `crd.names.singular`
            log.warning(f"CRD {crd.kube_kind.singular} " f"not found in {cluster}")
            continue
        config_dicts = load_all_configs(
            cluster=cluster, file_prefix=crd.file_prefix, soa_dir=soa_dir
        )
        if not config_dicts:
            continue
        crs = list_custom_resources(
            kube_client=kube_client,
            kind=crd.kube_kind,
            version=crd.version,
            group=crd.group,
        )
        for cr in crs:
            service = config_dicts.get(cr.service)
            if service is not None:
                instance = service.get(cr.instance)
                if instance is not None:
                    continue
            result = False
            try:
                delete_custom_resource(
                    kube_client=kube_client,
                    name=cr.name,
                    namespace=cr.namespace,
                    plural=crd.kube_kind.plural,
                    version=crd.version,
                    group=crd.group,
                )
                result = True
            except Exception:
                log.exception("Error while deleting CR {cr.name}")
            results.append(result)
    return all(results) if results else True


if __name__ == "__main__":
    main()
