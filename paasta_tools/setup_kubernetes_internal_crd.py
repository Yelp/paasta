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
Usage: ./setup_kubernetes_internal_crd.py [options]

Command line options:

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
- -c <cluster>, --cluster <cluster>: Specify a kubernetes cluster name
- -v, --verbose: Verbose output
"""
import argparse
import logging
import sys

from kubernetes.client import V1beta1CustomResourceDefinition
from kubernetes.client.rest import ApiException

from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import paasta_prefixed
from paasta_tools.utils import load_system_paasta_config

log = logging.getLogger(__name__)


INTERNAL_CRDS = [
    V1beta1CustomResourceDefinition(
        api_version="apiextensions.k8s.io/v1beta1",
        kind="CustomResourceDefinition",
        metadata={
            "name": "deploygroups.paasta.yelp.com",
            "labels": {
                paasta_prefixed("internal"): "true",
            },
        },
        spec={
            "group": "paasta.yelp.com",
            "versions": [{"name": "v1", "served": True, "storage": True}],
            "scope": "Namespaced",
            "names": {
                "plural": "deploygroups",
                "singular": "deploygroup",
                "kind": "DeployGroup",
                "shortNames": ["dg"],
            },
            "validation": {
                "openAPIV3Schema": {
                    "type": "object",
                    "properties": {
                        "service": {"type": "string"},
                        "deploy_group": {"type": "string"},
                        "git_sha": {"type": "string"},
                    },
                }
            },
        },
    ),
    V1beta1CustomResourceDefinition(
        api_version="apiextensions.k8s.io/v1beta1",
        kind="CustomResourceDefinition",
        metadata={
            "name": "startstopcontrols.paasta.yelp.com",
            "labels": {
                paasta_prefixed("internal"): "true",
            },
        },
        spec={
            "group": "paasta.yelp.com",
            "versions": [{"name": "v1", "served": True, "storage": True}],
            "scope": "Namespaced",
            "names": {
                "plural": "startstopcontrols",
                "singular": "startstopcontrol",
                "kind": "StartStopControl",
            },
            "validation": {
                "openAPIV3Schema": {
                    "type": "object",
                    "properties": {
                        "service": {"type": "string"},
                        "instance": {"type": "string"},
                        "desired_state": {"type": "string"},
                        "force_bounce": {"type": "string"},
                    },
                }
            },
        },
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Creates/updates Paasta-internal kubernetes CRDs."
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
        "-v", "--verbose", action="store_true", dest="verbose", default=False
    )
    args = parser.parse_args()
    return args


def main() -> None:
    args = parse_args()
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

    success = setup_kube_internal_crd(
        kube_client=kube_client,
        cluster=cluster,
    )
    sys.exit(0 if success else 1)


def setup_kube_internal_crd(
    kube_client: KubeClient,
    cluster: str,
) -> bool:
    existing_crds = kube_client.apiextensions.list_custom_resource_definition(
        label_selector=paasta_prefixed("internal")
    )

    success = True
    for desired_crd in INTERNAL_CRDS:
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
            log.info(
                f"deployed internal crd {desired_crd.metadata['name']} on cluster {cluster}"
            )
        except ApiException as exc:
            log.error(
                f"error deploying crd {desired_crd.metadata['name']} on cluster {cluster}, "
                f"status: {exc.status}, reason: {exc.reason}"
            )
            log.debug(exc.body)
            success = False

    return success


if __name__ == "__main__":
    main()
