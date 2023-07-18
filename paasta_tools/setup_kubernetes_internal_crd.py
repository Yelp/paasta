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

- -v, --verbose: Verbose output
"""
import argparse
import logging
import sys

from kubernetes.client import V1CustomResourceDefinition

from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import paasta_prefixed
from paasta_tools.kubernetes_tools import update_crds

log = logging.getLogger(__name__)


INTERNAL_CRDS = [
    V1CustomResourceDefinition(
        api_version="apiextensions.k8s.io/v1",
        kind="CustomResourceDefinition",
        metadata={
            "name": "deploygroups.paasta.yelp.com",
            "labels": {
                paasta_prefixed("internal"): "true",
            },
        },
        spec={
            "group": "paasta.yelp.com",
            "versions": [
                {
                    "name": "v1beta1",
                    "served": True,
                    "storage": True,
                    "schema": {
                        "openAPIV3Schema": {
                            "type": "object",
                            "properties": {
                                "service": {"type": "string"},
                                "deploy_group": {"type": "string"},
                                "git_sha": {"type": "string"},
                                "image_version": {"type": "string"},
                            },
                        }
                    },
                }
            ],
            "scope": "Namespaced",
            "names": {
                "plural": "deploygroups",
                "singular": "deploygroup",
                "kind": "DeployGroup",
                "shortNames": ["dg"],
            },
        },
    ),
    V1CustomResourceDefinition(
        api_version="apiextensions.k8s.io/v1",
        kind="CustomResourceDefinition",
        metadata={
            "name": "startstopcontrols.paasta.yelp.com",
            "labels": {
                paasta_prefixed("internal"): "true",
            },
        },
        spec={
            "group": "paasta.yelp.com",
            "versions": [
                {
                    "name": "v1beta1",
                    "served": True,
                    "storage": True,
                    "schema": {
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
                }
            ],
            "scope": "Namespaced",
            "names": {
                "plural": "startstopcontrols",
                "singular": "startstopcontrol",
                "kind": "StartStopControl",
            },
        },
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Creates/updates Paasta-internal kubernetes CRDs."
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

    kube_client = KubeClient()

    success = setup_kube_internal_crd(
        kube_client=kube_client,
    )
    sys.exit(0 if success else 1)


def setup_kube_internal_crd(
    kube_client: KubeClient,
) -> bool:
    existing_crds = kube_client.apiextensions.list_custom_resource_definition(
        label_selector=paasta_prefixed("internal")
    )
    return update_crds(
        kube_client=kube_client,
        desired_crds=INTERNAL_CRDS,
        existing_crds=existing_crds,
    )


if __name__ == "__main__":
    main()
