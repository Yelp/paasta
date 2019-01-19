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
- -v, --verbose: Verbose output
"""
import argparse
import logging
import sys
from typing import Sequence

from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import V1beta1CustomResourceDefinition

from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import load_system_paasta_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Creates/updates kubernetes CRDs.')
    parser.add_argument(
        'service_list', nargs='+',
        help="The list of services to create or update CRDs for",
        metavar="SERVICE",
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
    args = parser.parse_args()
    return args


def main() -> None:
    args = parse_args()
    soa_dir = args.soa_dir
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    system_paasta_config = load_system_paasta_config()
    kube_client = KubeClient()

    success = setup_kube_crd(
        kube_client=kube_client,
        cluster=system_paasta_config.get_cluster(),
        services=args.service_list,
        soa_dir=soa_dir,
    )
    sys.exit(0 if success else 1)


def setup_kube_crd(
        kube_client: KubeClient,
        cluster: str,
        services: Sequence[str],
        soa_dir: str=DEFAULT_SOA_DIR,
):
    for service in services:
        print(f"deploying {cluster}:{service}")


if __name__ == "__main__":
    main()
