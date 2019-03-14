#!/opt/venvs/paasta-tools/bin/python
# Copyright 2015-2016 Yelp Inc.
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
Usage: ./check_kubernetes_api.py [options]

This is a script that checks connectivity and credentials for Kubernetes API.
"""
import argparse
import logging
import sys

from paasta_tools.kubernetes_tools import KubeClient


log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-v', '--verbose', action='store_true', dest="verbose", default=False,
    )
    options = parser.parse_args()
    return options


def main() -> None:
    args = parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    kube_client = KubeClient()
    try:
        kube_client.core.list_namespace()
        log.info("API is ok")
        sys.exit(0)
    except Exception as exc:
        log.error(f"Error connecting to API: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
