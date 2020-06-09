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
import argparse

from paasta_tools import tron_tools


def parse_args():
    parser = argparse.ArgumentParser(
        description="Lists Tron namespaces for a cluster, excluding MASTER"
    )
    parser.add_argument(
        "-c",
        "--cluster",
        dest="cluster",
        default=None,
        help="Use a different Tron cluster",
    )
    parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        default=tron_tools.DEFAULT_SOA_DIR,
        help="Use a different soa config directory",
    )
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    namespaces = tron_tools.get_tron_namespaces(
        cluster=args.cluster, soa_dir=args.soa_dir
    )
    print("\n".join(namespaces))


if __name__ == "__main__":
    main()
