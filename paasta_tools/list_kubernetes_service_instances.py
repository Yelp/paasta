#!/usr/bin/env python
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
"""Usage: ./list_kubernetes_service_instances.py [options]

Enumerates all kubernetes instances for services in the soa directory that
are for the current cluster (defined by the kubernetes configuration file).

Outputs (to stdout) a list of service.instance (one per line)
for each instance found in kubernetes-<CLUSTER>.yaml for every folder
in the SOA Configuration directory.

Command line options:

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
- -c <CLUSTER>, --cluster <CLUSTER>: Specify which cluster of services to read
"""
import argparse
import sys

from paasta_tools import kubernetes_tools
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_services_for_cluster


def parse_args():
    parser = argparse.ArgumentParser(
        description="Lists kubernetes instances for a service."
    )
    parser.add_argument(
        "-c",
        "--cluster",
        dest="cluster",
        metavar="CLUSTER",
        default=None,
        help="define a specific cluster to read from",
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
        "--sanitise",
        action="store_true",
        help=(
            "Whether or not to sanitise service instance names before displaying "
            "them. Kubernetes apps created by PaaSTA use sanitised names."
        ),
    )
    parser.add_argument(
        "-t",
        "--instance-type",
        dest="instance_type",
        default="kubernetes",
        help="Instance type to list, default %(default)s",
    )
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    soa_dir = args.soa_dir
    cluster = args.cluster
    instances = get_services_for_cluster(
        cluster=cluster, instance_type=args.instance_type, soa_dir=soa_dir
    )
    service_instances = []
    for name, instance in instances:
        if args.sanitise:
            app_name = kubernetes_tools.get_kubernetes_app_name(name, instance)
        else:
            app_name = compose_job_id(name, instance)
        service_instances.append(app_name)
    print("\n".join(service_instances))
    sys.exit(0)


if __name__ == "__main__":
    main()
