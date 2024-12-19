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
from paasta_tools.cli.utils import get_instance_configs_for_service
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.spark_tools import SPARK_EXECUTOR_NAMESPACE
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import list_clusters
from paasta_tools.utils import list_services


def add_subparser(subparsers) -> None:
    list_parser = subparsers.add_parser(
        "list-namespaces",
        help="Lists all k8s namespaces used by instances of a service",
    )
    list_parser.add_argument(
        "-s",
        "--service",
        help="Name of the service which you want to list the namespaces for.",
        required=True,
    ).completer = lazy_choices_completer(list_services)
    # Most services likely don't need to filter by cluster/instance, and can add namespaces from all instances
    list_parser.add_argument(
        "-i",
        "--instance",
        help="Instance of the service that you want to list namespaces for. Like 'main' or 'canary'.",
        required=False,
    )
    list_parser.add_argument(
        "-c",
        "--cluster",
        help="Clusters that you want to list namespaces for. Like 'pnw-prod' or 'norcal-stagef'.",
        required=False,
    ).completer = lazy_choices_completer(list_clusters)
    list_parser.add_argument(
        "-y",
        "-d",
        "--soa-dir",
        dest="soa_dir",
        default=DEFAULT_SOA_DIR,
        required=False,
        help="define a different soa config directory",
    )
    list_parser.set_defaults(command=paasta_list_namespaces)


def paasta_list_namespaces(args):
    service = args.service
    soa_dir = args.soa_dir
    validate_service_name(service, soa_dir)

    namespaces = set()
    instance_configs = get_instance_configs_for_service(
        service=service, soa_dir=soa_dir, instances=args.instance, clusters=args.cluster
    )
    for instance in instance_configs:
        # We skip non-k8s instance types
        if instance.get_instance_type() in ("paasta-native", "adhoc"):
            continue
        namespaces.add(instance.get_namespace())
        # Tron instances are TronActionConfigs
        if (
            instance.get_instance_type() == "tron"
            and instance.get_executor() == "spark"
        ):
            # We also need paasta-spark for spark executors
            namespaces.add(SPARK_EXECUTOR_NAMESPACE)

    # Print in list format to be used in iam_roles
    print(list(namespaces))
    return 0
