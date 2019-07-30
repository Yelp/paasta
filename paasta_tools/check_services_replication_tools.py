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
import argparse
import logging
from typing import Callable
from typing import Sequence
from typing import Type

from mypy_extensions import Arg

from paasta_tools.kubernetes_tools import get_all_nodes
from paasta_tools.kubernetes_tools import get_all_pods
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import V1Pod
from paasta_tools.paasta_service_config_loader import PaastaServiceConfigLoader
from paasta_tools.smartstack_tools import KubeSmartstackReplicationChecker
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import InstanceConfig_T
from paasta_tools.utils import list_services
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import SPACER


log = logging.getLogger(__name__)

CheckServiceReplication = Callable[
    [
        Arg(InstanceConfig_T, "instance_config"),
        Arg(Sequence[V1Pod], "all_pods"),
        Arg(KubeSmartstackReplicationChecker, "smartstack_replication_checker"),
    ],
    None,
]


def parse_args() -> argparse.Namespace:
    epilog = "PERCENTAGE is an integer value representing the percentage of available to expected instances"
    parser = argparse.ArgumentParser(epilog=epilog)

    parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    parser.add_argument(
        "service_instance_list",
        nargs="*",
        help="The list of service instances to check",
        metavar="SERVICE%sINSTANCE" % SPACER,
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", dest="verbose", default=False
    )
    options = parser.parse_args()

    return options


def check_all_kubernetes_based_services_replication(
    soa_dir: str,
    service_instances: Sequence[str],
    instance_type_class: Type[InstanceConfig_T],
    check_service_replication: CheckServiceReplication,
    namespace: str,
) -> None:
    kube_client = KubeClient()
    all_pods = get_all_pods(kube_client=kube_client, namespace=namespace)
    all_nodes = get_all_nodes(kube_client)
    system_paasta_config = load_system_paasta_config()
    cluster = system_paasta_config.get_cluster()
    smartstack_replication_checker = KubeSmartstackReplicationChecker(
        nodes=all_nodes, system_paasta_config=system_paasta_config
    )
    service_instances_set = set(service_instances)

    for service in list_services(soa_dir=soa_dir):
        service_config = PaastaServiceConfigLoader(service=service, soa_dir=soa_dir)
        for instance_config in service_config.instance_configs(
            cluster=cluster, instance_type_class=instance_type_class
        ):
            if (
                service_instances_set
                and f"{service}{SPACER}{instance_config.instance}"
                not in service_instances_set
            ):
                continue
            if instance_config.get_docker_image():
                check_service_replication(
                    instance_config=instance_config,
                    all_pods=all_pods,
                    smartstack_replication_checker=smartstack_replication_checker,
                )
            else:
                log.debug(
                    "%s is not deployed. Skipping replication monitoring."
                    % instance_config.job_id
                )


def main(
    instance_type_class: Type[InstanceConfig_T],
    check_service_replication: CheckServiceReplication,
    namespace: str,
) -> None:
    args = parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)
    check_all_kubernetes_based_services_replication(
        soa_dir=args.soa_dir,
        service_instances=args.service_instance_list,
        instance_type_class=instance_type_class,
        check_service_replication=check_service_replication,
        namespace=namespace,
    )
