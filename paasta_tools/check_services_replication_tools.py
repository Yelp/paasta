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
import sys
from typing import Any
from typing import Callable
from typing import Container
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Type

from mypy_extensions import Arg
from mypy_extensions import NamedArg

from paasta_tools.kubernetes_tools import get_all_managed_namespaces
from paasta_tools.kubernetes_tools import get_all_nodes
from paasta_tools.kubernetes_tools import get_all_pods
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import V1Node
from paasta_tools.kubernetes_tools import V1Pod
from paasta_tools.metrics import metrics_lib
from paasta_tools.monitoring_tools import ReplicationChecker
from paasta_tools.paasta_service_config_loader import PaastaServiceConfigLoader
from paasta_tools.smartstack_tools import KubeSmartstackEnvoyReplicationChecker
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import InstanceConfig_T
from paasta_tools.utils import list_services
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import SPACER

try:
    import yelp_meteorite
except ImportError:
    yelp_meteorite = None

log = logging.getLogger(__name__)

CheckServiceReplication = Callable[
    [
        Arg(InstanceConfig_T, "instance_config"),
        Arg(Sequence[V1Pod], "all_pods"),
        Arg(Any, "replication_checker"),
        NamedArg(bool, "dry_run"),
    ],
    Optional[bool],
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    parser.add_argument(
        "--crit",
        dest="under_replicated_crit_pct",
        type=float,
        default=10,
        help="The percentage of under replicated service instances past which "
        "the script will return a critical status",
    )
    parser.add_argument(
        "--min-count-critical",
        dest="min_count_critical",
        type=int,
        default=5,
        help="The script will not return a critical status if the number of "
        "under replicated service instances is below this number, even if the "
        "percentage is above the critical percentage.",
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
    parser.add_argument(
        "--dry-run",
        action="store_true",
        dest="dry_run",
        help="Print Sensu alert events and metrics instead of sending them",
    )
    parser.add_argument(
        "--additional-namespaces",
        help="full names of namespaces to check services replication for that don't match --namespace-prefix"
        "Used only when service is kubernetes",
        dest="additional_namespaces",
        nargs="+",
        # we default this to paasta since we always want to run this check on paasta namespace
        # to avoid having two cron jobs running with two different namespace-prefix
        default=["paasta"],
    )
    parser.add_argument(
        "--eks",
        help="This flag checks k8 services running on EKS",
        dest="eks",
        action="store_true",
        default=False,
    )
    options = parser.parse_args()

    return options


def check_services_replication(
    soa_dir: str,
    cluster: str,
    service_instances: Sequence[str],
    instance_type_class: Type[InstanceConfig_T],
    check_service_replication: CheckServiceReplication,
    replication_checker: ReplicationChecker,
    all_pods: Sequence[V1Pod],
    dry_run: bool = False,
) -> Tuple[int, int]:
    service_instances_set = set(service_instances)
    replication_statuses: List[bool] = []

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
                is_well_replicated = check_service_replication(
                    instance_config=instance_config,
                    all_pods=all_pods,
                    replication_checker=replication_checker,
                    dry_run=dry_run,
                )
                if is_well_replicated is not None:
                    replication_statuses.append(is_well_replicated)

            else:
                log.debug(
                    "%s is not deployed. Skipping replication monitoring."
                    % instance_config.job_id
                )

    num_under_replicated = len(
        [status for status in replication_statuses if status is False]
    )
    return num_under_replicated, len(replication_statuses)


def emit_cluster_replication_metrics(
    pct_under_replicated: float,
    cluster: str,
    scheduler: str,
    dry_run: bool = False,
) -> None:
    metric_name = "paasta.pct_services_under_replicated"
    if dry_run:
        print(f"Would've sent value {pct_under_replicated} for metric '{metric_name}'")
    else:
        meteorite_dims = {"paasta_cluster": cluster, "scheduler": scheduler}
        gauge = yelp_meteorite.create_gauge(metric_name, meteorite_dims)
        gauge.set(pct_under_replicated)


def main(
    instance_type_class: Type[InstanceConfig_T],
    check_service_replication: CheckServiceReplication,
    namespace: str = None,
) -> None:
    args = parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    system_paasta_config = load_system_paasta_config()
    cluster = system_paasta_config.get_cluster()
    replication_checker: ReplicationChecker

    timer = metrics_lib.system_timer(dimensions=dict(eks=args.eks, cluster=cluster))

    timer.start()

    if namespace:
        pods, nodes = get_kubernetes_pods_and_nodes(namespace=namespace)
        replication_checker = KubeSmartstackEnvoyReplicationChecker(
            nodes=nodes,
            system_paasta_config=system_paasta_config,
        )
    else:
        pods, nodes = get_kubernetes_pods_and_nodes(
            additional_namespaces=args.additional_namespaces,
        )
        replication_checker = KubeSmartstackEnvoyReplicationChecker(
            nodes=nodes,
            system_paasta_config=system_paasta_config,
        )

    count_under_replicated, total = check_services_replication(
        soa_dir=args.soa_dir,
        cluster=cluster,
        service_instances=args.service_instance_list,
        instance_type_class=instance_type_class,
        check_service_replication=check_service_replication,
        replication_checker=replication_checker,
        all_pods=pods,
        dry_run=args.dry_run,
    )
    pct_under_replicated = 0 if total == 0 else 100 * count_under_replicated / total
    if yelp_meteorite is not None:
        emit_cluster_replication_metrics(
            pct_under_replicated,
            cluster,
            scheduler="kubernetes",
            dry_run=args.dry_run,
        )

    exit_code = 0
    if (
        pct_under_replicated >= args.under_replicated_crit_pct
        and count_under_replicated >= args.min_count_critical
    ):
        log.critical(
            f"{pct_under_replicated}% of instances ({count_under_replicated}/{total}) "
            f"are under replicated (past {args.under_replicated_crit_pct} is critical)!"
        )
        exit_code = 2

    timer.stop(tmp_dimensions={"result": exit_code})
    logging.info(
        f"Stopping timer for {cluster} (eks={args.eks}) with result {exit_code}: {timer()}ms elapsed"
    )
    sys.exit(exit_code)


def get_kubernetes_pods_and_nodes(
    namespace: Optional[str] = None,
    additional_namespaces: Optional[Container[str]] = None,
) -> Tuple[List[V1Pod], List[V1Node]]:
    kube_client = KubeClient()

    all_pods: List[V1Pod] = []
    if namespace:
        all_pods = get_all_pods(kube_client=kube_client, namespace=namespace)
    else:
        all_managed_namespaces = get_all_managed_namespaces(kube_client)
        for managed_namespace in all_managed_namespaces:
            all_pods.extend(
                get_all_pods(kube_client=kube_client, namespace=managed_namespace)
            )

    all_nodes = get_all_nodes(kube_client)

    return all_pods, all_nodes
