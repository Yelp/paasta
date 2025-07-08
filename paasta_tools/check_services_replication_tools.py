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
from multiprocessing import Pool
from os import cpu_count
from typing import Any
from typing import Callable
from typing import cast
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple
from typing import Type

from mypy_extensions import Arg
from mypy_extensions import NamedArg

from paasta_tools.kubernetes_tools import get_all_managed_namespaces
from paasta_tools.kubernetes_tools import get_all_nodes
from paasta_tools.kubernetes_tools import get_all_pods
from paasta_tools.kubernetes_tools import group_pods_by_service_instance
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
        Arg(Dict[str, Dict[str, List[V1Pod]]], "pods_by_service_instance"),
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
    pods_by_service_instance: Dict[str, Dict[str, List[V1Pod]]],
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
                    pods_by_service_instance=pods_by_service_instance,
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
        pods, nodes = get_kubernetes_pods_and_nodes()
        replication_checker = KubeSmartstackEnvoyReplicationChecker(
            nodes=nodes,
            system_paasta_config=system_paasta_config,
        )

    pods_by_service_instance = group_pods_by_service_instance(pods)

    count_under_replicated, total = check_services_replication(
        soa_dir=args.soa_dir,
        cluster=cluster,
        service_instances=args.service_instance_list,
        instance_type_class=instance_type_class,
        check_service_replication=check_service_replication,
        replication_checker=replication_checker,
        pods_by_service_instance=pods_by_service_instance,
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


# XXX: is there a base class for the k8s clientlib models that we could use to type `obj`?
def set_local_vars_configuration_to_none(obj: Any, visited: Set[int] = None) -> None:
    """
    Recursive function to ensure that k8s clientlib objects are pickleable.

    Without this, k8s clientlib objects can't be used by multiprocessing functions
    as those pickle data to shuttle between processes.
    """
    if visited is None:
        visited = set()

    # Avoid infinite recursion for objects that have already been visited
    obj_id = id(obj)
    if obj_id in visited:
        return
    visited.add(obj_id)

    # if the object has the attribute, set it to None to essentially delete it
    if hasattr(obj, "local_vars_configuration"):
        setattr(obj, "local_vars_configuration", None)

    # recursively check attributes of the object
    if hasattr(obj, "__dict__"):
        for attr_name, attr_value in obj.__dict__.items():
            set_local_vars_configuration_to_none(attr_value, visited)

    # if the object is iterable/a collection, iterate over its elements
    elif isinstance(obj, (list, tuple, set)):
        for item in obj:
            set_local_vars_configuration_to_none(item, visited)
    elif isinstance(obj, dict):
        for value in obj.values():
            set_local_vars_configuration_to_none(value, visited)


def __fetch_pods(namespace: str) -> List[V1Pod]:
    kube_client = KubeClient()
    pods = get_all_pods(kube_client, namespace)
    for pod in pods:
        # this is pretty silly, but V1Pod cannot be pickled otherwise since the local_vars_configuration member
        # is not picklable - and pretty much every k8s model has this member ;_;
        set_local_vars_configuration_to_none(pod)
    return pods


def __get_all_pods_parallel(from_namespaces: Set[str]) -> List[V1Pod]:
    all_pods: List[V1Pod] = []
    with Pool() as pool:
        for pod_list in pool.imap_unordered(
            __fetch_pods,
            from_namespaces,
            chunksize=len(from_namespaces) // cast(int, cpu_count()),
        ):
            all_pods.extend(pod_list)
    return all_pods


def get_kubernetes_pods_and_nodes(
    namespace: Optional[str] = None,
) -> Tuple[List[V1Pod], List[V1Node]]:
    kube_client = KubeClient()

    if namespace:
        all_pods = get_all_pods(kube_client=kube_client, namespace=namespace)
    else:
        all_managed_namespaces = set(get_all_managed_namespaces(kube_client))
        all_pods = __get_all_pods_parallel(all_managed_namespaces)

    all_nodes = get_all_nodes(kube_client)

    return all_pods, all_nodes
