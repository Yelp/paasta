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
import argparse
import io
import itertools
import logging
import sys
from contextlib import redirect_stdout
from typing import Mapping
from typing import MutableSequence
from typing import Optional
from typing import Sequence
from typing import Tuple

import a_sync
from marathon.exceptions import MarathonError
from mypy_extensions import TypedDict

from paasta_tools import __version__
from paasta_tools.cli.utils import get_instance_config
from paasta_tools.kubernetes_tools import is_kubernetes_available
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.marathon_tools import get_marathon_clients
from paasta_tools.marathon_tools import get_marathon_servers
from paasta_tools.marathon_tools import MarathonClient
from paasta_tools.marathon_tools import MarathonClients
from paasta_tools.mesos.exceptions import MasterNotAvailableException
from paasta_tools.mesos.master import MesosMaster
from paasta_tools.mesos.master import MesosState
from paasta_tools.mesos_tools import get_mesos_config_path
from paasta_tools.mesos_tools import get_mesos_leader
from paasta_tools.mesos_tools import get_mesos_master
from paasta_tools.mesos_tools import is_mesos_available
from paasta_tools.metrics import metastatus_lib
from paasta_tools.metrics.metastatus_lib import _GenericNodeGroupingFunctionT
from paasta_tools.metrics.metastatus_lib import _KeyFuncRetT
from paasta_tools.metrics.metastatus_lib import HealthCheckResult
from paasta_tools.metrics.metastatus_lib import ResourceUtilization
from paasta_tools.metrics.metastatus_lib import ResourceUtilizationDict
from paasta_tools.utils import format_table
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import print_with_indent


log = logging.getLogger("paasta_metastatus")
logging.basicConfig()
# kazoo can be really noisy - turn it down
logging.getLogger("kazoo").setLevel(logging.CRITICAL)

ServiceInstanceStats = TypedDict(
    "ServiceInstanceStats", {"mem": float, "cpus": float, "disk": float, "gpus": int}
)


class FatalError(Exception):
    def __init__(self, exit_code: int) -> None:
        self.exit_code = exit_code


def parse_args(argv):
    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "-g",
        "--groupings",
        nargs="+",
        default=["pool", "region"],
        help=(
            "Group resource information of slaves grouped by attribute."
            "Note: This is only effective with -vv"
        ),
    )
    parser.add_argument("-t", "--threshold", type=int, default=90)
    parser.add_argument("--use-mesos-cache", action="store_true", default=False)
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        dest="verbose",
        default=0,
        help="Print out more output regarding the state of the cluster",
    )
    parser.add_argument(
        "-s",
        "--service",
        help=(
            "Show how many of a given service instance can be run on a cluster slave."
            "Note: This is only effective with -vvv and --instance must also be specified"
        ),
    )
    parser.add_argument(
        "-i",
        "--instance",
        help=(
            "Show how many of a given service instance can be run on a cluster slave."
            "Note: This is only effective with -vvv and --service must also be specified"
        ),
    )
    return parser.parse_args(argv)


def get_marathon_framework_ids(
    marathon_clients: Sequence[MarathonClient],
) -> Sequence[str]:
    return [client.get_info().framework_id for client in marathon_clients]


def _run_mesos_checks(
    mesos_master: MesosMaster, mesos_state: MesosState
) -> Sequence[HealthCheckResult]:
    mesos_state_status = metastatus_lib.get_mesos_state_status(mesos_state)

    metrics = a_sync.block(mesos_master.metrics_snapshot)
    mesos_metrics_status = metastatus_lib.get_mesos_resource_utilization_health(
        mesos_metrics=metrics, mesos_state=mesos_state
    )
    return mesos_state_status + mesos_metrics_status  # type: ignore


def _run_marathon_checks(
    marathon_clients: Sequence[MarathonClient],
) -> Sequence[HealthCheckResult]:
    try:
        marathon_results = metastatus_lib.get_marathon_status(marathon_clients)
        return marathon_results
    except (MarathonError, ValueError) as e:
        print(PaastaColors.red(f"CRITICAL: Unable to contact Marathon cluster: {e}"))
        raise FatalError(2)


def all_marathon_clients(
    marathon_clients: MarathonClients,
) -> Sequence[MarathonClient]:
    return [
        c for c in itertools.chain(marathon_clients.current, marathon_clients.previous)
    ]


def utilization_table_by_grouping(
    groupings: Sequence[str],
    grouping_function: _GenericNodeGroupingFunctionT,
    resource_info_dict_grouped: Mapping[_KeyFuncRetT, ResourceUtilizationDict],
    threshold: float,
    service_instance_stats: Optional[ServiceInstanceStats] = None,
) -> Tuple[Sequence[MutableSequence[str]], bool]:
    static_headers = [
        "CPU (used/total)",
        "RAM (used/total)",
        "Disk (used/total)",
        "GPU (used/total)",
        "Agent count",
    ]
    # service_instance_stats could be None so check and insert a header if needed.
    if service_instance_stats:
        # Insert so agent count is still last
        static_headers.insert(-1, "Slots + Limiting Resource")

    all_rows = [[grouping.capitalize() for grouping in groupings] + static_headers]
    table_rows = []

    for grouping_values, resource_info_dict in resource_info_dict_grouped.items():
        resource_utilizations = (
            metastatus_lib.resource_utillizations_from_resource_info(
                total=resource_info_dict["total"], free=resource_info_dict["free"]
            )
        )
        healthcheck_utilization_pairs = [
            metastatus_lib.healthcheck_result_resource_utilization_pair_for_resource_utilization(
                utilization, threshold
            )
            for utilization in resource_utilizations
        ]
        healthy_exit = all(pair[0].healthy for pair in healthcheck_utilization_pairs)
        table_rows.append(
            metastatus_lib.get_table_rows_for_resource_info_dict(
                [v for g, v in grouping_values], healthcheck_utilization_pairs
            )
        )
        # Fill table rows with service-instance data if possible.
        if service_instance_stats:
            fill_table_rows_with_service_instance_stats(
                service_instance_stats, resource_utilizations, table_rows
            )

        # Always append the agent count last
        table_rows[-1].append(str(resource_info_dict["slave_count"]))

    table_rows = sorted(table_rows, key=lambda x: x[0 : len(groupings)])
    all_rows.extend(table_rows)

    return all_rows, healthy_exit


def utilization_table_by_grouping_from_mesos_state(
    groupings: Sequence[str],
    threshold: float,
    mesos_state: MesosState,
    service_instance_stats: Optional[ServiceInstanceStats] = None,
) -> Tuple[Sequence[MutableSequence[str]], bool]:
    grouping_function = metastatus_lib.key_func_for_attribute_multi(groupings)
    resource_info_dict_grouped = metastatus_lib.get_resource_utilization_by_grouping(
        grouping_function, mesos_state
    )

    return utilization_table_by_grouping(
        groupings,
        grouping_function,
        resource_info_dict_grouped,
        threshold,
        service_instance_stats,
    )


def utilization_table_by_grouping_from_kube(
    groupings: Sequence[str],
    threshold: float,
    kube_client: KubeClient,
    service_instance_stats: Optional[ServiceInstanceStats] = None,
) -> Tuple[Sequence[MutableSequence[str]], bool]:
    grouping_function = metastatus_lib.key_func_for_attribute_multi_kube(groupings)

    resource_info_dict_grouped = (
        metastatus_lib.get_resource_utilization_by_grouping_kube(
            grouping_function, kube_client
        )
    )

    return utilization_table_by_grouping(
        groupings,
        grouping_function,
        resource_info_dict_grouped,
        threshold,
        service_instance_stats,
    )


def fill_table_rows_with_service_instance_stats(
    service_instance_stats: ServiceInstanceStats,
    resource_utilizations: Sequence[ResourceUtilization],
    table_rows: MutableSequence[MutableSequence[str]],
) -> None:
    # Calculate the max number of runnable service instances given the current resources (e.g. cpus, mem, disk)
    resource_free_dict = {rsrc.metric: rsrc.free for rsrc in resource_utilizations}
    num_service_instances_allowed = float("inf")
    limiting_factor = "Unknown"
    # service_instance_stats.keys() should be a subset of resource_free_dict
    for rsrc_name, rsrc_amt_wanted in service_instance_stats.items():
        if rsrc_amt_wanted > 0:  # type: ignore
            # default=0 to indicate there is none of that resource
            rsrc_free = resource_free_dict.get(rsrc_name, 0)
            if (
                rsrc_free // rsrc_amt_wanted  # type: ignore
                < num_service_instances_allowed  # type: ignore
            ):
                limiting_factor = rsrc_name
                num_service_instances_allowed = (
                    rsrc_free // rsrc_amt_wanted  # type: ignore
                )
    table_rows[-1].append(
        "{:6} ; {}".format(int(num_service_instances_allowed), limiting_factor)
    )


def get_service_instance_stats(
    service: str, instance: str, cluster: str
) -> Optional[ServiceInstanceStats]:
    """Returns a Dict with stats about a given service instance.

    Args:
        service: the service name
        instance: the instance name
        cluster: the cluster name where the service instance will be searched for

    Returns:
        A Dict mapping resource name to the amount of that resource the particular service instance consumes.
    """
    if service is None or instance is None or cluster is None:
        return None

    try:
        instance_config = get_instance_config(service, instance, cluster)
        # Get all fields that are showed in the 'paasta metastatus -vvv' command
        if instance_config.get_gpus():
            gpus = int(instance_config.get_gpus())
        else:
            gpus = 0
        service_instance_stats = ServiceInstanceStats(
            mem=instance_config.get_mem(),
            cpus=instance_config.get_cpus(),
            disk=instance_config.get_disk(),
            gpus=gpus,
        )
        return service_instance_stats
    except Exception as e:
        log.error(
            f"Failed to get stats for service {service} instance {instance}: {str(e)}"
        )
        return None


def _run_kube_checks(
    kube_client: KubeClient,
) -> Sequence[HealthCheckResult]:
    kube_status = metastatus_lib.get_kube_status(kube_client)
    kube_metrics_status = metastatus_lib.get_kube_resource_utilization_health(
        kube_client=kube_client
    )
    return kube_status + kube_metrics_status  # type: ignore


def print_output(argv: Optional[Sequence[str]] = None) -> None:
    mesos_available = is_mesos_available()
    kube_available = is_kubernetes_available()

    args = parse_args(argv)

    system_paasta_config = load_system_paasta_config()

    if mesos_available:
        master_kwargs = {}
        # we don't want to be passing False to not override a possible True
        # value from system config
        if args.use_mesos_cache:
            master_kwargs["use_mesos_cache"] = True

        master = get_mesos_master(
            mesos_config_path=get_mesos_config_path(system_paasta_config),
            **master_kwargs,
        )

        marathon_servers = get_marathon_servers(system_paasta_config)
        marathon_clients = all_marathon_clients(get_marathon_clients(marathon_servers))

        try:
            mesos_state = a_sync.block(master.state)
            all_mesos_results = _run_mesos_checks(
                mesos_master=master, mesos_state=mesos_state
            )
        except MasterNotAvailableException as e:
            # if we can't connect to master at all,
            # then bomb out early
            print(PaastaColors.red("CRITICAL:  %s" % "\n".join(e.args)))
            raise FatalError(2)

        marathon_results = _run_marathon_checks(marathon_clients)
    else:
        marathon_results = [
            metastatus_lib.HealthCheckResult(
                message="Marathon is not configured to run here", healthy=True
            )
        ]
        all_mesos_results = [
            metastatus_lib.HealthCheckResult(
                message="Mesos is not configured to run here", healthy=True
            )
        ]

    if kube_available:
        kube_client = KubeClient()
        kube_results = _run_kube_checks(kube_client)
    else:
        kube_results = [
            metastatus_lib.HealthCheckResult(
                message="Kubernetes is not configured to run here", healthy=True
            )
        ]

    mesos_ok = all(metastatus_lib.status_for_results(all_mesos_results))
    marathon_ok = all(metastatus_lib.status_for_results(marathon_results))
    kube_ok = all(metastatus_lib.status_for_results(kube_results))

    mesos_summary = metastatus_lib.generate_summary_for_check("Mesos", mesos_ok)
    marathon_summary = metastatus_lib.generate_summary_for_check(
        "Marathon", marathon_ok
    )
    kube_summary = metastatus_lib.generate_summary_for_check("Kubernetes", kube_ok)

    healthy_exit = True if all([mesos_ok, marathon_ok]) else False

    print(f"Master paasta_tools version: {__version__}")
    print("Mesos leader: %s" % get_mesos_leader())
    metastatus_lib.print_results_for_healthchecks(
        mesos_summary, mesos_ok, all_mesos_results, args.verbose
    )
    if args.verbose > 1 and mesos_available:
        print_with_indent("Resources Grouped by %s" % ", ".join(args.groupings), 2)
        all_rows, healthy_exit = utilization_table_by_grouping_from_mesos_state(
            groupings=args.groupings, threshold=args.threshold, mesos_state=mesos_state
        )
        for line in format_table(all_rows):
            print_with_indent(line, 4)

        if args.verbose >= 3:
            print_with_indent("Per Slave Utilization", 2)
            cluster = system_paasta_config.get_cluster()
            service_instance_stats = get_service_instance_stats(
                args.service, args.instance, cluster
            )
            if service_instance_stats:
                print_with_indent(
                    "Service-Instance stats:" + str(service_instance_stats), 2
                )
            # print info about slaves here. Note that we don't make modifications to
            # the healthy_exit variable here, because we don't care about a single slave
            # having high usage.
            all_rows, _ = utilization_table_by_grouping_from_mesos_state(
                groupings=args.groupings + ["hostname"],
                threshold=args.threshold,
                mesos_state=mesos_state,
                service_instance_stats=service_instance_stats,
            )
            # The last column from utilization_table_by_grouping_from_mesos_state is "Agent count", which will always be
            # 1 for per-slave resources, so delete it.
            for row in all_rows:
                row.pop()

            for line in format_table(all_rows):
                print_with_indent(line, 4)
    metastatus_lib.print_results_for_healthchecks(
        marathon_summary, marathon_ok, marathon_results, args.verbose
    )
    metastatus_lib.print_results_for_healthchecks(
        kube_summary, kube_ok, kube_results, args.verbose
    )
    if args.verbose > 1 and kube_available:
        print_with_indent("Resources Grouped by %s" % ", ".join(args.groupings), 2)
        all_rows, healthy_exit = utilization_table_by_grouping_from_kube(
            groupings=args.groupings, threshold=args.threshold, kube_client=kube_client
        )
        for line in format_table(all_rows):
            print_with_indent(line, 4)

        if args.verbose >= 3:
            print_with_indent("Per Node Utilization", 2)
            cluster = system_paasta_config.get_cluster()
            service_instance_stats = get_service_instance_stats(
                args.service, args.instance, cluster
            )
            if service_instance_stats:
                print_with_indent(
                    "Service-Instance stats:" + str(service_instance_stats), 2
                )
            # print info about nodes here. Note that we don't make
            # modifications to the healthy_exit variable here, because we don't
            # care about a single node having high usage.
            all_rows, _ = utilization_table_by_grouping_from_kube(
                groupings=args.groupings + ["hostname"],
                threshold=args.threshold,
                kube_client=kube_client,
                service_instance_stats=service_instance_stats,
            )
            # The last column from utilization_table_by_grouping_from_kube is "Agent count", which will always be
            # 1 for per-node resources, so delete it.
            for row in all_rows:
                row.pop()

            for line in format_table(all_rows):
                print_with_indent(line, 4)

    if not healthy_exit:
        raise FatalError(2)


def get_output(argv: Optional[Sequence[str]] = None) -> Tuple[str, int]:
    output = io.StringIO()
    exit_code = 1
    with redirect_stdout(output):
        exit_code = 0
        try:
            print_output(argv)
        except FatalError as e:
            exit_code = e.exit_code
    ret = output.getvalue()
    return ret, exit_code


def main(argv: Optional[Sequence[str]] = None) -> None:
    exit_code = 0
    try:
        print_output(argv)
    except FatalError as e:
        exit_code = e.exit_code
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
