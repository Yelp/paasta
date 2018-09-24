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
from typing import List
from typing import MutableSequence
from typing import Optional
from typing import Sequence
from typing import Tuple

import a_sync
import chronos
from marathon.exceptions import MarathonError
from mypy_extensions import TypedDict

from paasta_tools import __version__
from paasta_tools.autoscaling.autoscaling_cluster_lib import AutoscalingInfo
from paasta_tools.autoscaling.autoscaling_cluster_lib import get_autoscaling_info_for_all_resources
from paasta_tools.chronos_tools import get_chronos_client
from paasta_tools.chronos_tools import load_chronos_config
from paasta_tools.cli.utils import get_instance_config
from paasta_tools.marathon_tools import get_marathon_clients
from paasta_tools.marathon_tools import get_marathon_servers
from paasta_tools.mesos.exceptions import MasterNotAvailableException
from paasta_tools.mesos.master import MesosState
from paasta_tools.mesos_tools import get_mesos_master
from paasta_tools.metrics import metastatus_lib
from paasta_tools.metrics.metastatus_lib import ResourceUtilization
from paasta_tools.utils import format_table
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import print_with_indent
from paasta_tools.utils import set_paasta_print_file


log = logging.getLogger('paasta_metastatus')
logging.basicConfig()
# kazoo can be really noisy - turn it down
logging.getLogger("kazoo").setLevel(logging.CRITICAL)
logging.getLogger("paasta_tools.autoscaling.autoscaling_cluster_lib").setLevel(logging.ERROR)

ServiceInstanceStats = TypedDict('ServiceInstanceStats', {'mem': float, 'cpus': float, 'disk': float, 'gpus': float})


class FatalError(Exception):
    def __init__(
        self,
        exit_code: int,
    ) -> None:
        self.exit_code = exit_code


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description='',
    )
    parser.add_argument(
        '-g',
        '--groupings',
        nargs='+',
        default=['pool', 'region'],
        help=(
            'Group resource information of slaves grouped by attribute.'
            'Note: This is only effective with -vv'
        ),
    )
    parser.add_argument('-t', '--threshold', type=int, default=90)
    parser.add_argument('--use-mesos-cache', action='store_true', default=False)
    parser.add_argument(
        '-a', '--autoscaling-info', action='store_true', default=False,
        dest="autoscaling_info",
    )
    parser.add_argument(
        '-v', '--verbose', action='count', dest="verbose", default=0,
        help="Print out more output regarding the state of the cluster",
    )
    parser.add_argument(
        '-s', '--service',
        help=(
            'Show how many of a given service instance can be run on a cluster slave.'
            'Note: This is only effective with -vvv and --instance must also be specified'
        ),
    )
    parser.add_argument(
        '-i', '--instance',
        help=(
            'Show how many of a given service instance can be run on a cluster slave.'
            'Note: This is only effective with -vvv and --service must also be specified'
        ),
    )
    return parser.parse_args(argv)


def get_marathon_framework_ids(marathon_clients):
    return [client.get_info().framework_id for client in marathon_clients]


def _run_mesos_checks(mesos_master, mesos_state):
    mesos_state_status = metastatus_lib.get_mesos_state_status(mesos_state)

    metrics = a_sync.block(mesos_master.metrics_snapshot)
    mesos_metrics_status = metastatus_lib.get_mesos_resource_utilization_health(
        mesos_metrics=metrics,
        mesos_state=mesos_state,
    )
    return mesos_state_status + mesos_metrics_status


def _run_marathon_checks(marathon_clients):
    try:
        marathon_results = metastatus_lib.get_marathon_status(marathon_clients)
        return marathon_results
    except (MarathonError, ValueError) as e:
        paasta_print(PaastaColors.red(f"CRITICAL: Unable to contact Marathon cluster: {e}"))
        raise FatalError(2)


def all_marathon_clients(marathon_clients):
    return [c for c in itertools.chain(marathon_clients.current, marathon_clients.previous)]


def utilization_table_by_grouping_from_mesos_state(
    groupings: Sequence[str],
    threshold: float,
    mesos_state: MesosState,
    service_instance_stats: Optional[ServiceInstanceStats] = None,
) -> Tuple[
    List[List[str]],
    bool,
]:
    grouping_function = metastatus_lib.key_func_for_attribute_multi(groupings)
    resource_info_dict_grouped = metastatus_lib.get_resource_utilization_by_grouping(
        grouping_function,
        mesos_state,
    )

    static_headers = [
        'CPU (used/total)',
        'RAM (used/total)',
        'Disk (used/total)',
        'GPU (used/total)',
        'Agent count',
    ]
    # service_instance_stats could be None so check and insert a header if needed.
    if service_instance_stats:
        # Insert so agent count is still last
        static_headers.insert(-1, 'Slots + Limiting Resource')

    all_rows = [
        [grouping.capitalize() for grouping in groupings] + static_headers,
    ]
    table_rows = []

    for grouping_values, resource_info_dict in resource_info_dict_grouped.items():
        resource_utilizations = metastatus_lib.resource_utillizations_from_resource_info(
            total=resource_info_dict['total'],
            free=resource_info_dict['free'],
        )
        healthcheck_utilization_pairs = [
            metastatus_lib.healthcheck_result_resource_utilization_pair_for_resource_utilization(
                utilization,
                threshold,
            )
            for utilization in resource_utilizations
        ]
        healthy_exit = all(pair[0].healthy for pair in healthcheck_utilization_pairs)
        table_rows.append(metastatus_lib.get_table_rows_for_resource_info_dict(
            [v for g, v in grouping_values],
            healthcheck_utilization_pairs,
        ))
        # Fill table rows with service-instance data if possible.
        if service_instance_stats:
            fill_table_rows_with_service_instance_stats(service_instance_stats, resource_utilizations, table_rows)

        # Always append the agent count last
        table_rows[-1].append(str(resource_info_dict['slave_count']))

    table_rows = sorted(table_rows, key=lambda x: x[0:len(groupings)])
    all_rows.extend(table_rows)

    return all_rows, healthy_exit


def fill_table_rows_with_service_instance_stats(
    service_instance_stats: ServiceInstanceStats,
    resource_utilizations: Sequence[ResourceUtilization],
    table_rows: MutableSequence[MutableSequence[str]],
) -> None:
    # Calculate the max number of runnable service instances given the current resources (e.g. cpus, mem, disk)
    resource_free_dict = {rsrc.metric: rsrc.free for rsrc in resource_utilizations}
    num_service_instances_allowed = float('inf')
    limiting_factor = 'Unknown'
    # service_instance_stats.keys() should be a subset of resource_free_dict
    for rsrc_name, rsrc_amt_wanted in service_instance_stats.items():
        if rsrc_amt_wanted > 0:
            # default=0 to indicate there is none of that resource
            rsrc_free = resource_free_dict.get(rsrc_name, 0)
            if rsrc_free // rsrc_amt_wanted < num_service_instances_allowed:
                limiting_factor = rsrc_name
                num_service_instances_allowed = rsrc_free // rsrc_amt_wanted
    table_rows[-1].append('{:6} ; {}'.format(int(num_service_instances_allowed), limiting_factor))


def get_service_instance_stats(service: str, instance: str, cluster: str) -> Optional[ServiceInstanceStats]:
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
        service_instance_stats = ServiceInstanceStats(
            mem=instance_config.get_mem(),
            cpus=instance_config.get_cpus(),
            disk=instance_config.get_disk(),
            gpus=instance_config.get_gpus(),
        )
        return service_instance_stats
    except Exception as e:
        log.error(f'Failed to get stats for service {service} instance {instance}: {str(e)}')
        return None


def print_output(argv: Optional[List[str]]=None) -> None:
    chronos_config = None
    args = parse_args(argv)

    system_paasta_config = load_system_paasta_config()

    master_kwargs = {}
    # we don't want to be passing False to not override a possible True
    # value from system config
    if args.use_mesos_cache:
        master_kwargs['use_mesos_cache'] = True
    master = get_mesos_master(**master_kwargs)

    marathon_servers = get_marathon_servers(system_paasta_config)
    marathon_clients = all_marathon_clients(get_marathon_clients(marathon_servers))

    try:
        mesos_state = a_sync.block(master.state)
        all_mesos_results = _run_mesos_checks(
            mesos_master=master,
            mesos_state=mesos_state,
        )
    except MasterNotAvailableException as e:
        # if we can't connect to master at all,
        # then bomb out early
        paasta_print(PaastaColors.red("CRITICAL:  %s" % '\n'.join(e.args)))
        raise FatalError(2)

    # Check to see if Chronos should be running here by checking for config
    chronos_config = load_chronos_config()

    if chronos_config:
        chronos_client = get_chronos_client(chronos_config, cached=True)
        try:
            chronos_results = metastatus_lib.get_chronos_status(chronos_client)
        except (chronos.ChronosAPIError) as e:
            paasta_print(PaastaColors.red("CRITICAL: Unable to contact Chronos! Error: %s" % e))
            raise FatalError(2)
    else:
        chronos_results = [metastatus_lib.HealthCheckResult(
            message='Chronos is not configured to run here',
            healthy=True,
        )]

    marathon_results = _run_marathon_checks(marathon_clients)

    mesos_ok = all(metastatus_lib.status_for_results(all_mesos_results))
    marathon_ok = all(metastatus_lib.status_for_results(marathon_results))
    chronos_ok = all(metastatus_lib.status_for_results(chronos_results))

    mesos_summary = metastatus_lib.generate_summary_for_check("Mesos", mesos_ok)
    marathon_summary = metastatus_lib.generate_summary_for_check("Marathon", marathon_ok)
    chronos_summary = metastatus_lib.generate_summary_for_check("Chronos", chronos_ok)

    healthy_exit = True if all([mesos_ok, marathon_ok, chronos_ok]) else False

    paasta_print(f"Master paasta_tools version: {__version__}")
    metastatus_lib.print_results_for_healthchecks(mesos_summary, mesos_ok, all_mesos_results, args.verbose)
    if args.verbose > 1:
        print_with_indent('Resources Grouped by %s' % ", ".join(args.groupings), 2)
        all_rows, healthy_exit = utilization_table_by_grouping_from_mesos_state(
            groupings=args.groupings,
            threshold=args.threshold,
            mesos_state=mesos_state,
        )
        for line in format_table(all_rows):
            print_with_indent(line, 4)

        if args.autoscaling_info:
            print_with_indent("Autoscaling resources:", 2)
            headers = [field.replace("_", " ").capitalize() for field in AutoscalingInfo._fields]
            table = [headers] + [[str(x) for x in asi] for asi in get_autoscaling_info_for_all_resources(mesos_state)]

            for line in format_table(table):
                print_with_indent(line, 4)

        if args.verbose >= 3:
            print_with_indent('Per Slave Utilization', 2)
            cluster = system_paasta_config.get_cluster()
            service_instance_stats = get_service_instance_stats(args.service, args.instance, cluster)
            if service_instance_stats:
                print_with_indent('Service-Instance stats:' + str(service_instance_stats), 2)
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
    metastatus_lib.print_results_for_healthchecks(marathon_summary, marathon_ok, marathon_results, args.verbose)
    metastatus_lib.print_results_for_healthchecks(chronos_summary, chronos_ok, chronos_results, args.verbose)

    if not healthy_exit:
        raise FatalError(2)


def get_output(argv: Optional[List[str]]=None) -> Tuple[str, int]:
    output = io.StringIO()
    exit_code = 1
    with set_paasta_print_file(output):
        exit_code = 0
        try:
            print_output(argv)
        except FatalError as e:
            exit_code = e.exit_code
    ret = output.getvalue()
    return ret, exit_code


def main(argv: Optional[List[str]]=None) -> None:
    exit_code = 0
    try:
        print_output(argv)
    except FatalError as e:
        exit_code = e.exit_code
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
