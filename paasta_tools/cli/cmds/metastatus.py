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
import os
from distutils.util import strtobool
from typing import Sequence
from typing import Tuple

from bravado.exception import HTTPError

from paasta_tools.api.client import get_paasta_api_client
from paasta_tools.cli.utils import execute_paasta_metastatus_on_remote_master
from paasta_tools.cli.utils import get_paasta_metastatus_cmd_args
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import list_clusters
from paasta_tools.utils import list_services
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import SystemPaastaConfig


def add_subparser(
    subparsers,
) -> None:
    status_parser = subparsers.add_parser(
        'metastatus',
        help="Display the status for an entire PaaSTA cluster",
        description=(
            "'paasta metastatus' is used to get the vital statistics about a PaaSTA "
            "cluster as a whole. This tool is helpful when answering the question: 'Is "
            "it just my service or the whole cluster that is broken?'\n\n"
            "metastatus operates by ssh'ing to a Mesos master of a remote cluster, and "
            "querying the local APIs."
        ),
        epilog=(
            "The metastatus command may time out during heavy load. When that happens "
            "users may execute the ssh command directly, in order to bypass the timeout."
        ),
    )
    status_parser.add_argument(
        '-v', '--verbose',
        action='count',
        dest="verbose",
        default=0,
        help="""Print out more output regarding the state of the cluster.
        Multiple v options increase verbosity. Maximum is 3.""",
    )
    clusters_help = (
        'A comma separated list of clusters to view. Defaults to view all clusters. '
        'Try: --clusters norcal-prod,nova-prod'
    )
    status_parser.add_argument(
        '-c', '--clusters',
        help=clusters_help,
    ).completer = lazy_choices_completer(list_clusters)
    status_parser.add_argument(
        '-d', '--soa-dir',
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    status_parser.add_argument(
        '-a', '--autoscaling-info',
        action='store_true',
        default=False,
        dest="autoscaling_info",
        help="Show cluster autoscaling info, implies -vv",
    )
    status_parser.add_argument(
        '--use-mesos-cache',
        action='store_true',
        default=False,
        dest="use_mesos_cache",
        help="Use Mesos cache for state.json and frameworks",
    )
    status_parser.add_argument(
        '-g',
        '--groupings',
        nargs='+',
        default=['region'],
        help=(
            'Group resource information of slaves grouped by attribute.'
            'Note: This is only effective with -vv'
        ),
    )
    # The service and instance args default to None if not specified.
    status_parser.add_argument(
        '-s',
        '--service',
        help=(
            'Show how many of a given service instance can be run on a cluster slave.'
            'Note: This is only effective with -vvv and --instance must also be specified'
        ),
    ).completer = lazy_choices_completer(list_services)
    status_parser.add_argument(
        '-i',
        '--instance',
        help=(
            'Show how many of a given service instance can be run on a cluster slave.'
            'Note: This is only effective with -vvv and --service must also be specified'
        ),
    )
    status_parser.set_defaults(command=paasta_metastatus)


def paasta_metastatus_on_api_endpoint(
    cluster: str,
    system_paasta_config: SystemPaastaConfig,
    groupings: Sequence[str],
    verbose: int,
    autoscaling_info: bool = False,
    use_mesos_cache: bool = False,
) -> Tuple[int, str]:
    client = get_paasta_api_client(cluster, system_paasta_config)
    if not client:
        paasta_print('Cannot get a paasta-api client')
        exit(1)

    try:
        cmd_args, _ = get_paasta_metastatus_cmd_args(
            groupings=groupings,
            verbose=verbose,
            autoscaling_info=autoscaling_info,
            use_mesos_cache=use_mesos_cache,
        )
        res = client.metastatus.metastatus(cmd_args=[str(arg) for arg in cmd_args]).result()
        output, exit_code = res.output, res.exit_code
    except HTTPError as exc:
        output, exit_code = exc.response.text, exc.status_code

    return exit_code, output


def print_cluster_status(
    cluster: str,
    system_paasta_config: SystemPaastaConfig,
    groupings: Sequence[str],
    verbose: int = 0,
    autoscaling_info: bool = False,
    use_mesos_cache: bool = False,
    use_api_endpoint: bool = False,
) -> int:
    """With a given cluster and verboseness, returns the status of the cluster
    output is printed directly to provide dashboards even if the cluster is unavailable"""
    if use_api_endpoint:
        metastatus_func = paasta_metastatus_on_api_endpoint
    else:
        metastatus_func = execute_paasta_metastatus_on_remote_master

    return_code, output = metastatus_func(
        cluster=cluster,
        system_paasta_config=system_paasta_config,
        groupings=groupings,
        verbose=verbose,
        autoscaling_info=autoscaling_info,
        use_mesos_cache=use_mesos_cache,
    )

    paasta_print("Cluster: %s" % cluster)
    paasta_print(get_cluster_dashboards(cluster))
    paasta_print(output)
    paasta_print()

    return return_code


def figure_out_clusters_to_inspect(
    args,
    all_clusters,
) -> Sequence[str]:
    if args.clusters is not None:
        clusters_to_inspect = args.clusters.split(",")
    else:
        clusters_to_inspect = all_clusters
    return clusters_to_inspect


def get_cluster_dashboards(
    cluster: str,
) -> str:
    """Returns the direct dashboards for humans to use for a given cluster"""
    SPACER = ' '
    try:
        dashboards = load_system_paasta_config().get_dashboard_links()[cluster]
    except KeyError as e:
        if e.args[0] == cluster:
            output = [PaastaColors.red('No dashboards configured for %s!' % cluster)]
        else:
            output = [PaastaColors.red('No dashboards configured!')]
    else:
        output = ['Dashboards:']
        spacing = max((len(label) for label in dashboards.keys())) + 1
        for label, urls in dashboards.items():
            if isinstance(urls, list):
                urls = "\n    %s" % '\n    '.join(urls)
            output.append('  {}:{}{}'.format(label, SPACER * (spacing - len(label)), PaastaColors.cyan(urls)))
    return '\n'.join(output)


def paasta_metastatus(
    args,
) -> int:
    """Print the status of a PaaSTA clusters"""
    soa_dir = args.soa_dir
    system_paasta_config = load_system_paasta_config()

    if 'USE_API_ENDPOINT' in os.environ:
        use_api_endpoint = strtobool(os.environ['USE_API_ENDPOINT'])
    else:
        use_api_endpoint = False

    all_clusters = list_clusters(soa_dir=soa_dir)
    clusters_to_inspect = figure_out_clusters_to_inspect(args, all_clusters)
    return_codes = []
    for cluster in clusters_to_inspect:
        if cluster in all_clusters:
            return_codes.append(
                print_cluster_status(
                    cluster=cluster,
                    system_paasta_config=system_paasta_config,
                    groupings=args.groupings,
                    verbose=args.verbose,
                    autoscaling_info=args.autoscaling_info,
                    use_mesos_cache=args.use_mesos_cache,
                    use_api_endpoint=use_api_endpoint,
                ),
            )
        else:
            paasta_print("Cluster %s doesn't look like a valid cluster?" % args.clusters)
            paasta_print("Try using tab completion to help complete the cluster name")
    return 0 if all([return_code == 0 for return_code in return_codes]) else 1
