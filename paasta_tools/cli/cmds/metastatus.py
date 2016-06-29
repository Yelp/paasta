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
from paasta_tools.cli.utils import execute_paasta_metastatus_on_remote_master
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import list_clusters
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PaastaColors


def add_subparser(subparsers):
    status_parser = subparsers.add_parser(
        'metastatus',
        help="Display the status for an entire PaaSTA cluster",
        description=(
            "'paasta metastatus' is used to get the vital statistics about a PaaaSTA "
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
        '-g',
        '--groupings',
        nargs='+',
        default=['region'],
        help=(
            'Group resource information of slaves grouped by attribute.'
            'Note: This is only effective with -vv'
        )
    )
    status_parser.add_argument('-H', '--humanize', action='store_true', dest="humanize", default=True,
                               help="Print human-readable sizes")
    status_parser.set_defaults(command=paasta_metastatus)


def print_cluster_status(cluster, system_paasta_config, humanize, groupings, verbose=0):
    """With a given cluster and verboseness, returns the status of the cluster
    output is printed directly to provide dashbaords even if the cluster is unavailable"""
    print "Cluster: %s" % cluster
    print get_cluster_dashboards(cluster)
    print execute_paasta_metastatus_on_remote_master(
        cluster=cluster,
        system_paasta_config=system_paasta_config,
        humanize=humanize,
        groupings=groupings,
        verbose=verbose
    )
    print ""


def figure_out_clusters_to_inspect(args, all_clusters):
    if args.clusters is not None:
        clusters_to_inspect = args.clusters.split(",")
    else:
        clusters_to_inspect = all_clusters
    return clusters_to_inspect


def get_cluster_dashboards(cluster):
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
        for label, url in dashboards.items():
            output.append('  %s:%s%s' % (label, SPACER * (spacing - len(label)), PaastaColors.cyan(url)))
    return '\n'.join(output)


def paasta_metastatus(args):
    """Print the status of a PaaSTA clusters"""
    soa_dir = args.soa_dir
    system_paasta_config = load_system_paasta_config()
    all_clusters = list_clusters(soa_dir=soa_dir)
    clusters_to_inspect = figure_out_clusters_to_inspect(args, all_clusters)
    for cluster in clusters_to_inspect:
        if cluster in all_clusters:
            print_cluster_status(
                cluster=cluster,
                system_paasta_config=system_paasta_config,
                humanize=args.humanize,
                groupings=args.groupings,
                verbose=args.verbose
            )
        else:
            print "Cluster %s doesn't look like a valid cluster?" % args.clusters
            print "Try using tab completion to help complete the cluster name"
