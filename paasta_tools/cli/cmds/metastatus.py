#!/usr/bin/env python
# Copyright 2015 Yelp Inc.
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
from paasta_tools.smartstack_tools import DEFAULT_SYNAPSE_PORT
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import list_clusters


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
        action='store_true',
        dest="verbose",
        default=False,
        help="Print out more output regarding the state of the cluster",
    )
    clusters_help = (
        'A comma separated list of clusters to view. Defaults to view all clusters. '
        'Try: --clusters norcal-prod,nova-prod'
    )
    status_parser.add_argument(
        '-c', '--clusters',
        help=clusters_help,
    ).completer = lazy_choices_completer(list_clusters)
    status_parser.set_defaults(command=paasta_metastatus)


def print_cluster_status(cluster, verbose=False):
    """With a given cluster and verboseness, returns the status of the cluster
    output is printed directly to provide dashbaords even if the cluster is unavailable"""
    print "Cluster: %s" % cluster
    print get_cluster_dashboards(cluster)
    print execute_paasta_metastatus_on_remote_master(cluster, verbose)
    print ""


def figure_out_clusters_to_inspect(args, all_clusters):
    if args.clusters is not None:
        clusters_to_inspect = args.clusters.split(",")
    else:
        clusters_to_inspect = all_clusters
    return clusters_to_inspect


def get_cluster_dashboards(cluster):
    """Returns the direct dashboards for humans to use for a given cluster"""
    output = []
    output.append("Warning: Dashboards in prod are not directly reachable. "
                  "See http://y/paasta-troubleshooting for instructions. (search for 'prod dashboards')")
    output.append("User Dashboards (Read Only):")
    output.append("  Mesos:    %s" % PaastaColors.cyan("http://mesos.paasta-%s.yelp/" % cluster))
    output.append("  Marathon: %s" % PaastaColors.cyan("http://marathon.paasta-%s.yelp/" % cluster))
    output.append("  Chronos:  %s" % PaastaColors.cyan("http://chronos.paasta-%s.yelp/" % cluster))
    output.append("  Synapse:  %s" % PaastaColors.cyan("http://paasta-%s.yelp:%s/" % (cluster, DEFAULT_SYNAPSE_PORT)))
    output.append("Admin Dashboards (Read/write, requires secrets):")
    output.append("  Mesos:    %s" % PaastaColors.cyan("http://paasta-%s.yelp:5050/" % cluster))
    output.append("  Marathon: %s" % PaastaColors.cyan("http://paasta-%s.yelp:5052/" % cluster))
    output.append("  Chronos:  %s" % PaastaColors.cyan("http://paasta-%s.yelp:5053/" % cluster))
    return '\n'.join(output)


def paasta_metastatus(args):
    """Print the status of a PaaSTA clusters"""
    all_clusters = list_clusters()
    clusters_to_inspect = figure_out_clusters_to_inspect(args, all_clusters)
    for cluster in clusters_to_inspect:
        if cluster in all_clusters:
            print_cluster_status(cluster, args.verbose)
        else:
            print "Cluster %s doesn't look like a valid cluster?" % args.clusters
            print "Try using tab completion to help complete the cluster name"
