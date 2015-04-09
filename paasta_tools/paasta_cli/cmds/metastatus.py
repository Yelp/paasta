#!/usr/bin/env python
from paasta_tools.marathon_tools import list_clusters
from paasta_tools.paasta_cli.utils import execute_paasta_metastatus_on_remote_master
from paasta_tools.paasta_cli.utils import lazy_choices_completer


def add_subparser(subparsers):
    status_parser = subparsers.add_parser(
        'metastatus',
        help="Display the status for an entire PaaSTA cluster")
    status_parser.add_argument('-v', '--verbose', action='store_true',
                               dest="verbose", default=False,
                               help="Print out more output regarding the state of the cluster")
    clusters_help = (
        'A comma separated list of clusters to view. Defaults to view all clusters. '
        'Try: --clusters norcal-prod,nova-prod'
    )
    status_parser.add_argument(
        '-c', '--clusters',
        help=clusters_help,
    ).completer = lazy_choices_completer(list_clusters)
    status_parser.set_defaults(command=paasta_metastatus)


def report_cluster_status(cluster, verbose=False):
    """With a given cluster and verboseness, returns the status of the cluster"""
    output = []
    output.append("cluster: %s" % cluster)
    output.append(execute_paasta_metastatus_on_remote_master(cluster, verbose))
    return '\n'.join(output)


def figure_out_clusters_to_inspect(args, all_clusters):
    if args.clusters is not None:
        clusters_to_inspect = args.clusters.split(",")
    else:
        clusters_to_inspect = all_clusters
    return clusters_to_inspect


def paasta_metastatus(args):
    """Print the status of a PaaSTA clusters"""
    all_clusters = list_clusters()
    clusters_to_inspect = figure_out_clusters_to_inspect(args, all_clusters)
    for cluster in clusters_to_inspect:
        if cluster in all_clusters:
            print report_cluster_status(cluster, args.verbose)
            print ""
        else:
            print "Cluster %s doesn't look like a valid cluster?"
            print "Try using tab completion to help complete the cluster name"
