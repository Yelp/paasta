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
from paasta_tools.autoscaling import load_boost
from paasta_tools.cli.utils import execute_paasta_cluster_boost_on_remote_master
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import list_clusters
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print


def add_subparser(subparsers):
    boost_parser = subparsers.add_parser(
        'boost',
        help="Set, print the status, or clear a capacity boost for a given region in a PaaSTA cluster",
        description=(
            "'paasta boost' is used to temporary provision more capacity in a given cluster "
            "It operates by ssh'ing to a Mesos master of a remote cluster, and "
            "interacting with the boost in the local zookeeper cluster. If you set or clear "
            "a boost, you may want to run the cluster autoscaler manually afterward."
        ),
        epilog=(
            "The boost command may time out during heavy load. When that happens "
            "users may execute the ssh command directly, in order to bypass the timeout."
        ),
    )
    boost_parser.add_argument(
        '-v', '--verbose',
        action='count',
        dest="verbose",
        default=0,
        help="""Print out more output regarding the state of the cluster.
        Multiple v options increase verbosity. Maximum is 3.""",
    )
    boost_parser.add_argument(
        '-c', '--cluster',
        type=str,
        required=True,
        help="""Paasta cluster(s) to boost. This option can take comma separated values.
        If auto-completion doesn't work, you can get a list of cluster with `paasta list-clusters'""",
    ).completer = lazy_choices_completer(list_clusters)
    boost_parser.add_argument(
        '--soa-dir',
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    boost_parser.add_argument(
        '-p', '--pool',
        type=str,
        default='default',
        help="Name of the pool you want to increase the capacity. Default is 'default' pool.",
    )
    boost_parser.add_argument(
        '-b', '--boost',
        type=float,
        default=load_boost.DEFAULT_BOOST_FACTOR,
        help="Boost factor to apply. Default is 1.5. A big failover should be 2, 3 is the max.",
    )
    boost_parser.add_argument(
        '-d', '--duration',
        type=int,
        default=load_boost.DEFAULT_BOOST_DURATION,
        help="Duration of the capacity boost in minutes. Default is 40",
    )
    boost_parser.add_argument(
        '-f', '--force',
        action='store_true',
        dest='override',
        help="Replace an existing boost. Default is false",
    )
    boost_parser.add_argument(
        'action',
        choices=[
            'set',
            'status',
            'clear',
        ],
        help="You can view the status, set or clear a boost.",
    )
    boost_parser.set_defaults(command=paasta_boost)


def paasta_boost(args):
    soa_dir = args.soa_dir
    system_paasta_config = load_system_paasta_config()
    all_clusters = list_clusters(soa_dir=soa_dir)
    clusters = args.cluster.split(',')
    for cluster in clusters:
        if cluster not in all_clusters:
            paasta_print(
                f"Error: {cluster} doesn't look like a valid cluster. " +
                "Here is a list of valid paasta clusters:\n" + "\n".join(all_clusters),
            )
            return 1

    return_code, output = execute_paasta_cluster_boost_on_remote_master(
        clusters=clusters,
        system_paasta_config=system_paasta_config,
        action=args.action,
        pool=args.pool,
        duration=args.duration if args.action == 'set' else None,
        override=args.override if args.action == 'set' else None,
        boost=args.boost if args.action == 'set' else None,
        verbose=args.verbose,
    )
    paasta_print(output)
    return return_code
