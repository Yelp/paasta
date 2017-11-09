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
from paasta_tools.autoscaling import cluster_boost
from paasta_tools.cli.utils import execute_paasta_cluster_boost_on_remote_master
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import list_clusters
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print


def add_subparser(subparsers):
    boost_parser = subparsers.add_parser(
        'boost',
        help="Set, get, or clear a capacity boost for a given region in a PaaSTA cluster",
        description=(
            "'paasta boost' is used to temporary provision more capacity in a given cluster "
            "It operates by ssh'ing to a Mesos master of a remote cluster, and "
            "interracting with the boost in the local zookeeper cluster. If you set or clear"
            "a boost, the cluster autoscaler will be run immediately (instead of every 20 min)"
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
    ).completer = lazy_choices_completer(list_clusters)
    boost_parser.add_argument(
        '-d', '--soa-dir',
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    boost_parser.add_argument(
        '-r', '--region',
        type=str,
        required=True,
        help="name of the AWS region where the pool is. eg: us-east-1",
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
        default=cluster_boost.DEFAULT_BOOST_FACTOR,
        help="Boost factor to apply. Default is 1.5. A big failover should be 2, 3 is the max.",
    )
    boost_parser.add_argument(
        '--duration',
        type=int,
        default=cluster_boost.DEFAULT_BOOST_DURATION,
        help="Duration of the capacity boost in minutes. Default is 40min.",
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
            'get',
            'clear',
        ],
        help="You can set, get or clear a boost.",
    )
    boost_parser.set_defaults(command=paasta_boost)


def paasta_boost(args):
    soa_dir = args.soa_dir
    system_paasta_config = load_system_paasta_config()
    all_clusters = list_clusters(soa_dir=soa_dir)
    if args.cluster in all_clusters:
        return_code, output = execute_paasta_cluster_boost_on_remote_master(
            cluster=args.cluster,
            system_paasta_config=system_paasta_config,
            action=args.action,
            region=args.region,
            pool=args.pool,
            duration=args.duration,
            override=args.override,
            boost=args.boost,
            verbose=args.verbose,
        )
    else:
        paasta_print("Cluster %s doesn't look like a valid cluster?" % args.cluster)
        paasta_print("Try using tab completion to help complete the cluster name")
    return return_code
