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
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import list_clusters


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        "list-clusters",
        help="Display a list of all PaaSTA clusters",
        description=(
            "'paasta list' inspects all of the PaaSTA services declared in the soa-configs "
            "directory, and prints the set of unique clusters that are used.\n\n"
            "The command can only report those clusters that are actually used by some services."
        ),
    )
    list_parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    list_parser.set_defaults(command=paasta_list_clusters)


def paasta_list_clusters(args, **kwargs):
    for cluster in list_clusters(soa_dir=args.soa_dir):
        print(cluster)
