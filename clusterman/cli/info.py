# Copyright 2019 Yelp Inc.
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
import simplejson as json

from clusterman.args import add_cluster_arg
from clusterman.args import add_json_arg
from clusterman.args import subparser
from clusterman.util import get_cluster_name_list
from clusterman.util import get_pool_name_list


def list_clusters(args):  # pragma: no cover
    if args.json:
        print(json.dumps(list(get_cluster_name_list())))
    else:
        print('\n'.join(get_cluster_name_list()))


@subparser('list-clusters', 'list available clusters', list_clusters)
def add_list_clusters_parser(subparser, required_named_args, optional_named_args):  # pragma: no cover
    add_json_arg(optional_named_args)


def list_pools(args):  # pragma: no cover
    if args.json:
        obj = {
            scheduler: list(get_pool_name_list(args.cluster, scheduler))
            for scheduler in ['mesos', 'kubernetes']
        }
        print(json.dumps(obj))
    else:
        for scheduler in ['mesos', 'kubernetes']:
            print(f'\n{scheduler.capitalize()} pools\n--------------------')
            print('\n'.join(get_pool_name_list(args.cluster, scheduler)))


@subparser('list-pools', 'list available pools in a cluster', list_pools)
def add_list_pools_parser(subparser, required_named_args, optional_named_args):  # pragma: no cover
    add_cluster_arg(required_named_args, required=True)
    add_json_arg(optional_named_args)
