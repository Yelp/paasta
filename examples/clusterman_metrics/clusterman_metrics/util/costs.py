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
import staticconf
from clusterman_metrics.util.constants import CONFIG_NAMESPACE


config_reader = staticconf.NamespaceReaders(CONFIG_NAMESPACE)


def estimate_cost_per_hour(
    cluster,
    pool,
    cpus=0,
    mem=0,
):
    cpu_cost = cpus * _get_resource_cost('cpus', cluster, pool)
    mem_cost = mem * _get_resource_cost('mem', cluster, pool)
    return max(cpu_cost, mem_cost)


def _get_resource_cost(resource, cluster, pool):
    default_cost = config_reader.read_float(
        'cost_per_hour.defaults.{}'.format(resource),
        default=0,
    )
    return config_reader.read_float(
        'cost_per_hour.{}.{}.{}'.format(cluster, pool, resource),
        default=default_cost,
    )


def should_warn(cost):
    threshold = config_reader.read_float(
        'cost_warning_threshold',
        default=100,
    )
    return cost > threshold
