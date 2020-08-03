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
import argparse
import socket
import time
from traceback import format_exc
from typing import Callable
from typing import cast
from typing import Generator
from typing import List
from typing import Mapping
from typing import MutableMapping
from typing import NamedTuple
from typing import Type
from typing import Union

import colorlog
import staticconf
from clusterman_metrics import ClustermanMetricsBotoClient
from clusterman_metrics import generate_key_with_dimensions
from clusterman_metrics import METADATA
from clusterman_metrics import SYSTEM_METRICS

from clusterman.args import add_cluster_arg
from clusterman.args import add_cluster_config_directory_arg
from clusterman.args import add_env_config_path_arg
from clusterman.autoscaler.pool_manager import PoolManager
from clusterman.config import load_cluster_pool_config
from clusterman.config import setup_config
from clusterman.mesos.metrics_generators import ClusterMetric
from clusterman.mesos.metrics_generators import generate_framework_metadata
from clusterman.mesos.metrics_generators import generate_simple_metadata
from clusterman.mesos.metrics_generators import generate_system_metrics
from clusterman.util import All
from clusterman.util import get_pool_name_list
from clusterman.util import setup_logging
from clusterman.util import splay_event_time
from examples.batch.util import BatchRunningSentinelMixin
from examples.batch.util import suppress_request_limit_exceeded

logger = colorlog.getLogger(__name__)


class MetricToWrite(NamedTuple):
    generator: Callable[[PoolManager], Generator[ClusterMetric, None, None]]
    type: str
    aggregate_meteorite_dims: bool
    pools: Union[Type[All], List['str']]


METRICS_TO_WRITE = [
    MetricToWrite(generate_system_metrics, SYSTEM_METRICS, aggregate_meteorite_dims=False, pools=All),
    MetricToWrite(generate_simple_metadata, METADATA, aggregate_meteorite_dims=False, pools=All),
    MetricToWrite(
        generate_framework_metadata,
        METADATA,
        aggregate_meteorite_dims=True,
        pools=['default'],
    ),
]


class ClusterMetricsCollector(BatchRunningSentinelMixin):
    def parse_args(self) -> None:
        parser = argparse.ArgumentParser()
        arg_group = parser.add_argument_group('ClusterMetricsCollector options')
        add_cluster_arg(arg_group, required=True)
        add_env_config_path_arg(arg_group)
        add_cluster_config_directory_arg(arg_group)
        self.options = parser.parse_args()

    def configure(self) -> None:
        setup_config(self.options)

        # Since we want to collect metrics for all the pools, we need to call setup_config
        # first to load the cluster config path, and then read all the entries in that directory
        self.pools: MutableMapping[str, List[str]] = {}
        for scheduler in {'mesos', 'kubernetes'}:
            self.pools[scheduler] = get_pool_name_list(self.options.cluster, scheduler)
        for scheduler, pools in self.pools.items():
            for pool in pools:
                load_cluster_pool_config(self.options.cluster, pool, scheduler, None)

        self.region = staticconf.read_string('aws.region')
        self.run_interval = staticconf.read_int('batches.cluster_metrics.run_interval_seconds')
        self.logger = logger

        self.metrics_client = ClustermanMetricsBotoClient(region_name=self.region)

    def load_pool_managers(self) -> None:
        logger.info('Reloading all PoolManagers')
        self.pool_managers: Mapping[str, PoolManager] = {}
        for scheduler, pools in self.pools.items():
            for pool in pools:
                try:
                    logger.info(f'Loading resource groups for {pool}.{scheduler} on {self.options.cluster}')
                    self.pool_managers[f'{pool}.{scheduler}'] = PoolManager(self.options.cluster, pool, scheduler)
                except Exception as e:
                    logger.exception(e)
                    continue

    @suppress_request_limit_exceeded()
    def run(self) -> None:
        self.load_pool_managers()  # Load the pools on the first run; do it here so we get logging
        self.make_running_sentinel()

        while True:
            time.sleep(splay_event_time(
                self.run_interval,
                self.__class__.__name__ + self.options.cluster,
            ))

            for pool, manager in self.pool_managers.items():
                logger.info(f'Reloading state for pool manager for pool {pool}')
                manager.reload_state()
                logger.info(f'Done reloading state for pool {pool}')

            self.write_all_metrics()

    def write_all_metrics(self) -> bool:
        successful = True

        for metric_to_write in METRICS_TO_WRITE:
            with self.metrics_client.get_writer(
                metric_to_write.type,
                metric_to_write.aggregate_meteorite_dims
            ) as writer:
                try:
                    self.write_metrics(writer, metric_to_write.generator, metric_to_write.pools)
                except socket.timeout:
                    # Try to get metrics for the rest of the clusters, but make sure we know this failed
                    logger.warn(f'Timed out getting cluster metric data:\n\n{format_exc()}')
                    successful = False
                    continue

        return successful

    def write_metrics(
        self,
        writer,
        metric_generator: Callable[[PoolManager], Generator[ClusterMetric, None, None]],
        pools: Union[Type[All], List[str]],
    ) -> None:
        for pool, manager in self.pool_managers.items():
            if pools != All and pool not in cast(List[str], pools):
                continue

            for cluster_metric in metric_generator(manager):
                metric_name = generate_key_with_dimensions(cluster_metric.metric_name, cluster_metric.dimensions)
                data = (metric_name, int(time.time()), cluster_metric.value)
                logger.info(f'Writing value {cluster_metric.value} for metric {metric_name} to metric store')

                writer.send(data)


if __name__ == '__main__':
    setup_logging()
    batch = ClusterMetricsCollector()
    batch.parse_args()
    batch.configure()
    batch.run()
