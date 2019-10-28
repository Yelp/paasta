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
import time

import colorlog

from clusterman.args import add_cluster_arg
from clusterman.args import add_cluster_config_directory_arg
from clusterman.args import add_env_config_path_arg
from clusterman.args import add_pool_arg
from clusterman.args import add_scheduler_arg
from clusterman.autoscaler.autoscaler import Autoscaler
from clusterman.autoscaler.pool_manager import PoolManager
from clusterman.config import setup_config
from clusterman.util import setup_logging
from clusterman.util import splay_event_time
from examples.batch.util import BatchRunningSentinelMixin
from examples.batch.util import suppress_request_limit_exceeded

logger = colorlog.getLogger(__name__)
colorlog.getLogger('clusterman_metrics')  # This just adds a handler to the clusterman_metrics logger


class AutoscalerBatch(BatchRunningSentinelMixin):
    def parse_args(self):
        parser = argparse.ArgumentParser()
        arg_group = parser.add_argument_group('AutoscalerBatch options')
        add_cluster_arg(arg_group, required=True)
        add_pool_arg(arg_group)
        add_scheduler_arg(arg_group)
        add_cluster_config_directory_arg(arg_group)
        add_env_config_path_arg(arg_group)
        arg_group.add_argument(
            '--dry-run',
            default=False,
            action='store_true',
            help='If true, will only log autoscaling decisions instead of modifying capacities',
        )
        self.options = parser.parse_args()

    def configure(self) -> None:
        setup_config(self.options)
        self.autoscaler = None
        self.logger = logger

        self.apps = [self.options.pool]  # TODO (CLUSTERMAN-126) someday these should not be the same thing

        pool_manager = PoolManager(
            self.options.cluster,
            self.options.pool,
            self.options.scheduler,
        )
        self.autoscaler = Autoscaler(
            self.options.cluster,
            self.options.pool,
            self.options.scheduler,
            self.apps,
            monitoring_enabled=(not self.options.dry_run),
            pool_manager=pool_manager,
        )

    def _autoscale(self) -> None:
        assert self.autoscaler
        time.sleep(splay_event_time(
            self.autoscaler.run_frequency,
            self.__class__.__name__ + self.options.cluster + self.options.pool,
        ))
        with suppress_request_limit_exceeded():
            self.autoscaler.run(dry_run=self.options.dry_run)

    def run(self) -> None:
        self.make_running_sentinel()
        while True:
            self._autoscale()


if __name__ == '__main__':
    setup_logging()
    batch = AutoscalerBatch()
    batch.parse_args()
    batch.configure()
    batch.run()
