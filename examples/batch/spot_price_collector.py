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

import arrow
import colorlog
import staticconf
from clusterman_metrics import ClustermanMetricsBotoClient
from clusterman_metrics import METADATA

from clusterman.args import add_env_config_path_arg
from clusterman.aws.spot_prices import spot_price_generator
from clusterman.aws.spot_prices import write_prices_with_dedupe
from clusterman.config import setup_config
from clusterman.util import setup_logging
from clusterman.util import splay_event_time
from examples.batch.util import BatchRunningSentinelMixin
from examples.batch.util import suppress_request_limit_exceeded

logger = colorlog.getLogger(__name__)


class SpotPriceCollector(BatchRunningSentinelMixin):
    def parse_args(self) -> None:
        parser = argparse.ArgumentParser()
        arg_group = parser.add_argument_group('SpotPriceCollector options')
        parser.add_argument(
            '--aws-region',
            required=True,
            choices=['us-west-1', 'us-west-2', 'us-east-1'],
            help='AWS region to collect spot pricing data for',
        )
        add_env_config_path_arg(arg_group)
        arg_group.add_argument(
            '--start-time',
            default=arrow.utcnow(),
            help=(
                'Start of period to collect prices for. Default is now. '
                'Suggested format: 2017-01-13T21:10:34-08:00 (if no timezone, will be parsed as UTC).'
            ),
            type=lambda d: arrow.get(d).to('utc'),
        )

        self.options = parser.parse_args()

    def configure(self) -> None:
        # Any keys in the env_config will override defaults in config.yaml.
        setup_config(self.options)

        self.logger = logger
        self.region = staticconf.read_string('aws.region')
        self.last_time_called = self.options.start_time
        self.run_interval = staticconf.read_int('batches.spot_prices.run_interval_seconds')
        self.dedupe_interval = staticconf.read_int('batches.spot_prices.dedupe_interval_seconds')
        self.metrics_client = ClustermanMetricsBotoClient(region_name=self.region)

    def write_prices(self, end_time, writer) -> None:
        prices = spot_price_generator(self.last_time_called, end_time)
        write_prices_with_dedupe(prices, writer, self.dedupe_interval)
        self.last_time_called = end_time

    def run(self) -> None:
        self.make_running_sentinel()
        while True:
            time.sleep(splay_event_time(
                self.run_interval,
                self.__class__.__name__ + staticconf.read_string('aws.region'),
            ))

            now = arrow.utcnow()
            with self.metrics_client.get_writer(METADATA) as writer:
                try:
                    with suppress_request_limit_exceeded():
                        self.write_prices(now, writer)
                except socket.timeout:
                    # We don't really care if we miss a few spot price changes so just continue here
                    logger.warn(f'Timed out getting spot prices:\n\n{format_exc()}')
                    continue


if __name__ == '__main__':
    setup_logging()
    batch = SpotPriceCollector()
    batch.parse_args()
    batch.configure()
    batch.run()
