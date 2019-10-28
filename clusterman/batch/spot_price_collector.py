import socket
import time
from traceback import format_exc

import arrow
import colorlog
import staticconf
from clusterman_metrics import ClustermanMetricsBotoClient
from clusterman_metrics import METADATA
from yelp_batch.batch import batch_command_line_arguments
from yelp_batch.batch import batch_configure
from yelp_batch.batch_daemon import BatchDaemon

from clusterman.args import add_disable_sensu_arg
from clusterman.args import add_env_config_path_arg
from clusterman.aws.spot_prices import spot_price_generator
from clusterman.aws.spot_prices import write_prices_with_dedupe
from clusterman.batch.util import BatchLoggingMixin
from clusterman.batch.util import BatchRunningSentinelMixin
from clusterman.batch.util import suppress_request_limit_exceeded
from clusterman.config import setup_config
from clusterman.util import sensu_checkin
from clusterman.util import setup_logging
from clusterman.util import splay_event_time

logger = colorlog.getLogger(__name__)


class SpotPriceCollector(BatchDaemon, BatchLoggingMixin, BatchRunningSentinelMixin):
    notify_emails = ['compute-infra@yelp.com']

    @batch_command_line_arguments
    def parse_args(self, parser):
        arg_group = parser.add_argument_group('SpotPriceCollector options')
        parser.add_argument(
            '--aws-region',
            required=True,
            choices=['us-west-1', 'us-west-2', 'us-east-1'],
            help='AWS region to collect spot pricing data for',
        )
        add_env_config_path_arg(arg_group)
        add_disable_sensu_arg(arg_group)
        arg_group.add_argument(
            '--start-time',
            default=arrow.utcnow(),
            help=(
                'Start of period to collect prices for. Default is now. '
                'Suggested format: 2017-01-13T21:10:34-08:00 (if no timezone, will be parsed as UTC).'
            ),
            type=lambda d: arrow.get(d).to('utc'),
        )

    @batch_configure
    def configure_initial(self):
        # Any keys in the env_config will override defaults in config.yaml.
        setup_config(self.options)

        self.logger = logger
        self.region = staticconf.read_string('aws.region')
        self.last_time_called = self.options.start_time
        self.run_interval = staticconf.read_int('batches.spot_prices.run_interval_seconds')
        self.dedupe_interval = staticconf.read_int('batches.spot_prices.dedupe_interval_seconds')
        self.metrics_client = ClustermanMetricsBotoClient(region_name=self.region)

    def write_prices(self, end_time, writer):
        prices = spot_price_generator(self.last_time_called, end_time)
        write_prices_with_dedupe(prices, writer, self.dedupe_interval)
        self.last_time_called = end_time

    def run(self):
        while self.running:
            time.sleep(splay_event_time(
                self.run_interval,
                self.get_name() + staticconf.read_string('aws.region'),
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

            # Report successful run to Sensu.
            sensu_args = dict(
                check_name='check_clusterman_spot_prices_running',
                output='OK: clusterman spot_prices was successful',
                check_every='1m',
                source=self.options.aws_region,
                ttl='10m',
                noop=self.options.disable_sensu,
            )
            sensu_checkin(**sensu_args)


if __name__ == '__main__':
    setup_logging()
    SpotPriceCollector().start()
