import argparse
import operator
from collections import defaultdict

import arrow
import colorlog
import staticconf
from clusterman_metrics import ClustermanMetricsSimulationClient
from clusterman_metrics import METADATA
from clusterman_metrics import SYSTEM_METRICS

from clusterman.args import add_branch_or_tag_arg
from clusterman.args import add_cluster_arg
from clusterman.args import add_cluster_config_directory_arg
from clusterman.args import add_pool_arg
from clusterman.args import add_scheduler_arg
from clusterman.args import add_start_end_args
from clusterman.args import subparser
from clusterman.aws.markets import get_market_resources
from clusterman.aws.markets import InstanceMarket
from clusterman.reports.report_types import REPORT_TYPES
from clusterman.simulator.event import AutoscalingEvent
from clusterman.simulator.event import InstancePriceChangeEvent
from clusterman.simulator.event import ModifyClusterSizeEvent
from clusterman.simulator.io import read_object_from_compressed_json
from clusterman.simulator.io import write_object_to_compressed_json
from clusterman.simulator.simulator import Simulator
from clusterman.simulator.util import SimulationMetadata
from clusterman.util import get_cluster_dimensions
from clusterman.util import parse_time_string
from clusterman.util import splay_event_time

logger = colorlog.getLogger(__name__)
colorlog.getLogger('clusterman_metrics')

try:
    # this currently fails for our paasta docker image
    # but we don't actually need to generate reports on paasta
    # so I'm just catching the error for now
    from clusterman.reports.reports import make_report
except ImportError as e:
    logger.warning(f'ImportError: {e}, unable to import code to make reports, some simulator commands will fail')

    def make_report(name, simulator, start_time, end_time, output_prefix='', tz='US/Pacific'):
        logger.error('Unable to generate report due to missing imports')


def _load_metrics(metrics_data_files, pool):
    metrics = defaultdict(dict)
    for metrics_file in (metrics_data_files or []):
        try:
            data = read_object_from_compressed_json(metrics_file, raw_timestamps=True)
            for metric_type, values in data.items():
                metrics[metric_type].update(values)
        except OSError as e:
            logger.warn(f'{str(e)}: no metrics loaded')
    region_name = staticconf.read_string('aws.region')
    metrics_client = ClustermanMetricsSimulationClient(metrics, region_name=region_name, app_identifier=pool)
    return metrics_client


def _populate_autoscaling_events(simulator, start_time, end_time):
    current_time = start_time.shift(seconds=splay_event_time(
        simulator.autoscaler.run_frequency,
        'simulated-autoscaler',
        timestamp=start_time.timestamp,
    ))
    while current_time < end_time:
        simulator.add_event(AutoscalingEvent(current_time))
        current_time = current_time.shift(seconds=splay_event_time(
            simulator.autoscaler.run_frequency,
            'simulated-autoscaler',
            timestamp=current_time.timestamp,
        ))


def _populate_cluster_size_events(simulator, start_time, end_time):
    capacity_metrics = simulator.metrics_client.get_metric_values(
        f'fulfilled_capacity',
        METADATA,
        start_time.timestamp,
        end_time.timestamp,
        use_cache=False,
        extra_dimensions=get_cluster_dimensions(
            simulator.metadata.cluster,
            simulator.metadata.pool,
            simulator.metadata.scheduler,
        ),
    )
    for i, (timestamp, data) in enumerate(capacity_metrics['fulfilled_capacity']):
        market_data = {}
        for market_str, value in data.items():
            market = InstanceMarket.parse(market_str)
            weight = get_market_resources(market).cpus // staticconf.read_int('cpus_per_weight')
            market_data[market] = int(value) // weight
        simulator.markets |= set(market_data.keys())
        use_join_delay = (i != 0)  # Want to start the cluster out at the expected capacity
        simulator.add_event(ModifyClusterSizeEvent(arrow.get(timestamp), market_data, use_join_delay))


def _populate_allocated_resources(simulator, start_time, end_time):
    allocated_metrics = simulator.metrics_client.get_metric_values(
        f'cpus_allocated',
        SYSTEM_METRICS,
        start_time.timestamp,
        end_time.timestamp,
        use_cache=False,
        extra_dimensions=get_cluster_dimensions(
            simulator.metadata.cluster,
            simulator.metadata.pool,
            simulator.metadata.scheduler,
        ),
    )
    # It's OK to just directly set up the timeseries here, instead of using events; if the autoscaler
    # depends on these values it will re-read it from the metrics client anyways.
    #
    # In the future, we may want to make the simulator smarter (if the value of cpus_allocated exceeds the
    # simulated total cpus, for example), but for right now I don't care (CLUSTERMAN-145)
    for timestamp, data in allocated_metrics['cpus_allocated']:
        simulator.mesos_cpus_allocated.add_breakpoint(arrow.get(timestamp), float(data))


def _populate_price_changes(simulator, start_time, end_time, discount):
    for market in simulator.markets:
        market_prices = simulator.metrics_client.get_metric_values(
            f'spot_prices',
            METADATA,
            start_time.timestamp,
            end_time.timestamp,
            use_cache=False,
            extra_dimensions={
                'aws_availability_zone': market.az,
                'aws_instance_type': market.instance,
            }
        )
        for timestamp, price in market_prices['spot_prices']:
            price = float(price) * (discount or 1.0)
            simulator.add_event(InstancePriceChangeEvent(
                arrow.get(timestamp),
                {market: price},
            ))


def _run_simulation(args, metrics_client):
    metadata = SimulationMetadata(args.name, args.cluster, args.pool, args.scheduler)
    simulator = Simulator(metadata, args.start_time, args.end_time, args.autoscaler_config, metrics_client)
    if simulator.autoscaler:
        _populate_autoscaling_events(simulator, args.start_time, args.end_time)
    else:
        _populate_cluster_size_events(simulator, args.start_time, args.end_time)

    _populate_allocated_resources(simulator, args.start_time, args.end_time)
    _populate_price_changes(simulator, args.start_time, args.end_time, args.discount)

    simulator.run()
    return simulator


def main(args):
    args.start_time = parse_time_string(args.start_time)
    args.end_time = parse_time_string(args.end_time)

    staticconf.DictConfiguration({
        'join_delay_mean_seconds': args.join_delay_params[0],
        'join_delay_stdev_seconds': args.join_delay_params[1],
        'cpus_per_weight': args.cpus_per_weight,
        'ebs_volume_size': args.ebs_volume_size,
    })
    # We can provide up to two simulation objects to compare.  If we load two simulator objects to compare,
    # we don't need to run a simulation here.  If the user specifies --compare but only gives one object,
    # then we need to run a simulation now, and use that to compare to the saved sim
    sims = []
    if args.compare:
        if len(args.compare) > 2:
            raise argparse.ArgumentError(None, f'Cannot compare more than two simulations: {args.compare}')
        sims = [read_object_from_compressed_json(sim_file) for sim_file in args.compare]

    if len(sims) < 2:
        metrics_client = _load_metrics(args.metrics_data_files, args.pool)
        simulator = _run_simulation(args, metrics_client)
        sims.insert(0, simulator)

    if len(sims) == 2:
        cmp_fn = getattr(operator, args.comparison_operator)
        final_simulator = cmp_fn(*sims)
    else:
        final_simulator = sims[0]

    if args.simulation_result_file:
        write_object_to_compressed_json(final_simulator, args.simulation_result_file)

    if hasattr(args, 'reports'):
        if 'all' in args.reports:
            args.reports = REPORT_TYPES.keys()

        for report in args.reports:
            make_report(report, final_simulator, args.start_time, args.end_time, args.output_prefix)


@subparser('simulate', 'simulate the behavior of a cluster', main)
def add_simulate_parser(subparser, required_named_args, optional_named_args):  # pragma: no cover
    add_start_end_args(
        required_named_args,
        'simulation start time',
        'simulation end time',
    )
    add_cluster_arg(required_named_args, required=False)
    add_pool_arg(required_named_args)
    add_scheduler_arg(required_named_args)
    add_cluster_config_directory_arg(optional_named_args)
    add_branch_or_tag_arg(optional_named_args)
    required_named_args.add_argument(
        '--name',
        default='simulation',
        help='Name for the simulation (helpful when comparing two simulations)',
    )
    optional_named_args.add_argument(
        '--autoscaler-config',
        default=None,
        help='file containing the spot fleet request JSON data for the autoscaler',
    )
    optional_named_args.add_argument(
        '--reports',
        nargs='+',
        choices=list(REPORT_TYPES.keys()) + ['all'],
        default=[],
        help='type(s) of reports to generate from the simulation',
    )
    optional_named_args.add_argument(
        '--metrics-data-files',
        metavar='filename',
        nargs='+',
        help='provide simulated values for one or more metric time series',
    )
    optional_named_args.add_argument(
        '--cpus-per-weight',
        type=int,
        default=1,
        help='how many CPUs are present in one unit of weight',
    )
    optional_named_args.add_argument(
        '--ebs-volume-size',
        type=int,
        metavar='GB',
        default=0,
        help='size of EBS volume for EBS-only instances'
    )
    optional_named_args.add_argument(
        '--discount',
        metavar='percent',
        type=float,
        default=None,
        help='optional discount to apply to cost calculations',
    )
    optional_named_args.add_argument(
        '--join-delay-params',
        metavar=('mean', 'stdev (seconds)'),
        nargs=2,
        type=int,
        default=[0, 0],
        help='parameters to control long to wait before a host joins the cluster (normally distributed)'
    )
    optional_named_args.add_argument(
        '--output-prefix',
        default='',
        help='filename prefix for generated reports',
    )
    optional_named_args.add_argument(
        '--simulation-result-file',
        metavar='filename',
        help='specify filename to save simulation result for comparison',
    )
    optional_named_args.add_argument(
        '--compare',
        metavar='filename',
        nargs='+',
        help='specify one or two filenames to compare simulation result',
    )
    optional_named_args.add_argument(
        '--comparison-operator',
        choices=['add', 'sub', 'mul', 'truediv'],
        default='truediv',
        help='operation to use for comparing simulations; valid choices are binary functions from the operator module',
    )
