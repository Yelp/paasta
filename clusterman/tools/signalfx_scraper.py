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
from collections import defaultdict

from clusterman_metrics.util.constants import METRIC_TYPES

from clusterman.args import add_start_end_args
from clusterman.common.sfx import Aggregation
from clusterman.common.sfx import basic_sfx_query
from clusterman.simulator.io import write_object_to_compressed_json
from clusterman.util import ask_for_choice
from clusterman.util import parse_time_string


def _parse_extra_options(opt_array):
    """ Convert any options that can't be parsed by argparse into a kwargs dict; if an option is specified
    multiple times, it will appear as a list in the results

    :param opt_array: a list of "option=value" strings
    :returns: a dict mapping from options -> values
    """
    kwargs = {}
    for opt_string in opt_array:
        opt, val = [s.strip() for s in opt_string.split('=')]
        if opt in kwargs:
            if not isinstance(kwargs[opt], list):
                kwargs[opt] = [kwargs[opt]]
            kwargs[opt].append(val)
        else:
            kwargs[opt] = val
    return kwargs


def main(args):
    """Download metrics from SignalFX and save them in a format that the Clusterman simulator can use."""
    kwargs = _parse_extra_options(args.option)
    start_time = parse_time_string(args.start_time)
    end_time = parse_time_string(args.end_time)
    args.dest_metric_names = args.dest_metric_names or args.src_metric_names

    if len(args.src_metric_names) != len(args.dest_metric_names):
        raise ValueError(
            'Different number of source and destination metrics\n'
            f'src = {args.src_metric_names}, dest = {args.dest_metric_names}'
        )

    # Get clusterman metric type for each downloaded metric.
    metric_options = list(METRIC_TYPES)
    metric_types = {}
    values = {}
    for src in args.src_metric_names:
        metric_types[src] = ask_for_choice(f'What metric type is {src}?', metric_options)

    values = defaultdict(dict)
    api_token = args.api_token
    filters = [s.split(':') for s in args.filter]
    for src, dest in zip(args.src_metric_names, args.dest_metric_names):
        print(f'Querying SignalFX for {src}')
        metric_type = metric_types[src]
        values[metric_type][dest] = basic_sfx_query(
            api_token,
            src,
            start_time,
            end_time,
            filters=filters,
            aggregation=Aggregation('sum', by=['AZ', 'inst_type']),
            extrapolation='last_value',
            max_extrapolations=3,
            **kwargs,
        )

    write_object_to_compressed_json(dict(values), args.dest_file)


def get_parser():
    parser = argparse.ArgumentParser()
    required_named_args = parser.add_argument_group('required arguments')
    optional_named_args = parser.add_argument_group('optional arguments')

    required_named_args.add_argument(
        '--src-metric-names',
        nargs='+',
        metavar='metric-name',
        required=True,
        help='list of source SignalFX metric names to backfill from',
    )
    required_named_args.add_argument(
        '--dest-file',
        metavar='filename',
        required=True,
        help=(
            'Filename to append the data to, in compressed JSON format. If the destination metric name '
            'already exists, you have the option to overwrite it.'
        )
    )
    required_named_args.add_argument(
        '--api-token',
        required=True,
        help='SignalFX API token, from user profile',
    )
    add_start_end_args(
        required_named_args,
        'initial time for queried datapoints',
        'final time for queried datapoints',
    )

    optional_named_args.add_argument(
        '--filter',
        default=[],
        nargs='*',
        help='Filters for SignalFX metrics, as dimension:value strings.',
    )
    optional_named_args.add_argument(
        '--dest-metric-names',
        nargs='*',
        metavar='metric-name',
        default=None,
        help='list of destination metric names to backfill to (if None, same as --src-metric-names)',
    )
    optional_named_args.add_argument(
        '-o', '--option',
        default=[],
        nargs='*',
        help=('additional options to be passed into the SignalFX query, as "opt=value" strings'),
    )
    return parser


if __name__ == '__main__':
    args = get_parser().parse_args()
    main(args)
