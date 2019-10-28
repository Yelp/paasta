import random
import time
from collections import defaultdict

import arrow
import yaml
from clusterman_metrics import ClustermanMetricsBotoClient

from clusterman.args import subparser
from clusterman.simulator.io import write_object_to_compressed_json
from clusterman.util import parse_time_interval_seconds
from clusterman.util import parse_time_string


def get_values_function(values_conf):
    """ Returns a function to generate metric values based on configuration

    There are two modes of operation:
    1. Use functions from the python random library to generate data; the config
       should be a dict in the format

       {'distribution': <function_name>, 'params': <distribution_parameters>}

       where function_name is a function from random, and distribution_parameters
       is the kwargs for the distribution function.
    2. Return a constant
    """
    try:
        gen_func = getattr(random, values_conf['distribution'])
        return lambda: gen_func(**values_conf['params'])
    except (AttributeError, TypeError):
        return lambda: int(values_conf)


def get_frequency_function(frequency_conf):
    """ Returns a function to compute the next event time for a metric timeseries, based on configuration

    There are two modes of operation:
    1. Fixed frequency intervals; in this case, the config should be a single string that
       can be parsed by parsedatetime (e.g., 1m, 2h, 3 months, etc).
    2. Randomly generated using functions from the python random library; the config should
       be a dict in the format

           {'distribution': <function_name>, 'params': <distribution_parameters>}

       where function_name is a function from random, and distribution_parameters is the
       kwargs for the distribution function.
       To ensure timestamp won't be duplicated, we use 1 second shift as the minimal interval.
    """
    if isinstance(frequency_conf, str):
        f = parse_time_interval_seconds(frequency_conf)
        return lambda current_time: current_time.shift(seconds=f)
    else:
        gen_func = getattr(random, frequency_conf['distribution'])
        return lambda current_time: current_time.shift(seconds=int(gen_func(**frequency_conf['params'])) + 1)


def get_markets_and_values(dict_keys, values_func):
    """ Randomly choose N unique elements from dict_keys, where 1<= N <= len(dict_keys)
        For each selected elements, assign a value from value_func and return
    """
    return {k: values_func() for k in random.sample(dict_keys, random.randint(1, len(dict_keys)))}


def get_historical_data(metric_key, metric_type, config, start_time, end_time):
    """ Returns a list of tuples (timestamp, value) based on the historical data in database, in which value is
        calculated by a*x + b.
    """
    aws_region = config['values']['aws_region']
    boto_client = ClustermanMetricsBotoClient(aws_region)
    result_key, result_items = boto_client.get_metric_values(
        metric_key,
        metric_type,
        start_time.timestamp,
        end_time.timestamp,
    )
    metric = []
    a = config['values']['params']['a']
    b = config['values']['params']['b']
    for item in result_items:
        metric.append((arrow.get(item[0]), a * float(item[1]) + b))
    return metric


def get_random_data(config, start_time, end_time):
    next_time_func = get_frequency_function(config['frequency'])
    values_func = get_values_function(config['values'])

    current_time = start_time
    metric = []
    while current_time < end_time:
        if 'dict_keys' in config:
            metric.append((current_time, get_markets_and_values(config['dict_keys'], values_func)))
        else:
            metric.append((current_time, values_func()))
        current_time = next_time_func(current_time)
    return metric


def load_experimental_design(inputfile):
    """ Generate metric timeseries data from an experimental design .yaml file

    The format of this file should be:
    metric_type:
      metric_name:
        start_time: XXXX
        end_time: YYYY
        dict_keys: <AWS market specifiction> (optional)
        frequency: <frequency specification>
        values: <values specification>

    This will generate a set of metric values between XXXX and YYYY, with the interarrival
    time between events meeting the frequency specification and the metric values corresponding
    to the values specification. When frequency is specified as historical, this will use the
    data in database (aws_region should be provided as a value parameter) to generate timeseries
    data.

    :returns: a dictionary of metric_type -> (metric_name -> timeseries data)
    """
    with open(inputfile) as f:
        design = yaml.safe_load(f.read())

    metrics = {}
    for metric_type, metric_design in design.items():
        metrics[metric_type] = defaultdict(list)
        for metric_name, config in metric_design.items():
            start_time = parse_time_string(config['start_time'])
            end_time = parse_time_string(config['end_time'])

            if config['frequency'] == 'historical':
                metrics[metric_type][metric_name] = get_historical_data(
                    metric_name,
                    metric_type,
                    config,
                    start_time,
                    end_time
                )
            else:
                metrics[metric_type][metric_name] = get_random_data(config, start_time, end_time)

    return metrics


def main(args):
    if not args.seed:
        args.seed = int(time.time())

    print(f'Random seed: {args.seed}')
    random.seed(args.seed)

    metrics_data = load_experimental_design(args.input)
    write_object_to_compressed_json(metrics_data, args.output)


@subparser('generate-data', 'generate data for a simulation based on an experimental design', main)
def add_generate_data_parser(subparser, required_named_args, optional_named_args):  # pragma: no coveer
    required_named_args.add_argument(
        '-i', '--input',
        required=True,
        metavar='filename',
        help='experimental design .yaml file',
    )
    required_named_args.add_argument(
        '-o', '--output',
        default='metrics.json.gz',
        metavar='filename',
        help='output file for generated data',
    )
    optional_named_args.add_argument(
        '--seed',
        default=None,
        help='seed value for the random number generator',
    )
