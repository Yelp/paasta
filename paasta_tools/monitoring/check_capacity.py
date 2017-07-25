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
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse
import json
import sys
from collections import defaultdict

from paasta_tools.api.client import get_paasta_api_client
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print


def parse_capacity_check_options():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        'type', choices=['cpus', 'mem', 'disk'], type=str,
        help='The resource to check.',
    )
    parser.add_argument(
        '--warn', '-w', dest='warn', type=float, default=80,
        help='Level to emit a warning status.',
    )
    parser.add_argument(
        '--crit', '-c', dest='crit', type=float, default=90,
        help='Level to emit a critical status.',
    )
    parser.add_argument(
        '--overrides', dest='overrides', type=str,
        help='json file of per-attribute overrides.\n'
        'In the format {attribute_name: {value: {warn: {cpus: num, disk: num, mem: num}, '
        'crit: {cpus: num, disk: num, mem: num}}}}',
    )
    parser.add_argument(
        '--cluster', dest='cluster', type=str,
        help='Cluster to check. Defaults to looking for the current cluster.',
    )
    parser.add_argument(
        '--attributes', dest='attributes', type=str, default='pool',
        help='Comma separated list of attributes to check.\n'
        'By default, only checks attributes individually; see --cross-product-check',
    )
    parser.add_argument(
        '--cross-product-check', dest='cpc', action='store_true',
        help='When specified, --cross-product-check will check combinations of '
        'attributes instead of individual attributes. \n'
        'eg if attributes is \'pool,region\', this will cause the check to fail '
        'if any region-pool combination is below the threashold, or either\'s override',
    )
    options = parser.parse_args()

    return options


def calc_percent_usage(resource_item, value_to_check):
    values = resource_item[value_to_check]
    if values['total'] == 0:
        return 0
    return 100 * (values['used'] / values['total'])


def error_message(failures, level, cluster, value_to_check):
    result = "%s cluster %s %s usage:\n" % (level, cluster, value_to_check)
    results = []
    for f in failures:
        attrs = ", ".join(["%s: %s" % (e['attr'], e['value']) for e in f['attrs']])
        results.append(
            "    %s is at %s percent %s, maximum %s percent" % (
                attrs, f['current'], value_to_check,
                f['maximum'],
            ),
        )

    result += "\n".join(results)

    return result


def run_capacity_check():
    options = parse_capacity_check_options()
    system_paasta_config = load_system_paasta_config()
    cluster = options.cluster if options.cluster is not None else system_paasta_config.get_cluster()
    value_to_check = options.type

    client = get_paasta_api_client(cluster=options.cluster)
    if client is None:
        paasta_print('UNKNOWN Failed to load paasta api client')
        sys.exit(3)

    if options.overrides:
        with open(options.overrides, 'r') as f:
            overrides = json.loads(f.read())
    else:
        overrides = {}

    attributes = options.attributes.split(',')

    if options.cpc:
        resource_use = {'all': client.resources.resources(groupings=attributes).result()}
    else:
        resource_use = {a: client.resources.resources(groupings=[a]).result() for a in attributes}

    default_check = {
        'warn': {
            'cpus': options.warn,
            'mem': options.warn,
            'disk': options.warn,
        },
        'crit': {
            'cpus': options.crit,
            'mem': options.crit,
            'disk': options.crit,
        },
    }

    failures = defaultdict(list)
    for attribute, values in resource_use.items():
        for usage_value in values:
            attribute_value = usage_value['groupings'].get(attribute, 'unknown')
            check = overrides.get('attribute', {}).get(attribute_value, default_check)
            usage_percent = calc_percent_usage(usage_value, value_to_check)
            for c in ['crit', 'warn']:
                if usage_percent > check[c][value_to_check]:
                    failures[c].append({
                        'attrs': [{'attr': a, 'value': v} for a, v in usage_value['groupings'].items()],
                        'maximum': check[c][value_to_check], 'current': usage_percent,
                    })
                    break

    return_value = [0]
    if len(failures['crit']) > 0:
        result = error_message(failures['crit'], 'CRITICAL', cluster, value_to_check)
        paasta_print(result)
        return_value.append(2)
    if len(failures['warn']) > 0:
        result = error_message(failures['warn'], 'WARNING', cluster, value_to_check)
        paasta_print(result)
        return_value.append(1)

    if max(return_value) == 0:
        paasta_print("OK cluster %s is below critical capacity in %s" % (cluster, value_to_check))

    sys.exit(max(return_value))


if __name__ == "__main__":
    run_capacity_check()
