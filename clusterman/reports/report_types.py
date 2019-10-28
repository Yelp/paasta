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
from collections import namedtuple

import numpy as np

ReportProperties = namedtuple('ReportProperties', [
    'title',
    'trend_rollup',
    'plot_title_formatter',
    'trend_axis_formatter',
    'legend_formatter',
    'trend_label',
    'error_threshold',
])


def DEFAULT_TREND_ROLLUP(data): return np.percentile(data, [25, 50, 75])


REPORT_TYPES = {
    'cpus': ReportProperties(
        title='vCPU capacity',
        trend_rollup=DEFAULT_TREND_ROLLUP,
        plot_title_formatter=lambda data: f'Average capacity: {int(np.mean(data))} vCPUs',
        trend_axis_formatter=int,
        legend_formatter=lambda val: f'{int(round(val))} vCPUs',
        trend_label='vCPUs/day',
        error_threshold='-0',
    ),
    'cpus_allocated': ReportProperties(
        title='vCPUs allocated',
        trend_rollup=DEFAULT_TREND_ROLLUP,
        plot_title_formatter=lambda data: f'Average allocated capacity: {int(np.mean(data))} vCPUs',
        trend_axis_formatter=int,
        legend_formatter=lambda val: f'{int(round(val))} vCPUs',
        trend_label='vCPUs/day',
        error_threshold='mesos_cpus',
    ),
    'unused_cpus': ReportProperties(
        title='Unused vCPUs',
        trend_rollup=DEFAULT_TREND_ROLLUP,
        plot_title_formatter=lambda data: f'Average unused capacity: {int(np.mean(data))} vCPUs',
        trend_axis_formatter=int,
        legend_formatter=lambda val: f'{int(round(val))} vCPUs',
        trend_label='vCPUs/day',
        error_threshold='-0',
    ),
    'cost': ReportProperties(
        title='Cost',
        trend_rollup=DEFAULT_TREND_ROLLUP,
        plot_title_formatter=lambda data: f'Total cost: ${sum(data):,.2f}',
        trend_axis_formatter=lambda val: f'${val:,.2f}',
        legend_formatter=lambda val: f'${val:,.2f}/minute',
        trend_label='cost/minute',
        error_threshold='-0',
    ),
    'unused_cpus_cost': ReportProperties(
        title='Cost of unused vCPUs',
        trend_rollup=DEFAULT_TREND_ROLLUP,
        plot_title_formatter=lambda data: f'Unused cost: ${sum(data):,.2f}',
        trend_axis_formatter=lambda val: f'${val:,.2f}',
        legend_formatter=lambda val: f'${val:,.2f}/minute',
        trend_label='Unused cost/minute',
        error_threshold='-0',
    ),
    'cost_per_cpu': ReportProperties(
        title='Cost per vCPU',
        trend_rollup=DEFAULT_TREND_ROLLUP,
        plot_title_formatter=lambda data: f'Average cost per vCPU: ${np.mean(data):,.4f}',
        trend_axis_formatter=lambda val: f'${val:,.4f}',
        legend_formatter=lambda val: f'${val:,.4f}/vCPU-minute',
        trend_label='cost/vCPU-minute',
        error_threshold='-0',
    ),
    'oversubscribed': ReportProperties(
        title='Oversubscribed vCPUs',
        trend_rollup=DEFAULT_TREND_ROLLUP,
        plot_title_formatter=lambda data: f'Average oversubscribed capacity: {int(np.mean(data))} vCPUs',
        trend_axis_formatter=int,
        legend_formatter=lambda val: f'{int(round(val))} vCPUs',
        trend_label='Oversubscribed vCPUs/day',
        error_threshold='-0',
    )
}
