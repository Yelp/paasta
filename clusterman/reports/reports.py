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
from datetime import timedelta
from datetime import tzinfo

import arrow
from matplotlib.cm import get_cmap
from matplotlib.figure import Figure
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

from clusterman.reports.constants import AXIS_DIMENSION_INCHES
from clusterman.reports.constants import COLORMAP
from clusterman.reports.constants import ERROR_COLOR
from clusterman.reports.constants import FIGURE_DPI
from clusterman.reports.constants import SUBTITLE_SPACING
from clusterman.reports.constants import TREND_LINE_COLOR
from clusterman.reports.constants import TREND_RANGE_ALPHA
from clusterman.reports.constants import TREND_RANGE_COLOR
from clusterman.reports.data_transforms import transform_heatmap_data
from clusterman.reports.data_transforms import transform_trend_data
from clusterman.reports.plots import generate_heatmap_trend_grid
from clusterman.reports.plots import PlotStruct
from clusterman.reports.report_types import REPORT_TYPES


def _get_error_threshold_function(error_threshold, simulator):
    """ Returns a boolean-valued function that indicates whether a particular (x,y) pair is
    outside a specified threshold

    :param error_threshold: can be None or a string; if this is a string, the first character MAY be '+' or '-', to
        indicate whether values above or below the threshold (respectively) are considered "bad".  If nothing is
        specified, this defaults to '+'.  The remainder of the string MUST either be a constant value convertible to a
        float, or the name of a piecewise constant function in the simulator.  In the former case, any value above/below
        the constant is considered "bad", and in the latter, any value above/below the value of the piecewise constant
        function at that point in time is considered "bad".

        Examples:
          * '100', '+100' => values greater than 100 exceed the threshold
          * '-0' => values below 0 exceed the threshold
          * 'cost_per_hour' => values above the simulator's cost_per_hour function exceed the threshold
          * '-cpus' => values below the simulator's cpu function exceed the threshold
    :param simulator: a Simulator object
    :returns: a boolean-valued threshold function
    """
    if not error_threshold:
        return lambda x, y: False

    reverse = False
    if error_threshold[0] in ('-', '+'):
        reverse = error_threshold[0] == '-'
        error_threshold = error_threshold[1:]
    try:
        constant = float(error_threshold)
        return lambda x, y: ((y > constant) if not reverse else (y < constant))
    except ValueError:
        piecewise = getattr(simulator, error_threshold)
        return lambda x, y: ((y > piecewise.call(x)) if not reverse else (y < piecewise.call(x)))


def _make_report_title(fig, report, sim_metadata, months):
    report_title = f'{report.title} '
    if len(months) > 1:
        report_title += 'from {start} to {end}'.format(
            start=months[0][0].format('MMMM YYYY'),
            end=months[-1][0].format('MMMM YYYY'),
        )
    else:
        report_title += 'for {month}'.format(month=months[0][0].format('MMMM YYYY'))
    title = fig.suptitle(report_title, fontsize=14)
    y_axis_points = AXIS_DIMENSION_INCHES[1] * FIGURE_DPI * len(months)
    subtitle_abs_y = y_axis_points * title.get_position()[1] - SUBTITLE_SPACING
    subtitle_rel_y = subtitle_abs_y / y_axis_points
    fig.text(
        0.5, subtitle_rel_y,
        f'{sim_metadata.name}\nCluster: {sim_metadata.cluster}; Pool: {sim_metadata.pool}',
        va='top', ha='center',
        fontsize=6,
    )


def _make_heatmap_legend_marker(color, label):
    return Line2D(
        [0, 1], [0, 0],  # These don't matter
        marker='o', markerfacecolor=color,
        color='white',  # The line is white so it doesn't appear in the legend box, just the marker
        label=label,
    )


def _make_legend(fig, heatmap_range, has_errors, legend_formatter):
    cmap = get_cmap(COLORMAP)
    low, high = cmap(0.0), cmap(1.0)
    low_marker = _make_heatmap_legend_marker(low, 'min (p5): ' + legend_formatter(heatmap_range[0]))
    high_marker = _make_heatmap_legend_marker(high, 'max (p95): ' + legend_formatter(heatmap_range[1]))
    handles = [low_marker, high_marker]
    if has_errors:
        error_marker = _make_heatmap_legend_marker(ERROR_COLOR, 'exceeds threshold')
        handles.append(error_marker)
    fig.legend(handles=handles, loc='upper left', fontsize=6)

    trend_line = Line2D([0, 1], [0, 0], color=TREND_LINE_COLOR, label='average')
    bestfit_line = Line2D([0, 1], [0, 0], color='black', dashes=(1, 1), linewidth=0.75, label='best fit line')
    trend_patch = Patch(color=TREND_RANGE_COLOR, alpha=TREND_RANGE_ALPHA, label='interquartile range', linewidth=0)
    fig.legend(handles=[trend_line, bestfit_line, trend_patch], loc='upper right', fontsize=6)


def _make_axis_titles(report, report_data, months):
    titles = {}
    for mstart, mend in months:
        mstart_ind = report_data.bisect(mstart)
        mend_ind = report_data.bisect(mend)
        titles[mstart] = report.plot_title_formatter(report_data.values()[mstart_ind:mend_ind])
    return titles


def make_report(name, simulator, start_time, end_time, output_prefix='', tz='US/Pacific'):
    """ Create a report for a clusterman simulation run

    The layout for this report is a set of rows (one for each month in the time range)
    with [heatmap, trend_line] charts in each row.  The overall cost for each month is printed
    for each row, and the overall cost is given in the title

    :param name: the name of the report to generate
    :param simulator: a Simulator object
    :param start_time: the earliest time for the report
    :param end_time: the latest time for the report
    :param tz: a timezone string or object to interpret the chart data in
    """
    begin = arrow.now()
    print(f'Generating {name} report...')
    report = REPORT_TYPES[name]
    report_data = simulator.get_data(name, start_time, end_time, timedelta(seconds=60))

    if not isinstance(tz, tzinfo):
        tz = arrow.parser.TzinfoParser.parse(tz)
    local_start = start_time.to(tz)
    local_end = end_time.to(tz)

    months = arrow.Arrow.span_range('month', local_start, local_end)
    fig = Figure(figsize=(AXIS_DIMENSION_INCHES[0], AXIS_DIMENSION_INCHES[1] * len(months)))
    _make_report_title(fig, report, simulator.metadata, months)

    error_threshold_fn = _get_error_threshold_function(report.error_threshold, simulator)

    heatmap_data, error_data, *heatmap_range = transform_heatmap_data(report_data, error_threshold_fn, months, tz)
    trend_data, *trend_range = transform_trend_data(report_data, months, report.trend_rollup)

    heatmap = PlotStruct(heatmap_data, error_data, heatmap_range, _make_axis_titles(report, report_data, months))
    trend = PlotStruct(trend_data, None, trend_range, report.trend_label, report.trend_axis_formatter)

    generate_heatmap_trend_grid(fig, heatmap, trend, months, tz)
    # This ugly bit of code is just checking every month of error data (things that exceeded their threshold);
    # if any month has a datapoint that exceeded its threshold, display the red dot in the legend.  Otherwise don't.
    has_errors = any([len(data_points[0]) > 0 for data_points in error_data.values()])
    _make_legend(fig, heatmap_range, has_errors, report.legend_formatter)

    if output_prefix:
        output_prefix += '_'
    output_file = f'{output_prefix}{name}.png'
    processing_time = (arrow.now() - begin).total_seconds()
    fig.savefig(output_file, dpi=FIGURE_DPI)
    print(f'Done!  Report saved as {output_file} ({processing_time}s)')
