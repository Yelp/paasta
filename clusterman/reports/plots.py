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
import math
from collections import namedtuple

import arrow
import numpy as np
from matplotlib.dates import DateFormatter
from matplotlib.gridspec import GridSpec
from matplotlib.ticker import FuncFormatter

from clusterman.reports.constants import COLORMAP
from clusterman.reports.constants import ERROR_COLOR
from clusterman.reports.constants import MAGNITUDE_STRINGS
from clusterman.reports.constants import TREND_LINE_COLOR
from clusterman.reports.constants import TREND_RANGE_ALPHA
from clusterman.reports.constants import TREND_RANGE_COLOR


GRID_COLUMNS = 2
GRID_COLUMN_RATIOS = (3, 1)
GRID_LAYOUT_RECT = (0, 0, 1, 0.82)
REPORT_TITLE_SIZE = 14
PLOT_TITLE_SIZE = 12
AXIS_TITLE_SIZE = 8
TICK_SIZE = 8
PADDING = 5


PlotStruct = namedtuple('PlotStruct', ['data', 'error_data', 'range_', 'labels', 'ytick_formatter'])
# ytick_formatter defaults to the identity function
PlotStruct.__new__.__defaults__ = (lambda x: x,)  # type: ignore


def generate_heatmap_trend_grid(fig, heatmap, trend, months, tz):
    grid = GridSpec(len(months), GRID_COLUMNS, width_ratios=GRID_COLUMN_RATIOS)
    for i, (mstart, mend) in enumerate(months):
        # Want to show the graphs in reverse chronological order
        rev_index = len(months) - (i + 1)

        # Plot the heatmap
        heatmap_ax = fig.add_subplot(grid[2 * rev_index])
        _plot_heatmap(
            heatmap_ax,
            *heatmap.data[mstart],
            *heatmap.error_data[mstart],
            mstart,
            mend,
            tz=tz,
            show_ylabel=(len(months) > 1),  # print a y-axis label only when the report covers multiple months
            **dict(zip(('vmin', 'vmax'), heatmap.range_)),
        )
        heatmap_ax.xaxis.set_visible(False)
        heatmap_ax.set_title(heatmap.labels[mstart], fontsize=REPORT_TITLE_SIZE)

        # Plot the trend line
        trend_ax = fig.add_subplot(grid[2 * rev_index + 1])
        _plot_trend(
            trend_ax,
            *trend.data[mstart],
            xlim=[mstart.day, mend.day],
            ylim=trend.range_,
            ylabel=trend.labels,
            ytick_formatter=trend.ytick_formatter,
        )
    else:
        # We only show the x-axis on the bottom-most heatmap
        heatmap_ax.xaxis.set_visible(True)

    fig.tight_layout(rect=GRID_LAYOUT_RECT)


def _plot_heatmap(ax, x, y, z, ex, ey, ez, mstart, mend, tz, show_ylabel, **kwargs):
    ax.scatter(  # plot the "valid" points
        x, y,
        c=z,
        alpha=0.5,
        linewidths=0,
        s=3,
        cmap=COLORMAP,
        **kwargs,
    )
    ax.scatter(  # plot the "invalid" (i.e., above/below the threshold) points
        ex, ey,
        c=ERROR_COLOR,
        alpha=0.5,
        linewidths=0,
        s=3,
        **kwargs,
    )
    # Global plot settings
    ax.patch.set_facecolor('black')

    # x-axis settings
    ax.set_xlim(arrow.get(0).replace(tzinfo=tz).datetime, arrow.get(0).replace(tzinfo=tz).ceil('day').datetime)
    ax.xaxis.set_major_formatter(DateFormatter('%H:%M', tz=tz))
    ax.xaxis.set_tick_params(labelsize=TICK_SIZE)

    # y-axis settings
    ax.set_ylim(mstart.shift(days=-1).datetime, mend.datetime)
    ax.yaxis.set_major_formatter(DateFormatter('%m-%d', tz=tz))
    ax.yaxis.set_tick_params(direction='out', labelsize=TICK_SIZE)
    ax.yaxis.set_ticks([r.datetime for r in arrow.Arrow.range('week', mstart, mend)])
    if show_ylabel:
        ax.set_ylabel(mstart.format('MMMM'), fontsize=PLOT_TITLE_SIZE, labelpad=PADDING)


def _plot_trend(ax, x, q1, y, q3, xlim, ylim, ylabel, ytick_formatter):
    # compute the best-fit (linear) trend line
    fit = np.polyfit(x, y, 1)
    fit_fn = np.poly1d(fit)

    # plot the data, the trendlines, and the interquartile range (if given)
    ax.plot(x, y, color=TREND_LINE_COLOR)
    ax.plot(x, fit_fn(x), '--k', dashes=(1, 1), linewidth=0.75)
    if all(q1) and all(q3):
        ax.fill_between(x, q1, q3, facecolor=TREND_RANGE_COLOR, alpha=TREND_RANGE_ALPHA, linewidths=0)

    # x-axis settings
    ax.set_xlim(*xlim)
    ax.spines['top'].set_visible(False)
    ax.xaxis.set_ticks_position('bottom')
    ax.xaxis.set_tick_params(labelsize=TICK_SIZE)
    ax.set_xlabel('Day of month', fontsize=AXIS_TITLE_SIZE)

    # y-axis settings
    ax.set_ylim(*ylim)
    ax.spines['right'].set_visible(False)
    ax.yaxis.set_ticks_position('left')
    ax.yaxis.set_tick_params(labelsize=TICK_SIZE)

    trend_yaxis_major_formatter, yaxis_magnitude = _trend_yaxis_major_formatter(ylim[-1], ytick_formatter)
    if yaxis_magnitude:
        ylabel += f'\n({yaxis_magnitude})'
    ax.set_ylabel(ylabel, fontsize=AXIS_TITLE_SIZE)
    ax.yaxis.set_major_formatter(FuncFormatter(trend_yaxis_major_formatter))


def _trend_yaxis_major_formatter(max_ylim, ytick_formatter):
    ylim_magnitude = int(math.log10(max_ylim) // 3) if max_ylim > 1 else 0

    def format_large_trend_numbers(x, pos):
        if ylim_magnitude == 0:
            return ytick_formatter(x)
        threshold = 1000**ylim_magnitude
        q, r = divmod(x, threshold)
        return ytick_formatter(int(q) if q != 0 or r == 0 else r / threshold)

    return format_large_trend_numbers, MAGNITUDE_STRINGS[ylim_magnitude]
