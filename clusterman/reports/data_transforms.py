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
import arrow
import numpy as np


def transform_heatmap_data(data, error_threshold_fn, months, tz):
    """ Transform input data into positions and values for heatmap plotting

    :param data: a SortedDict mapping from timestamp -> value
    :param error_threshold_fn: a function that takes an (x, y) pair and returns True if y is outside the threshold at x
    :param months: a list of (mstart, mend) tuples for grouping the output data
    :param tz: what timezone the output data should be interpreted as
    :returns: a dict of month -> [<x-data>, <y-data>, <values>] lists, as well as the p5/p95 values
        (the p5/p95 values are used to set the min/max color range of the heatmap)
    """
    data_by_month = {}
    error_data_by_month = {}
    min_val, max_val = float('inf'), float('-inf')
    for mstart, mend in months:
        mstart_index = data.bisect_left(mstart)
        mend_index = data.bisect_right(mend)

        # We want the y-axis to just be a date (year-month-day) and the x-axis to just be a time (hour-minute-second)
        # However, the matplotlib DatetimeFormatter won't take just date or time objects; so to get a canonical
        # datetime object we use the beginning UNIX epoch time and then replace the date/time with the correct values

        mdates, mtimes, mvals = [], [], []
        edates, etimes, evals = [], [], []
        for i in range(mstart_index, mend_index):
            utc_k, v = data.keys()[i], data.values()[i]
            k = utc_k.to(tz)
            date = k.replace(year=1970, month=1, day=1).datetime
            time = k.replace(hour=0, minute=0, second=0, microsecond=0).datetime
            if error_threshold_fn(utc_k, v):
                edates.append(date), etimes.append(time), evals.append(v)
            else:
                mdates.append(date), mtimes.append(time), mvals.append(v)
        p5, p95 = np.percentile([data.values()[i] for i in range(mstart_index, mend_index)], [5, 95])
        min_val = min(min_val, p5)
        max_val = max(max_val, p95)
        data_by_month[mstart] = (mdates, mtimes, mvals)
        error_data_by_month[mstart] = (edates, etimes, evals)
    return data_by_month, error_data_by_month, min_val, max_val


def transform_trend_data(data, months, trend_rollup):
    """ Transform input data into (x,y) values aggregated over each day of the month

    :param data: a SortedDict mapping from timestamp -> value
    :param months: a list of (mstart, mend) tuples for grouping the output data
    :param trend_rollup: a function accepting a list of data points to aggregate data per-day;
        this function should return a tuple q1, q2, q3, where the points defined by the q2's describe the
        solid line of the trend plot, and points in the range [q1, q3] are filled in on the trend plot.
        (many trend_rollups will want to return the mean and interquartile range, hence the variable names; however,
        this interpretation is not required)
    :returns: a dict of month -> [<day-of-month>, <lower-range>, <aggregated-value>, <upper-range>] lists,
        as well as the min/max aggregated values for the trend rollup
        (the min/max values sets the range for the trend plot)
    """
    data_by_month = {}
    min_val, max_val = 0, 0  # min_val sets the minimum y-value of the plot axis, which should always be 0 or less
    for mstart, mend in months:
        aggregated_daily_data = []

        # For each day in the month aggregate the data according to the chosen method
        for dstart, dend in arrow.Arrow.span_range('day', mstart, mend):
            dstart_index = data.bisect_left(dstart)
            dend_index = data.bisect_right(dend)
            day_slice = data.values()[dstart_index:dend_index]
            if not day_slice:  # if there's no data for a given day, np.percentile will fail
                continue

            q1, q2, q3 = trend_rollup(day_slice)
            min_val = min(v for v in (min_val, q1, q2, q3) if v is not None)
            max_val = max(v for v in (max_val, q2, q2, q3) if v is not None)
            aggregated_daily_data.append((dstart.day, q1, q2, q3))

        data_by_month[mstart] = zip(*aggregated_daily_data)
    return data_by_month, min_val, max_val
