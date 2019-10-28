import bisect
import logging
import re
from collections import defaultdict
from contextlib import contextmanager
from numbers import Number
from typing import Mapping
from typing import Optional

from clusterman_metrics.boto_client import ClustermanMetricsBotoClient
from clusterman_metrics.boto_client import MetricsValuesDict  # noqa (just used for type-checking)
from clusterman_metrics.util.constants import METRIC_TYPES
from clusterman_metrics.util.meteorite import generate_key_with_dimensions


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def _validate_metrics_object(obj):
    if not obj:
        return

    if not set(obj).issubset(METRIC_TYPES):
        raise ValueError('Invalid metric type in {gen}; valid choices are {valid}'.format(
            gen=list(obj),
            valid=list(METRIC_TYPES),
        ))

    for metric in obj.values():
        for key in metric:
            timestamps = [t for t, __ in metric[key]]
            if len(set(timestamps)) != len(timestamps):
                raise ValueError('Duplicate timestamps detected in {key}'.format(key=metric[key]))
            if any([not isinstance(timestamp, Number) for timestamp, __ in metric[key]]):
                raise ValueError('Invalid timestamp values for {key}'.format(key=metric[key]))
            if any([not isinstance(value, Number) and not isinstance(value, dict) for __, value in metric[key]]):
                raise ValueError('Invalid metric values for {key}'.format(key=metric[key]))
            metric[key] = sorted(metric[key])


class ClustermanMetricsSimulationClient(ClustermanMetricsBotoClient):
    """
    Metrics client for simulations that substitutes some metrics with generated data.
    Read-only (does not write metrics).
    """

    def __init__(self, generated_metrics, *args, **kwargs):
        """
        :param generated_metrics: dict of generated timeseries to use.
            Metrics not found here will be read from regular data.
            Should be in this format:
            {
                metric_type: {
                    metric_key: [(timestamp, value), ...],
                    ...
                },
                ...
            }
        """
        super(ClustermanMetricsSimulationClient, self).__init__(
            *args,
            **kwargs
        )
        _validate_metrics_object(generated_metrics)
        self.generated_metrics = generated_metrics

    @contextmanager
    def get_writer(*args, **kwargs):
        def log_values(*args, **kwargs):
            while True:
                values = yield
                logger.warning('Client is read-only; not writing {values}'.format(values=values))

        coroutine = log_values()
        try:
            next(coroutine)
            yield coroutine
        finally:
            coroutine.close()

    def _get_new_metric_values(
        self,
        key_prefix: str,
        metric_query: str,
        metric_type: str,
        time_start: int,
        time_end: int,
        is_regex: bool = False,
        extra_dimensions: Optional[Mapping[str, str]] = None,
    ) -> MetricsValuesDict:
        metrics: MetricsValuesDict = defaultdict(list)
        if is_regex:
            generated_keys = {
                key_prefix + metric_key
                for metric_key in self.generated_metrics.get(metric_type, {})
                if re.search(metric_query, metric_key)
            }
        else:
            generated_keys = {key_prefix + metric_query}

        for metric_key in generated_keys:
            full_query_key = generate_key_with_dimensions(metric_key, extra_dimensions)
            try:
                full_timeseries = self.generated_metrics.get(metric_type, {})[full_query_key]
            except KeyError:
                continue

            start_index = bisect.bisect_left(full_timeseries, (time_start,))
            end_index = bisect.bisect_right(full_timeseries, (time_end,))
            # (time_end,) will always be less than (time_end, <val>), so if there is a value for
            # (time_end, <val>) in the list, we need to increase the end index by one to include it.
            # This assumes there is at most one value for each timestamp.
            if len(full_timeseries) > end_index and full_timeseries[end_index][0] == time_end:
                end_index += 1

            metrics[metric_key] = full_timeseries[start_index:end_index]

        if metrics:
            return metrics
        else:
            return super(ClustermanMetricsSimulationClient, self)._get_new_metric_values(
                key_prefix,
                metric_query,
                metric_type,
                time_start,
                time_end,
                is_regex,
                extra_dimensions,
            )
