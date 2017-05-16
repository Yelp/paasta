from __future__ import absolute_import
from __future__ import unicode_literals

from paasta_tools.autoscaling.utils import get_autoscaling_component
from paasta_tools.autoscaling.utils import register_autoscaling_component


FORECAST_POLICY_KEY = 'forecast_policy'


def get_forecast_policy(name):
    """
    Returns a forecast policy matching the given name. Only used by decision policies that try to forecast load, like
    the proportional decision policy.
    """
    return get_autoscaling_component(name, FORECAST_POLICY_KEY)


@register_autoscaling_component('current', FORECAST_POLICY_KEY)
def current_value_forecast_policy(historical_load, **kwargs):
    """A prediction policy that assumes that the value any time in the future will be the same as the current value.

    :param historical_load: a list of (timestamp, value)s, where timestamp is a unix timestamp and value is load.
    """
    return historical_load[-1][1]


@register_autoscaling_component('moving_average', FORECAST_POLICY_KEY)
def moving_average_forecast_policy(historical_load, moving_average_window_seconds, **kwargs):
    """Does a simple average of all historical load data points within the moving average window. Weights all data
    points within the window equally.

    :param historical_load: a list of (timestamp, value)s, where timestamp is a unix timestamp and value is load.
    """

    window_end, _ = historical_load[-1]
    window_begin = window_end - moving_average_window_seconds

    count = 0
    total = 0
    for timestamp, value in historical_load:
        if timestamp >= window_begin and timestamp <= window_end:
            count += 1
            total += value

    return total / count
