from __future__ import absolute_import
from __future__ import unicode_literals

from paasta_tools.autoscaling import forecasting


def test_moving_average_forecast_policy():
    historical_load = [
        (1, 100),
        (2, 120),
        (3, 140),
        (4, 160),
        (5, 180),
        (6, 200),
        (7, 220),
    ]

    assert 170 == forecasting.moving_average_forecast_policy(historical_load,
                                                             moving_average_window_seconds=5)
    assert 220 == forecasting.moving_average_forecast_policy(historical_load,
                                                             moving_average_window_seconds=0.5)
