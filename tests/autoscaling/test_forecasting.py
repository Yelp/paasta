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

    assert 170 == forecasting.moving_average_forecast_policy(
        historical_load, moving_average_window_seconds=5
    )
    assert 220 == forecasting.moving_average_forecast_policy(
        historical_load, moving_average_window_seconds=0.5
    )


def test_linreg_forecast_policy():
    historical_load = [
        (1, 100),
        (2, 120),
        (3, 140),
        (4, 160),
        (5, 180),
        (6, 200),
        (7, 220),
    ]

    assert 220 == forecasting.linreg_forecast_policy(
        historical_load, linreg_window_seconds=7, linreg_extrapolation_seconds=0
    )
    assert 1000 == forecasting.linreg_forecast_policy(
        historical_load, linreg_window_seconds=7, linreg_extrapolation_seconds=39
    )

    # We should handle the case where there's only 1 data point within the window.
    assert 220 == forecasting.linreg_forecast_policy(
        historical_load, linreg_window_seconds=0, linreg_extrapolation_seconds=0
    )
    assert 220 == forecasting.linreg_forecast_policy(
        historical_load, linreg_window_seconds=0, linreg_extrapolation_seconds=10
    )
    assert 1000 == forecasting.linreg_forecast_policy(
        historical_load,
        linreg_window_seconds=0,
        linreg_extrapolation_seconds=78,
        linreg_default_slope=10,
    )

    historical_load_2 = [
        (1, 100),
        (2, 100),
        (3, 100),
        (4, 100),
        (5, 100),
        (6, 100),
        (1, 100),
        (2, 200),
        (3, 300),
        (4, 400),
        (5, 500),
        (6, 600),
    ]

    assert 350 == forecasting.linreg_forecast_policy(
        historical_load_2, linreg_window_seconds=7, linreg_extrapolation_seconds=0
    )
