import mock
import pytest

from paasta_tools.long_running_service_tools import AutoscalingParamsDict
from paasta_tools.setup_prometheus_adapter_config import _minify_promql
from paasta_tools.setup_prometheus_adapter_config import (
    create_instance_active_requests_scaling_rule,
)
from paasta_tools.setup_prometheus_adapter_config import (
    create_instance_arbitrary_promql_scaling_rule,
)
from paasta_tools.setup_prometheus_adapter_config import (
    create_instance_cpu_scaling_rule,
)
from paasta_tools.setup_prometheus_adapter_config import (
    create_instance_gunicorn_scaling_rule,
)
from paasta_tools.setup_prometheus_adapter_config import (
    create_instance_uwsgi_scaling_rule,
)
from paasta_tools.setup_prometheus_adapter_config import get_rules_for_service_instance
from paasta_tools.setup_prometheus_adapter_config import (
    should_create_active_requests_scaling_rule,
)
from paasta_tools.setup_prometheus_adapter_config import should_create_cpu_scaling_rule
from paasta_tools.setup_prometheus_adapter_config import (
    should_create_gunicorn_scaling_rule,
)
from paasta_tools.setup_prometheus_adapter_config import (
    should_create_uwsgi_scaling_rule,
)
from paasta_tools.utils import InstanceConfig_T
from paasta_tools.utils import SystemPaastaConfig

MOCK_SYSTEM_PAASTA_CONFIG = SystemPaastaConfig(
    {},
    "/mock/system/configs",
)


@pytest.mark.parametrize(
    "autoscaling_config,expected",
    [
        (
            {
                "metrics_provider": "cpu",
                "decision_policy": "bespoke",
                "moving_average_window_seconds": 123,
                "setpoint": 0.653,
            },
            False,
        ),
        (
            {
                "metrics_provider": "active-requests",
                "moving_average_window_seconds": 124,
                "desired_active_requests_per_replica": 0.425,
            },
            True,
        ),
        (
            {
                "metrics_provider": "active-requests",
                "desired_active_requests_per_replica": 0.764,
            },
            True,
        ),
    ],
)
def test_should_create_active_requests_scaling_rule(
    autoscaling_config: AutoscalingParamsDict, expected: bool
) -> None:
    should_create, reason = should_create_active_requests_scaling_rule(
        autoscaling_config=autoscaling_config
    )

    assert should_create == expected
    if expected:
        assert reason is None
    else:
        assert reason is not None


def test_create_instance_active_requests_scaling_rule() -> None:
    service_name = "test_service"
    instance_config = mock.Mock(
        instance="test_instance",
        get_autoscaling_params=mock.Mock(
            return_value={
                "metrics_provider": "active-requests",
                "desired_active_requests_per_replica": 12,
                "moving_average_window_seconds": 20120302,
            }
        ),
    )
    paasta_cluster = "test_cluster"

    with mock.patch(
        "paasta_tools.setup_prometheus_adapter_config.load_system_paasta_config",
        autospec=True,
        return_value=MOCK_SYSTEM_PAASTA_CONFIG,
    ):
        rule = create_instance_active_requests_scaling_rule(
            service=service_name,
            instance_config=instance_config,
            paasta_cluster=paasta_cluster,
        )

    # we test that the format of the dictionary is as expected with mypy
    # and we don't want to test the full contents of the retval since then
    # we're basically just writting a change-detector test - instead, we test
    # that we're actually using our inputs
    assert service_name in rule["seriesQuery"]
    assert instance_config.instance in rule["seriesQuery"]
    assert paasta_cluster in rule["seriesQuery"]
    # these two numbers are distinctive and unlikely to be used as constants
    assert (
        str(
            instance_config.get_autoscaling_params()[
                "desired_active_requests_per_replica"
            ]
        )
        in rule["metricsQuery"]
    )
    assert (
        str(instance_config.get_autoscaling_params()["moving_average_window_seconds"])
        in rule["metricsQuery"]
    )


@pytest.mark.parametrize(
    "autoscaling_config,expected",
    [
        (
            {
                "metrics_provider": "cpu",
                "decision_policy": "bespoke",
                "moving_average_window_seconds": 123,
                "setpoint": 0.653,
            },
            False,
        ),
        (
            {
                "metrics_provider": "uwsgi",
                "moving_average_window_seconds": 124,
                "setpoint": 0.425,
            },
            True,
        ),
        (
            {
                "metrics_provider": "uwsgi",
                "use_prometheus": True,
                "moving_average_window_seconds": 544,
                "setpoint": 0.764,
            },
            True,
        ),
        (
            {
                "metrics_provider": "uwsgi",
                "use_prometheus": False,
                "moving_average_window_seconds": 544,
                "setpoint": 0.764,
            },
            False,
        ),
    ],
)
def test_should_create_uswgi_scaling_rule(
    autoscaling_config: AutoscalingParamsDict, expected: bool
) -> None:
    should_create, reason = should_create_uwsgi_scaling_rule(
        autoscaling_config=autoscaling_config
    )

    assert should_create == expected
    if expected:
        assert reason is None
    else:
        assert reason is not None


def test_create_instance_uwsgi_scaling_rule() -> None:
    service_name = "test_service"
    instance_config = mock.Mock(
        instance="test_instance",
        get_autoscaling_params=mock.Mock(
            return_value={
                "metrics_provider": "uwsgi",
                "setpoint": 0.1234567890,
                "moving_average_window_seconds": 20120302,
                "use_prometheus": True,
            }
        ),
    )
    paasta_cluster = "test_cluster"

    with mock.patch(
        "paasta_tools.setup_prometheus_adapter_config.load_system_paasta_config",
        autospec=True,
        return_value=MOCK_SYSTEM_PAASTA_CONFIG,
    ):
        rule = create_instance_uwsgi_scaling_rule(
            service=service_name,
            instance_config=instance_config,
            paasta_cluster=paasta_cluster,
        )

    # we test that the format of the dictionary is as expected with mypy
    # and we don't want to test the full contents of the retval since then
    # we're basically just writting a change-detector test - instead, we test
    # that we're actually using our inputs
    assert service_name in rule["seriesQuery"]
    assert instance_config.instance in rule["seriesQuery"]
    assert paasta_cluster in rule["seriesQuery"]
    # these two numbers are distinctive and unlikely to be used as constants
    assert (
        str(instance_config.get_autoscaling_params()["setpoint"])
        in rule["metricsQuery"]
    )
    assert (
        str(instance_config.get_autoscaling_params()["moving_average_window_seconds"])
        in rule["metricsQuery"]
    )


@pytest.mark.parametrize(
    "autoscaling_config,expected",
    [
        (
            {
                "metrics_provider": "cpu",
                "use_prometheus": True,
                "moving_average_window_seconds": 123,
                "setpoint": 0.653,
            },
            True,
        ),
        (
            {
                "metrics_provider": "cpu",
                "moving_average_window_seconds": 123,
                "setpoint": 0.653,
            },
            False,
        ),
        (
            {
                "metrics_provider": "uwsgi",
                "moving_average_window_seconds": 124,
                "setpoint": 0.425,
            },
            False,
        ),
        (
            {
                "metrics_provider": "uwsgi",
                "use_prometheus": True,
                "moving_average_window_seconds": 544,
                "setpoint": 0.764,
            },
            False,
        ),
    ],
)
def test_should_create_cpu_scaling_rule(
    autoscaling_config: AutoscalingParamsDict, expected: bool
) -> None:
    should_create, reason = should_create_cpu_scaling_rule(
        autoscaling_config=autoscaling_config
    )

    assert should_create == expected
    if expected:
        assert reason is None
    else:
        assert reason is not None


def test_create_instance_cpu_scaling_rule() -> None:
    service_name = "test_service"
    instance_config = mock.Mock(
        instance="test_instance",
        get_namespace=mock.Mock(return_value="test_namespace"),
        get_autoscaling_params=mock.Mock(
            return_value={
                "metrics_provider": "cpu",
                "setpoint": 0.1234567890,
                "moving_average_window_seconds": 20120302,
                "use_prometheus": True,
            }
        ),
    )
    paasta_cluster = "test_cluster"

    rule = create_instance_cpu_scaling_rule(
        service=service_name,
        instance_config=instance_config,
        paasta_cluster=paasta_cluster,
    )

    # our query doesn't include the setpoint as we'll just give the HPA the current CPU usage and
    # let the HPA compare that to the setpoint directly
    assert (
        str(instance_config.get_autoscaling_params()["moving_average_window_seconds"])
        in rule["metricsQuery"]
    )
    assert str(instance_config.get_namespace()) in rule["metricsQuery"]


@pytest.mark.parametrize(
    "autoscaling_config,expected",
    [
        (
            {
                "metrics_provider": "cpu",
                "decision_policy": "bespoke",
                "moving_average_window_seconds": 123,
                "setpoint": 0.653,
            },
            False,
        ),
        (
            {
                "metrics_provider": "gunicorn",
                "moving_average_window_seconds": 124,
                "setpoint": 0.425,
            },
            True,
        ),
    ],
)
def test_should_create_gunicorn_scaling_rule(
    autoscaling_config: AutoscalingParamsDict, expected: bool
) -> None:
    should_create, reason = should_create_gunicorn_scaling_rule(
        autoscaling_config=autoscaling_config
    )

    assert should_create == expected
    if expected:
        assert reason is None
    else:
        assert reason is not None


def test_create_instance_gunicorn_scaling_rule() -> None:
    service_name = "test_service"
    instance_config = mock.Mock(
        instance="test_instance",
        get_autoscaling_params=mock.Mock(
            return_value={
                "metrics_provider": "gunicorn",
                "setpoint": 0.1234567890,
                "moving_average_window_seconds": 20120302,
                "use_prometheus": True,
            }
        ),
    )
    paasta_cluster = "test_cluster"

    with mock.patch(
        "paasta_tools.setup_prometheus_adapter_config.load_system_paasta_config",
        autospec=True,
        return_value=MOCK_SYSTEM_PAASTA_CONFIG,
    ):
        rule = create_instance_gunicorn_scaling_rule(
            service=service_name,
            instance_config=instance_config,
            paasta_cluster=paasta_cluster,
        )

    # we test that the format of the dictionary is as expected with mypy
    # and we don't want to test the full contents of the retval since then
    # we're basically just writting a change-detector test - instead, we test
    # that we're actually using our inputs
    assert service_name in rule["seriesQuery"]
    assert instance_config.instance in rule["seriesQuery"]
    assert paasta_cluster in rule["seriesQuery"]
    # these two numbers are distinctive and unlikely to be used as constants
    assert (
        str(instance_config.get_autoscaling_params()["setpoint"])
        in rule["metricsQuery"]
    )
    assert (
        str(instance_config.get_autoscaling_params()["moving_average_window_seconds"])
        in rule["metricsQuery"]
    )


@pytest.mark.parametrize(
    "instance_config,expected_rules",
    [
        (
            mock.Mock(
                instance="instance",
                get_namespace=mock.Mock(return_value="test_namespace"),
                get_autoscaling_params=mock.Mock(
                    return_value={
                        "metrics_provider": "uwsgi",
                        "setpoint": 0.1234567890,
                        "moving_average_window_seconds": 20120302,
                        "use_prometheus": True,
                    }
                ),
            ),
            1,
        ),
        (
            mock.Mock(
                instance="instance",
                get_namespace=mock.Mock(return_value="test_namespace"),
                get_autoscaling_params=mock.Mock(
                    return_value={
                        "metrics_provider": "uwsgi",
                        "setpoint": 0.1234567890,
                        "moving_average_window_seconds": 20120302,
                        "use_prometheus": False,
                    }
                ),
            ),
            0,
        ),
        (
            mock.Mock(
                instance="instance",
                get_namespace=mock.Mock(return_value="test_namespace"),
                get_autoscaling_params=mock.Mock(
                    return_value={
                        "metrics_provider": "cpu",
                        "setpoint": 0.1234567890,
                        "moving_average_window_seconds": 20120302,
                        "use_prometheus": False,
                    }
                ),
            ),
            0,
        ),
        (
            mock.Mock(
                instance="instance",
                get_namespace=mock.Mock(return_value="test_namespace"),
                get_autoscaling_params=mock.Mock(
                    return_value={
                        "metrics_provider": "cpu",
                        "setpoint": 0.1234567890,
                        "moving_average_window_seconds": 20120302,
                        "use_prometheus": True,
                    }
                ),
            ),
            1,
        ),
    ],
)
def test_get_rules_for_service_instance(
    instance_config: InstanceConfig_T,
    expected_rules: int,
) -> None:
    with mock.patch(
        "paasta_tools.setup_prometheus_adapter_config.load_system_paasta_config",
        autospec=True,
        return_value=MOCK_SYSTEM_PAASTA_CONFIG,
    ):
        assert (
            len(
                get_rules_for_service_instance(
                    service_name="service",
                    instance_config=instance_config,
                    paasta_cluster="cluster",
                )
            )
            == expected_rules
        )


@pytest.mark.parametrize(
    "query,expected",
    [
        # empty strings shouldn't be touched (or happen...)
        ("", ""),
        # we should have no leading/trailing whitespace
        ("\t\t\ttest{label='a'}\n", "test{label='a'}"),
        # we should collapse internal whitespace
        ("\t \ttest{label='a'}\n   test{label='b'}", "test{label='a'} test{label='b'}"),
        # we shouldn't touch whitespace inside of labels
        ("\t \ttest{label='a  b'}\n", "test{label='a  b'}"),
    ],
)
def test__minify_promql(query: str, expected: str) -> None:
    assert _minify_promql(query) == expected


def test_create_instance_arbitrary_promql_scaling_rule_no_seriesQuery():
    rule = create_instance_arbitrary_promql_scaling_rule(
        service="service",
        instance_config=mock.Mock(
            instance="instance",
            get_namespace=mock.Mock(return_value="paasta"),
            get_autoscaling_params=mock.Mock(
                return_value={"prometheus_adapter_config": {"metricsQuery": "foo"}}
            ),
        ),
        paasta_cluster="cluster",
    )

    assert rule == {
        "name": {"as": "service-instance-arbitrary-promql"},
        "resources": {
            "overrides": {
                "namespace": {"resource": "namespace"},
                "deployment": {"group": "apps", "resource": "deployments"},
            },
        },
        "metricsQuery": "label_replace( label_replace( foo, 'deployment', 'service-instance', '', '' ), 'namespace', 'paasta', '', '' )",
        "seriesQuery": "kube_deployment_labels{ deployment='service-instance', paasta_cluster='cluster', namespace='paasta' }",
    }


def test_create_instance_arbitrary_promql_scaling_rule_with_seriesQuery():
    rule = create_instance_arbitrary_promql_scaling_rule(
        service="service",
        instance_config=mock.Mock(
            instance="instance",
            get_namespace=mock.Mock(return_value="test_namespace"),
            get_autoscaling_params=mock.Mock(
                return_value={
                    "prometheus_adapter_config": {
                        "metricsQuery": "foo",
                        "seriesQuery": "bar",
                    }
                }
            ),
        ),
        paasta_cluster="cluster",
    )

    assert rule == {
        "name": {"as": "service-instance-arbitrary-promql"},
        "resources": {
            "overrides": {
                "namespace": {"resource": "namespace"},
                "deployment": {"group": "apps", "resource": "deployments"},
            },
        },
        "metricsQuery": "foo",  # if seriesQuery is specified, the user's metricsQuery should be unaltered.
        "seriesQuery": "bar",
    }
