import mock
import pytest

from paasta_tools.autoscaling.utils import MetricsProviderDict
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.long_running_service_tools import METRICS_PROVIDER_ACTIVE_REQUESTS
from paasta_tools.long_running_service_tools import METRICS_PROVIDER_CPU
from paasta_tools.long_running_service_tools import METRICS_PROVIDER_GUNICORN
from paasta_tools.long_running_service_tools import METRICS_PROVIDER_UWSGI
from paasta_tools.long_running_service_tools import METRICS_PROVIDER_UWSGI_V2
from paasta_tools.setup_prometheus_adapter_config import _minify_promql
from paasta_tools.setup_prometheus_adapter_config import (
    create_instance_active_requests_scaling_rule,
)
from paasta_tools.setup_prometheus_adapter_config import (
    create_instance_arbitrary_promql_scaling_rule,
)
from paasta_tools.setup_prometheus_adapter_config import (
    create_instance_gunicorn_scaling_rule,
)
from paasta_tools.setup_prometheus_adapter_config import (
    create_instance_uwsgi_scaling_rule,
)
from paasta_tools.setup_prometheus_adapter_config import (
    create_instance_uwsgi_v2_scaling_rule,
)
from paasta_tools.setup_prometheus_adapter_config import get_rules_for_service_instance
from paasta_tools.utils import SystemPaastaConfig

MOCK_SYSTEM_PAASTA_CONFIG = SystemPaastaConfig(
    {},
    "/mock/system/configs",
)


@pytest.mark.parametrize(
    "registrations,expected_instance",
    [
        (
            ["test_service.abc", "test_service.xyz", "test_service.123"],
            "test_instance",
        ),
        (
            ["test_service.xyz"],
            "xyz",
        ),
    ],
)
def test_create_instance_active_requests_scaling_rule(
    registrations: list, expected_instance: str
) -> None:
    service_name = "test_service"
    instance_config = mock.Mock(
        instance="test_instance",
        get_registrations=mock.Mock(return_value=registrations),
    )
    metrics_provider_config = MetricsProviderDict(
        {
            "type": METRICS_PROVIDER_ACTIVE_REQUESTS,
            "desired_active_requests_per_replica": 12,
            "moving_average_window_seconds": 20120302,
        }
    )
    paasta_cluster = "test_cluster"
    rule = create_instance_active_requests_scaling_rule(
        service=service_name,
        instance_config=instance_config,
        metrics_provider_config=metrics_provider_config,
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
        str(metrics_provider_config["desired_active_requests_per_replica"])
        in rule["metricsQuery"]
    )
    assert (
        str(metrics_provider_config["moving_average_window_seconds"])
        in rule["metricsQuery"]
    )
    assert f"paasta_instance='{expected_instance}'" in rule["metricsQuery"]


def test_create_instance_uwsgi_scaling_rule() -> None:
    service_name = "test_service"
    instance_config = mock.Mock(instance="test_instance")
    metrics_provider_config = MetricsProviderDict(
        {
            "type": METRICS_PROVIDER_UWSGI,
            "setpoint": 0.1234567890,
            "moving_average_window_seconds": 20120302,
        }
    )
    paasta_cluster = "test_cluster"
    rule = create_instance_uwsgi_scaling_rule(
        service=service_name,
        instance_config=instance_config,
        metrics_provider_config=metrics_provider_config,
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
    assert str(metrics_provider_config["setpoint"]) in rule["metricsQuery"]
    assert (
        str(metrics_provider_config["moving_average_window_seconds"])
        in rule["metricsQuery"]
    )


def test_create_instance_uwsgi_v2_scaling_rule() -> None:
    service_name = "test_service"
    instance_config = mock.Mock(instance="test_instance")
    metrics_provider_config = MetricsProviderDict(
        {
            "type": METRICS_PROVIDER_UWSGI_V2,
            "setpoint": 0.1234567890,
            "moving_average_window_seconds": 20120302,
        }
    )
    paasta_cluster = "test_cluster"
    rule = create_instance_uwsgi_v2_scaling_rule(
        service=service_name,
        instance_config=instance_config,
        metrics_provider_config=metrics_provider_config,
        paasta_cluster=paasta_cluster,
    )

    # we test that the format of the dictionary is as expected with mypy
    # and we don't want to test the full contents of the retval since then
    # we're basically just writing a change-detector test - instead, we test
    # that we're actually using our inputs
    assert service_name in rule["seriesQuery"]
    assert instance_config.instance in rule["seriesQuery"]
    assert paasta_cluster in rule["seriesQuery"]

    # Unlike uwsgi(v1), we don't use the setpoint in this query -- the HPA will have the setpoint as its target.
    assert str(metrics_provider_config["setpoint"]) not in rule["metricsQuery"]
    assert (
        str(metrics_provider_config["moving_average_window_seconds"])
        in rule["metricsQuery"]
    )


def test_create_instance_gunicorn_scaling_rule() -> None:
    service_name = "test_service"
    instance_config = mock.Mock(instance="test_instance")
    metrics_provider_config = MetricsProviderDict(
        {
            "type": METRICS_PROVIDER_GUNICORN,
            "setpoint": 0.1234567890,
            "moving_average_window_seconds": 20120302,
        }
    )
    paasta_cluster = "test_cluster"
    rule = create_instance_gunicorn_scaling_rule(
        service=service_name,
        instance_config=instance_config,
        metrics_provider_config=metrics_provider_config,
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
    assert str(metrics_provider_config["setpoint"]) in rule["metricsQuery"]
    assert (
        str(metrics_provider_config["moving_average_window_seconds"])
        in rule["metricsQuery"]
    )


@pytest.mark.parametrize(
    "instance_config,expected_rules",
    [
        (
            mock.Mock(
                instance="instance",
                get_namespace=mock.Mock(return_value="test_namespace"),
                get_autoscaling_metrics_provider=mock.Mock(
                    side_effect=lambda x: (
                        {
                            "type": METRICS_PROVIDER_CPU,
                            "setpoint": 0.1234567890,
                            "moving_average_window_seconds": 20120302,
                        }
                        if x == METRICS_PROVIDER_CPU
                        else None
                    )
                ),
            ),
            0,
        ),
        (
            mock.Mock(
                instance="instance",
                get_namespace=mock.Mock(return_value="test_namespace"),
                get_autoscaling_metrics_provider=mock.Mock(
                    side_effect=lambda x: (
                        {
                            "type": METRICS_PROVIDER_UWSGI,
                            "setpoint": 0.1234567890,
                            "moving_average_window_seconds": 20120302,
                        }
                        if x == METRICS_PROVIDER_UWSGI
                        else None
                    )
                ),
            ),
            1,
        ),
        (
            mock.Mock(
                instance="instance",
                get_namespace=mock.Mock(return_value="test_namespace"),
                get_autoscaling_metrics_provider=mock.Mock(
                    side_effect=lambda x: (
                        {
                            "type": METRICS_PROVIDER_UWSGI,
                            "setpoint": 0.1234567890,
                            "moving_average_window_seconds": 20120302,
                        }
                        if x == METRICS_PROVIDER_UWSGI
                        else (
                            {
                                "type": METRICS_PROVIDER_CPU,
                                "setpoint": 0.1234567890,
                                "moving_average_window_seconds": 20120302,
                            }
                            if x == METRICS_PROVIDER_CPU
                            else None
                        )
                    )
                ),
            ),
            1,
        ),
        (
            mock.Mock(
                instance="instance",
                get_namespace=mock.Mock(return_value="test_namespace"),
                get_autoscaling_metrics_provider=mock.Mock(
                    side_effect=lambda x: (
                        {
                            "type": METRICS_PROVIDER_UWSGI,
                            "setpoint": 0.1234567890,
                            "moving_average_window_seconds": 20120302,
                        }
                        if x == METRICS_PROVIDER_UWSGI
                        else (
                            {
                                "type": METRICS_PROVIDER_GUNICORN,
                                "setpoint": 0.1234567890,
                                "moving_average_window_seconds": 20120302,
                            }
                            if x == METRICS_PROVIDER_GUNICORN
                            else None
                        )
                    )
                ),
            ),
            2,
        ),
    ],
)
def test_get_rules_for_service_instance(
    instance_config: KubernetesDeploymentConfig,
    expected_rules: int,
) -> None:
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
        ),
        metrics_provider_config={"prometheus_adapter_config": {"metricsQuery": "foo"}},
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
        ),
        metrics_provider_config={
            "prometheus_adapter_config": {
                "metricsQuery": "foo",
                "seriesQuery": "bar",
            }
        },
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
