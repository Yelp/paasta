import pytest
from kubernetes.client import V2CrossVersionObjectReference
from kubernetes.client import V2ExternalMetricSource
from kubernetes.client import V2ExternalMetricStatus
from kubernetes.client import V2MetricIdentifier
from kubernetes.client import V2MetricSpec
from kubernetes.client import V2MetricStatus
from kubernetes.client import V2MetricTarget
from kubernetes.client import V2MetricValueStatus
from kubernetes.client import V2ObjectMetricSource
from kubernetes.client import V2ObjectMetricStatus
from kubernetes.client import V2PodsMetricSource
from kubernetes.client import V2PodsMetricStatus
from kubernetes.client import V2ResourceMetricSource
from kubernetes.client import V2ResourceMetricStatus

from paasta_tools.instance.hpa_metrics_parser import HPAMetricsParser


@pytest.fixture
def parser():
    return HPAMetricsParser(hpa=None)


def test_parse_target_external_metric_value(parser):
    metric_spec = V2MetricSpec(
        type="External",
        external=V2ExternalMetricSource(
            metric=V2MetricIdentifier(name="foo"),
            target=V2MetricTarget(
                type="Value",
                average_value=12,
            ),
        ),
    )
    status = parser.parse_target(metric_spec)
    assert status["name"] == "foo"
    assert status["target_value"] == "12"


def test_parse_target_external_metric_average_value(parser):
    # The parser handles this case, but it's not currently
    # used in kubernetes_tools
    metric_spec = V2MetricSpec(
        type="External",
        external=V2ExternalMetricSource(
            metric=V2MetricIdentifier(name="foo"),
            target=V2MetricTarget(
                type="AverageValue",
                average_value=0.5,
            ),
        ),
    )
    status = parser.parse_target(metric_spec)
    assert status["name"] == "foo"
    assert status["target_value"] == "0.5"


def test_parse_target_pod_metric(parser):
    metric_spec = V2MetricSpec(
        type="Pods",
        pods=V2PodsMetricSource(
            metric=V2MetricIdentifier(name="foo"),
            target=V2MetricTarget(
                type="AverageValue",
                average_value=0.5,
            ),
        ),
    )
    status = parser.parse_target(metric_spec)
    assert status["name"] == "foo"
    assert status["target_value"] == "0.5"


def test_parse_target_resource_metric(parser):
    metric_spec = V2MetricSpec(
        type="Resource",
        resource=V2ResourceMetricSource(
            name="cpu",
            target=V2MetricTarget(
                type="Utilization",
                average_utilization=0.5,
            ),
        ),
    )
    status = parser.parse_target(metric_spec)
    assert status["name"] == "cpu"
    assert status["target_value"] == "0.5"


def test_parse_target_object_metric(parser):
    metric_spec = V2MetricSpec(
        type="Object",
        object=V2ObjectMetricSource(
            metric=V2MetricIdentifier(name="some-metric"),
            described_object=V2CrossVersionObjectReference(
                api_version="apps/v1", kind="Deployment", name="deployment"
            ),
            target=V2MetricTarget(
                type="Value",
                value=1,
            ),
        ),
    )
    status = parser.parse_target(metric_spec)
    assert status["name"] == "some-metric"
    assert status["target_value"] == "1"


def test_parse_current_external_metric_value(parser):
    metric_status = V2MetricStatus(
        type="External",
        external=V2ExternalMetricStatus(
            current=V2MetricValueStatus(
                value=4,
            ),
            metric=V2MetricIdentifier(name="foo"),
        ),
    )
    status = parser.parse_current(metric_status)
    assert status["name"] == "foo"
    assert status["current_value"] == "4"


def test_parse_current_external_metric_average_value(parser):
    # The parser handles this case, but it's not currently
    # used in kubernetes_tools
    metric_status = V2MetricStatus(
        type="External",
        external=V2ExternalMetricStatus(
            current=V2MetricValueStatus(
                average_value=0.4,
            ),
            metric=V2MetricIdentifier(name="foo"),
        ),
    )
    status = parser.parse_current(metric_status)
    assert status["name"] == "foo"
    assert status["current_value"] == "0.4"


def test_parse_current_pod_metric(parser):
    metric_status = V2MetricStatus(
        type="Pods",
        pods=V2PodsMetricStatus(
            current=V2MetricValueStatus(
                average_value=0.4,
            ),
            metric=V2MetricIdentifier(name="foo"),
        ),
    )
    status = parser.parse_current(metric_status)
    assert status["name"] == "foo"
    assert status["current_value"] == "0.4"


def test_parse_current_resource_metric(parser):
    metric_status = V2MetricStatus(
        type="Resource",
        resource=V2ResourceMetricStatus(
            current=V2MetricValueStatus(
                average_utilization=0.4,
            ),
            name="cpu",
        ),
    )
    status = parser.parse_current(metric_status)
    assert status["name"] == "cpu"
    assert status["current_value"] == "0.4"


def test_parse_current_object_metric(parser):
    metric_status = V2MetricStatus(
        type="Object",
        object=V2ObjectMetricStatus(
            current=V2MetricValueStatus(value=0.1),
            metric=V2MetricIdentifier(name="some-metric"),
            described_object=V2CrossVersionObjectReference(
                api_version="apps/v1", kind="Deployment", name="deployment"
            ),
        ),
    )

    status = parser.parse_current(metric_status)
    assert status["name"] == "some-metric"
    assert status["current_value"] == "0.1"


def test_parse_current_empty_string_metric(parser):
    metric_status = V2MetricStatus(
        type="",
    )

    status = parser.parse_current(metric_status)
    assert status is None
