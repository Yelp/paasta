import pytest
from kubernetes.client import V2beta2CrossVersionObjectReference
from kubernetes.client import V2beta2ExternalMetricSource
from kubernetes.client import V2beta2ExternalMetricStatus
from kubernetes.client import V2beta2MetricIdentifier
from kubernetes.client import V2beta2MetricSpec
from kubernetes.client import V2beta2MetricStatus
from kubernetes.client import V2beta2MetricTarget
from kubernetes.client import V2beta2MetricValueStatus
from kubernetes.client import V2beta2ObjectMetricSource
from kubernetes.client import V2beta2PodsMetricSource
from kubernetes.client import V2beta2PodsMetricStatus
from kubernetes.client import V2beta2ResourceMetricSource
from kubernetes.client import V2beta2ResourceMetricStatus
from kubernetes.client.models.v2beta2_object_metric_status import (
    V2beta2ObjectMetricStatus,
)

from paasta_tools.instance.hpa_metrics_parser import HPAMetricsParser


@pytest.fixture
def parser():
    return HPAMetricsParser(hpa=None)


def test_parse_target_external_metric_value(parser):
    metric_spec = V2beta2MetricSpec(
        type="External",
        external=V2beta2ExternalMetricSource(
            metric=V2beta2MetricIdentifier(name="foo"),
            target=V2beta2MetricTarget(type="Value", average_value=12,),
        ),
    )
    status = parser.parse_target(metric_spec)
    assert status["name"] == "foo"
    assert status["target_value"] == "12"


def test_parse_target_external_metric_average_value(parser):
    # The parser handles this case, but it's not currently
    # used in kubernetes_tools
    metric_spec = V2beta2MetricSpec(
        type="External",
        external=V2beta2ExternalMetricSource(
            metric=V2beta2MetricIdentifier(name="foo"),
            target=V2beta2MetricTarget(type="AverageValue", average_value=0.5,),
        ),
    )
    status = parser.parse_target(metric_spec)
    assert status["name"] == "foo"
    assert status["target_value"] == "0.5"


def test_parse_target_pod_metric(parser):
    metric_spec = V2beta2MetricSpec(
        type="Pods",
        pods=V2beta2PodsMetricSource(
            metric=V2beta2MetricIdentifier(name="foo"),
            target=V2beta2MetricTarget(type="AverageValue", average_value=0.5,),
        ),
    )
    status = parser.parse_target(metric_spec)
    assert status["name"] == "foo"
    assert status["target_value"] == "0.5"


def test_parse_target_resource_metric(parser):
    metric_spec = V2beta2MetricSpec(
        type="Resource",
        resource=V2beta2ResourceMetricSource(
            name="cpu",
            target=V2beta2MetricTarget(type="Utilization", average_utilization=0.5,),
        ),
    )
    status = parser.parse_target(metric_spec)
    assert status["name"] == "cpu"
    assert status["target_value"] == "0.5"


def test_parse_target_object_metric(parser):
    metric_spec = V2beta2MetricSpec(
        type="Object",
        object=V2beta2ObjectMetricSource(
            metric=V2beta2MetricIdentifier(name="some-metric"),
            described_object=V2beta2CrossVersionObjectReference(
                api_version="apps/v1", kind="Deployment", name="deployment"
            ),
            target=V2beta2MetricTarget(type="Value", value=1,),
        ),
    )
    status = parser.parse_target(metric_spec)
    assert status["name"] == "some-metric"
    assert status["target_value"] == "1"


def test_parse_current_external_metric_value(parser):
    metric_status = V2beta2MetricStatus(
        type="External",
        external=V2beta2ExternalMetricStatus(
            current=V2beta2MetricValueStatus(value=4,),
            metric=V2beta2MetricIdentifier(name="foo"),
        ),
    )
    status = parser.parse_current(metric_status)
    assert status["name"] == "foo"
    assert status["current_value"] == "4"


def test_parse_current_external_metric_average_value(parser):
    # The parser handles this case, but it's not currently
    # used in kubernetes_tools
    metric_status = V2beta2MetricStatus(
        type="External",
        external=V2beta2ExternalMetricStatus(
            current=V2beta2MetricValueStatus(average_value=0.4,),
            metric=V2beta2MetricIdentifier(name="foo"),
        ),
    )
    status = parser.parse_current(metric_status)
    assert status["name"] == "foo"
    assert status["current_value"] == "0.4"


def test_parse_current_pod_metric(parser):
    metric_status = V2beta2MetricStatus(
        type="Pods",
        pods=V2beta2PodsMetricStatus(
            current=V2beta2MetricValueStatus(average_value=0.4,),
            metric=V2beta2MetricIdentifier(name="foo"),
        ),
    )
    status = parser.parse_current(metric_status)
    assert status["name"] == "foo"
    assert status["current_value"] == "0.4"


def test_parse_current_resource_metric(parser):
    metric_status = V2beta2MetricStatus(
        type="Resource",
        resource=V2beta2ResourceMetricStatus(
            current=V2beta2MetricValueStatus(average_utilization=0.4,), name="cpu",
        ),
    )
    status = parser.parse_current(metric_status)
    assert status["name"] == "cpu"
    assert status["current_value"] == "0.4"


def test_parse_current_object_metric(parser):
    metric_status = V2beta2MetricStatus(
        type="Object",
        object=V2beta2ObjectMetricStatus(
            current=V2beta2MetricValueStatus(value=0.1),
            metric=V2beta2MetricIdentifier(name="some-metric"),
            described_object=V2beta2CrossVersionObjectReference(
                api_version="apps/v1", kind="Deployment", name="deployment"
            ),
        ),
    )

    status = parser.parse_current(metric_status)
    assert status["name"] == "some-metric"
    assert status["current_value"] == "0.1"


def test_parse_current_empty_string_metric(parser):
    metric_status = V2beta2MetricStatus(type="",)

    status = parser.parse_current(metric_status)
    assert status is None
