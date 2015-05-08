# Unit tests for soa_collector.collect_gauge()
import pytest
import soa_collector
from soa_collector import collect_gauge


@pytest.fixture
def node():
    return {
        "value": 0.985,
        "type": "gauge"
    }


@pytest.fixture
def bucket():
    return []


@pytest.fixture
def metric_segments():
    return ['percent-idle']


def test_happy_path(metric_segments, node, bucket):
    assert collect_gauge(metric_segments, node, bucket)
    assert len(bucket) == 1
    actual_segments, actual_value, actual_type = bucket.pop()
    assert actual_segments == metric_segments
    assert actual_value == node['value']
    assert actual_type == soa_collector.METRIC_TYPE_GAUGE


def test_missing_value(metric_segments, node, bucket):
    del node['value']
    assert not collect_gauge(metric_segments, node, bucket)
    assert len(bucket) == 0


def test_missing_type(metric_segments, node, bucket):
    del node['type']
    assert not collect_gauge(metric_segments, node, bucket)
    assert len(bucket) == 0


def test_type_not_gauge(metric_segments, node, bucket):
    node['type'] = 'foofoo'
    assert not collect_gauge(metric_segments, node, bucket)
    assert len(bucket) == 0
