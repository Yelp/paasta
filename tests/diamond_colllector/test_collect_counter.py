# Unit tests for soa_collector.collect_counter()
import pytest
import soa_collector
from soa_collector import collect_counter


@pytest.fixture
def node():
    return {
        "count": 0,
        "type": "counter"
    }


@pytest.fixture
def bucket():
    return []


@pytest.fixture
def metric_segments():
    return ['active-suspended-requests']


def test_happy_path(metric_segments, node, bucket):
    assert collect_counter(metric_segments, node, bucket)
    assert len(bucket) == 1
    actual_segments, actual_value, actual_type = bucket.pop()
    assert actual_segments == metric_segments
    assert actual_value == node['count']
    assert actual_type == soa_collector.METRIC_TYPE_COUNTER


def test_missing_count(metric_segments, node, bucket):
    del node['count']
    assert not collect_counter(metric_segments, node, bucket)
    assert len(bucket) == 0


def test_missing_type(metric_segments, node, bucket):
    del node['type']
    assert not collect_counter(metric_segments, node, bucket)
    assert len(bucket) == 0


def test_type_not_counter(metric_segments, node, bucket):
    node['type'] = 'foofoo'
    assert not collect_counter(metric_segments, node, bucket)
    assert len(bucket) == 0
