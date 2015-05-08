# Unit tests for soa_collector.collect_histogram()
import pytest
from soa_collector import collect_histogram


@pytest.fixture
def node():
    return {
        "type": "histogram",
        "count": 1,
        "min": 2,
        "max": 2,
        "mean": 2,
        "std_dev": 0,
        "median": 2,
        "p75": 2,
        "p95": 2,
        "p98": 2,
        "p99": 2,
        "p999": 2
    }


@pytest.fixture
def bucket():
    return []


@pytest.fixture
def metric_segments():
    return ['prefix-length']


def test_happy_path(metric_segments, node, bucket):
    assert collect_histogram(metric_segments, node, bucket)
    assert len(bucket) == len(node) - len(['type'])
    while bucket:
        actual_segments, actual_value, actual_type = bucket.pop()
        assert actual_segments[0]  == metric_segments[0]
        assert len(actual_segments) == 2


def test_missing_type(metric_segments, node, bucket):
    del node['type']
    assert not collect_histogram(metric_segments, node, bucket)
    assert len(bucket) == 0


def test_type_not_recognized(metric_segments, node, bucket):
    node['type'] = 'foofoo'
    assert not collect_histogram(metric_segments, node, bucket)
    assert len(bucket) == 0
