# Unit tests for soa_collector.collect_rate()
import pytest
from soa_collector import collect_rate


@pytest.fixture
def node():
    return {
        "m15": 0,
        "m5": 0,
        "m1": 0,
        "mean": 0,
        "count": 0,
        "unit": "seconds"
    }


@pytest.fixture
def bucket():
    return []


@pytest.fixture
def metric_segments():
    return ['rate']


def test_happy_path(metric_segments, node, bucket):
    assert collect_rate(metric_segments, node, bucket)
    assert len(bucket) == len(node) - 1  # remove 1 for skipped 'unit' entry
    while bucket:
        actual_segments, actual_value, actual_type = bucket.pop()
        assert actual_segments[0]  == metric_segments[0]
        assert len(actual_segments) == 2


def test_missing_unit(metric_segments, node, bucket):
    del node['unit']
    assert not collect_rate(metric_segments, node, bucket)
    assert len(bucket) == 0


def test_unit_not_recognized(metric_segments, node, bucket):
    node['unit'] = 'foofoo'
    assert not collect_rate(metric_segments, node, bucket)
    assert len(bucket) == 0
