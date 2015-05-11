# Unit tests for soa_collector.collect_meter()
import pytest

from paasta_tools.diamond_collector.soa_collector import collect_meter


@pytest.fixture
def node():
    return {
        "m15": 0,
        "m5": 0,
        "m1": 0,
        "mean": 0,
        "count": 0,
        "unit": "seconds",
        "event_type": "requests",
        "type": "meter"
    }


@pytest.fixture
def bucket():
    return []


@pytest.fixture
def metric_segments():
    return ['suspends']


def test_happy_path(metric_segments, node, bucket):
    assert collect_meter(metric_segments, node, bucket)
    assert len(bucket) == len(node) - len(['unit', 'event_type', 'type'])
    while bucket:
        actual_segments, actual_value, actual_type = bucket.pop()
        key = actual_segments[-1]
        assert node[key] == actual_value


def test_missing_unit(metric_segments, node, bucket):
    del node['unit']
    assert not collect_meter(metric_segments, node, bucket)
    assert len(bucket) == 0


def test_missing_event_type(metric_segments, node, bucket):
    del node['event_type']
    assert not collect_meter(metric_segments, node, bucket)
    assert len(bucket) == 0


def test_missing_type(metric_segments, node, bucket):
    del node['type']
    assert not collect_meter(metric_segments, node, bucket)
    assert len(bucket) == 0


def test_unit_not_recognized(metric_segments, node, bucket):
    node['unit'] = 'foofoo'
    assert not collect_meter(metric_segments, node, bucket)
    assert len(bucket) == 0


def test_type_not_recognized(metric_segments, node, bucket):
    node['type'] = 'foofoo'
    assert not collect_meter(metric_segments, node, bucket)
    assert len(bucket) == 0
