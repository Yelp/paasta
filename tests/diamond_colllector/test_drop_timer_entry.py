# Unit tests for soa_collector.drop_timer_entry()
import pytest
from soa_collector import drop_timer_entry


@pytest.fixture
def node():
    return {
        "rate": {
            "m15": 0,
            "m5": 0,
            "m1": 0,
            "mean": 0,
            "count": 0,
            "unit": "seconds"
        },
        "duration": {
            "p999": 0,
            "p99": 0,
            "p98": 0,
            "unit": "milliseconds",
            "min": 0,
            "max": 0,
            "mean": 0,
            "std_dev": 0,
            "median": 0,
            "p75": 0,
            "p95": 0
        },
        "type": "timer"
    }


@pytest.fixture
def bucket():
    return []


@pytest.fixture
def metric_segments():
    return ['org.eclipse.jetty.servlet.ServletContextHandler', 'trace-requests']


def test_happy_path(metric_segments, node, bucket):
    assert not drop_timer_entry(metric_segments, node, bucket)
    assert len(bucket) == 0
    assert 'type' not in node


def test_missing_type(metric_segments, node, bucket):
    del node['type']
    assert not drop_timer_entry(metric_segments, node, bucket)
    assert len(bucket) == 0


def test_type_not_recognized(metric_segments, node, bucket):
    node['type'] = 'foofoo'
    assert not drop_timer_entry(metric_segments, node, bucket)
    assert len(bucket) == 0
    assert 'type' in node


def test_peers_not_dicts(metric_segments, node, bucket):
    node['rate'] = ['foo', 'bar']
    assert not drop_timer_entry(metric_segments, node, bucket)
    assert len(bucket) == 0
    assert 'type' in node