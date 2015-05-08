# Unit tests for soa_collector.drop_jvm_node()
import pytest
import soa_collector
from soa_collector import drop_jvm_node


@pytest.fixture
def node():
    return {
        "version": "1.7.0_15-b03",
        "name": "Java HotSpot(TM) 64-Bit Server VM"
    }


@pytest.fixture
def bucket():
    return []


@pytest.fixture
def metric_segments():
    return ['vm']


def test_happy_path(metric_segments, node, bucket):
    assert drop_jvm_node(metric_segments, node, bucket)
    assert len(bucket) == 0


def test_vm_not_parent_segment(metric_segments, node, bucket):
    metric_segments = ['foo']
    assert not drop_jvm_node(metric_segments, node, bucket)
    assert len(bucket) == 0


def test_missing_version(metric_segments, node, bucket):
    del node['version']
    assert not drop_jvm_node(metric_segments, node, bucket)
    assert len(bucket) == 0


def test_missing_name(metric_segments, node, bucket):
    del node['name']
    assert not drop_jvm_node(metric_segments, node, bucket)
    assert len(bucket) == 0
