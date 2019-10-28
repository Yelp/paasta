import mock
import pytest

from clusterman.aws.markets import InstanceMarket
from clusterman.simulator.simulated_aws_cluster import SimulatedAWSCluster


@pytest.fixture
def cluster(simulator):
    cluster = SimulatedAWSCluster(simulator)
    cluster.simulator.current_time.shift(seconds=+42)
    cluster.modify_size({
        InstanceMarket('m4.4xlarge', 'us-west-1a'): 4,
        InstanceMarket('i2.8xlarge', 'us-west-1a'): 2,
        InstanceMarket('i2.8xlarge', 'us-west-2a'): 1,
    })
    cluster.ebs_storage += 3000
    return cluster


@pytest.yield_fixture
def fake_markets():
    with mock.patch('clusterman.aws.markets.EC2_INSTANCE_TYPES') as mock_instance_types, \
            mock.patch('clusterman.aws.markets.EC2_AZS') as mock_azs:
        mock_instance_types.__contains__.return_value = True
        mock_azs.__contains__.return_value = True
        yield


def test_valid_market(fake_markets):
    InstanceMarket('foo', 'bar')


def test_invalid_market():
    with pytest.raises(ValueError):
        InstanceMarket('foo', 'bar')


def test_modify_size(cluster):
    cluster.simulator.current_time.shift(seconds=+76)
    added_instances, removed_instances = cluster.modify_size({
        InstanceMarket('m4.4xlarge', 'us-west-1a'): 1,
        InstanceMarket('i2.8xlarge', 'us-west-1a'): 4,
    })
    assert len(added_instances) == 2
    assert len(removed_instances) == 4
    assert len(cluster) == 5


def test_cpu_mem_disk(cluster):
    assert len(cluster) == 7
    assert cluster.cpus == 160
    assert cluster.mem == 988
    assert cluster.disk == 22200


def test_remove_instances(cluster):
    cluster.simulator.current_time.shift(seconds=+42)
    cluster.modify_size({
        InstanceMarket('m4.4xlarge', 'us-west-1a'): 1,
        InstanceMarket('i2.8xlarge', 'us-west-1a'): 1,
    })

    assert len(cluster) == 2
    assert cluster.cpus == 48
    assert cluster.mem == 308
    assert cluster.disk == 9400


def test_terminate_instances_by_id(cluster):
    terminate_instances_ids = []
    remaining_instances_ids = []
    for i, id in enumerate(cluster.instances):
        if i % 3:
            terminate_instances_ids.append(id)
        else:
            remaining_instances_ids.append(id)
    cluster.terminate_instances_by_id(terminate_instances_ids)
    for id in terminate_instances_ids:
        assert id not in cluster.instances
    for id in remaining_instances_ids:
        assert id in cluster.instances
