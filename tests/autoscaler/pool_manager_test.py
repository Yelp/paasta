# Copyright 2019 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import mock
import pytest
import staticconf
import staticconf.testing

from clusterman.autoscaler.pool_manager import ClusterNodeMetadata
from clusterman.autoscaler.pool_manager import PoolManager
from clusterman.aws.aws_resource_group import AWSResourceGroup
from clusterman.exceptions import AllResourceGroupsAreStaleError
from clusterman.exceptions import PoolManagerError
from clusterman.exceptions import ResourceGroupError
from clusterman.interfaces.cluster_connector import AgentMetadata
from clusterman.interfaces.cluster_connector import AgentState
from clusterman.interfaces.resource_group import InstanceMetadata


def _make_metadata(
    rg_id,
    instance_id,
    agent_state=AgentState.RUNNING,
    is_stale=False,
    weight=1,
    tasks=5,
    batch_tasks=0,
):
    return ClusterNodeMetadata(
        AgentMetadata(
            agent_id='foo',
            batch_task_count=batch_tasks,
            state=agent_state,
            task_count=tasks,
        ),
        InstanceMetadata(
            group_id=rg_id,
            hostname='host1',
            instance_id=instance_id,
            ip_address='1.2.3.4',
            is_stale=is_stale,
            market='market-1',
            state='running',
            uptime=1000,
            weight=weight,
        ),
    )


@pytest.fixture
def mock_resource_groups():
    return {
        f'sfr-{i}': mock.Mock(
            id=f'sfr-{i}',
            instance_ids=[f'i-{i}'],
            target_capacity=i * 2 + 1,
            fulfilled_capacity=i * 6,
            market_capacities={'market-1': i, 'market-2': i * 2, 'market-3': i * 3},
            is_stale=False,
            market_weight=mock.Mock(return_value=1.0),
            terminate_instances_by_id=mock.Mock(return_value=[]),
            spec=AWSResourceGroup,
            mark_stale=mock.Mock(side_effect=NotImplementedError),
            min_capacity=0,
            max_capacity=float('inf'),
        )
        for i in range(7)
    }


@pytest.fixture
def mock_pool_manager(mock_resource_groups):
    with mock.patch(
        'clusterman.aws.spot_fleet_resource_group.SpotFleetResourceGroup.load',
        return_value={},
    ), mock.patch(
        'clusterman.autoscaler.pool_manager.DrainingClient',
        autospec=True,
    ), mock.patch(
        'clusterman.autoscaler.pool_manager.PoolManager.reload_state'
    ), mock.patch(
        'clusterman.autoscaler.pool_manager.ClusterConnector.load',
    ):
        manager = PoolManager('mesos-test', 'bar', 'mesos')
        manager.resource_groups = mock_resource_groups

        return manager


def test_pool_manager_init(mock_pool_manager, mock_resource_groups):
    assert mock_pool_manager.cluster == 'mesos-test'
    assert mock_pool_manager.pool == 'bar'
    assert mock_pool_manager.scheduler == 'mesos'
    with staticconf.testing.MockConfiguration(
        {
            'scaling_limits': {
                'max_tasks_to_kill': 'inf',
                'max_weight_to_add': 100,
                'max_weight_to_remove': 100,
                'min_capacity': 3,
                'max_capacity': 3,
            },
        },
        namespace='bar.mesos_config',
    ), mock.patch(
        'clusterman.aws.spot_fleet_resource_group.SpotFleetResourceGroup.load',
        return_value={},
    ), mock.patch(
        'clusterman.autoscaler.pool_manager.DrainingClient',
        autospec=True,
    ), mock.patch(
        'clusterman.autoscaler.pool_manager.PoolManager.reload_state'
    ):
        mock_manager = PoolManager('mesos-test', 'bar', 'mesos')
        mock_manager.resource_groups = mock_resource_groups
        assert mock_manager.max_tasks_to_kill == float('inf')


def test_mark_stale(mock_pool_manager, caplog):
    mock_pool_manager.mark_stale(False)
    for r in caplog.records:
        if r.message:
            assert 'Skipping' in r.message


def test_modify_target_capacity_no_resource_groups(mock_pool_manager):
    mock_pool_manager.resource_groups = []
    with pytest.raises(PoolManagerError):
        mock_pool_manager.modify_target_capacity(1234)


def test_modify_target_capacity_skip_failing_group(mock_pool_manager):
    list(mock_pool_manager.resource_groups.values())[0].modify_target_capacity.side_effect = ResourceGroupError('foo')
    with mock.patch('clusterman.autoscaler.pool_manager.get_monitoring_client') as mock_monitoring_client:
        mock_pool_manager.modify_target_capacity(1234)
        assert mock_monitoring_client.return_value.create_counter.call_count == 1


@pytest.mark.parametrize('new_target,constrained_target', ((100, 90), (10, 49)))
def test_modify_target_capacity(new_target, constrained_target, mock_pool_manager):
    mock_pool_manager.prune_excess_fulfilled_capacity = mock.Mock()
    mock_pool_manager._constrain_target_capacity = mock.Mock(return_value=constrained_target)
    mock_pool_manager._compute_new_resource_group_targets = mock.Mock(return_value={f'sfr-{i}': i for i in range(7)})

    assert mock_pool_manager.modify_target_capacity(new_target) == constrained_target
    assert mock_pool_manager._constrain_target_capacity.call_count == 1
    assert mock_pool_manager.prune_excess_fulfilled_capacity.call_count == 1
    assert mock_pool_manager._compute_new_resource_group_targets.call_count == 1
    for i, group in enumerate(mock_pool_manager.resource_groups.values()):
        assert group.modify_target_capacity.call_count == 1
        assert group.modify_target_capacity.call_args[0][0] == i


class TestPruneExcessFulfilledCapacity:
    @pytest.fixture
    def mock_nodes_to_prune(self):
        return {
            'sfr-1': [mock.Mock(instance=mock.Mock(instance_id=1))],
            'sfr-3': [
                mock.Mock(instance=mock.Mock(instance_id=4)),
                mock.Mock(instance=mock.Mock(instance_id=5)),
                mock.Mock(instance=mock.Mock(instance_id=6)),
            ],
        }

    @pytest.fixture
    def mock_pool_manager(self, mock_pool_manager, mock_nodes_to_prune):
        mock_pool_manager._choose_nodes_to_prune = mock.Mock(return_value=mock_nodes_to_prune)
        mock_pool_manager.draining_client = mock.Mock()
        mock_pool_manager.terminate_instances_by_id = mock.Mock()
        return mock_pool_manager

    def test_dry_run(self, mock_pool_manager):
        mock_pool_manager.prune_excess_fulfilled_capacity(100, dry_run=True)
        assert mock_pool_manager.draining_client.submit_instance_for_draining.call_count == 0
        assert mock_pool_manager.terminate_instances_by_id.call_count == 0

    def test_drain_queue(self, mock_pool_manager, mock_nodes_to_prune):
        mock_pool_manager.draining_enabled = True
        mock_pool_manager.prune_excess_fulfilled_capacity(100)
        assert mock_pool_manager.draining_client.submit_instance_for_draining.call_args_list == [
            mock.call(mock_nodes_to_prune['sfr-1'][0].instance, sender=AWSResourceGroup, scheduler='mesos'),
            mock.call(mock_nodes_to_prune['sfr-3'][0].instance, sender=AWSResourceGroup, scheduler='mesos'),
            mock.call(mock_nodes_to_prune['sfr-3'][1].instance, sender=AWSResourceGroup, scheduler='mesos'),
            mock.call(mock_nodes_to_prune['sfr-3'][2].instance, sender=AWSResourceGroup, scheduler='mesos'),
        ]

    def test_terminate_immediately(self, mock_pool_manager):
        mock_pool_manager.prune_excess_fulfilled_capacity(100)
        assert mock_pool_manager.resource_groups['sfr-1'].terminate_instances_by_id.call_args == mock.call([1])
        assert mock_pool_manager.resource_groups['sfr-3'].terminate_instances_by_id.call_args == mock.call([4, 5, 6])


@mock.patch('clusterman.autoscaler.pool_manager.logger', autospec=True)
class TestReloadResourceGroups:
    def test_malformed_config(self, mock_logger, mock_pool_manager):
        with staticconf.testing.MockConfiguration(
            {'resource_groups': ['asdf']},
            namespace='bar.mesos_config',
        ):
            mock_pool_manager.pool_config = staticconf.NamespaceReaders('bar.mesos_config')
            mock_pool_manager._reload_resource_groups()

        assert not mock_pool_manager.resource_groups
        assert 'Malformed config' in mock_logger.error.call_args[0][0]

    def test_unknown_rg_type(self, mock_logger, mock_pool_manager):
        with staticconf.testing.MockConfiguration(
            {'resource_groups': [{'fake_rg_type': 'bar'}]},
            namespace='bar.mesos_config',
        ):
            mock_pool_manager.pool_config = staticconf.NamespaceReaders('bar.mesos_config')
            mock_pool_manager._reload_resource_groups()

        assert not mock_pool_manager.resource_groups
        assert 'Unknown resource group' in mock_logger.error.call_args[0][0]

    def test_successful(self, mock_logger, mock_pool_manager):
        with mock.patch.dict(
            'clusterman.autoscaler.pool_manager.RESOURCE_GROUPS',
            {'sfr': mock.Mock(load=mock.Mock(return_value={'rg1': mock.Mock()}))},
        ), staticconf.testing.PatchConfiguration(
            {'resource_groups': [{'sfr': {'tag': 'puppet:role::paasta'}}]},
            namespace='bar.mesos_config',
        ):
            mock_pool_manager._reload_resource_groups()

        assert len(mock_pool_manager.resource_groups) == 1
        assert 'rg1' in mock_pool_manager.resource_groups


@mock.patch('clusterman.autoscaler.pool_manager.logger')
@pytest.mark.parametrize('force', [True, False])
class TestConstrainTargetCapacity:
    def test_positive_delta(self, mock_logger, force, mock_pool_manager):
        assert mock_pool_manager._constrain_target_capacity(100, force) == 100
        assert mock_pool_manager._constrain_target_capacity(1000, force) == (1000 if force else 249)
        mock_pool_manager.max_capacity = 97
        assert mock_pool_manager._constrain_target_capacity(1000, force) == (1000 if force else 97)
        assert mock_logger.warning.call_count == 2

    def test_negative_delta(self, mock_logger, force, mock_pool_manager):
        assert mock_pool_manager._constrain_target_capacity(40, force) == 40
        assert mock_pool_manager._constrain_target_capacity(20, force) == (20 if force else 39)
        mock_pool_manager.min_capacity = 45
        assert mock_pool_manager._constrain_target_capacity(20, force) == (20 if force else 45)
        assert mock_logger.warning.call_count == 2

    def test_zero_delta(self, mock_logger, force, mock_pool_manager):
        assert mock_pool_manager._constrain_target_capacity(49, force) == 49


@mock.patch('clusterman.autoscaler.pool_manager.logger', autospec=True)
class TestChooseNodesToPrune:

    @pytest.fixture
    def mock_pool_manager(self, mock_pool_manager):
        mock_pool_manager.non_orphan_fulfilled_capacity = 126
        return mock_pool_manager

    def test_fulfilled_capacity_under_target(self, mock_logger, mock_pool_manager):
        assert mock_pool_manager._choose_nodes_to_prune(300, None) == {}

    def test_no_nodes_to_kill(self, mock_logger, mock_pool_manager):
        mock_pool_manager._get_prioritized_killable_nodes = mock.Mock(return_value=[])
        assert mock_pool_manager._choose_nodes_to_prune(100, None) == {}

    def test_killable_node_max_weight_to_remove(self, mock_logger, mock_pool_manager):
        mock_pool_manager._get_prioritized_killable_nodes = mock.Mock(return_value=[
            _make_metadata('sfr-1', 'i-1', weight=1000)
        ])
        assert mock_pool_manager._choose_nodes_to_prune(100, None) == {}
        assert 'would take us over our max_weight_to_remove' in mock_logger.info.call_args[0][0]

    def test_killable_node_under_group_capacity(self, mock_logger, mock_pool_manager):
        mock_pool_manager._get_prioritized_killable_nodes = mock.Mock(return_value=[
            _make_metadata('sfr-1', 'i-1', weight=1000)
        ])
        mock_pool_manager.max_weight_to_remove = 10000
        assert mock_pool_manager._choose_nodes_to_prune(100, None) == {}
        assert 'is at target capacity' in mock_logger.info.call_args[0][0]

    def test_killable_node_too_many_tasks(self, mock_logger, mock_pool_manager):
        mock_pool_manager._get_prioritized_killable_nodes = mock.Mock(return_value=[
            _make_metadata('sfr-1', 'i-1')
        ])
        assert mock_pool_manager._choose_nodes_to_prune(100, None) == {}
        assert 'would take us over our max_tasks_to_kill' in mock_logger.info.call_args[0][0]

    def test_killable_nodes_under_target_capacity(self, mock_logger, mock_pool_manager):
        mock_pool_manager._get_prioritized_killable_nodes = mock.Mock(return_value=[
            _make_metadata('sfr-1', 'i-1', weight=2)
        ])
        mock_pool_manager.max_tasks_to_kill = 100
        assert mock_pool_manager._choose_nodes_to_prune(125, None) == {}
        assert 'would take us under our target_capacity' in mock_logger.info.call_args[0][0]

    def test_kill_node(self, mock_logger, mock_pool_manager):
        mock_pool_manager._get_prioritized_killable_nodes = mock.Mock(return_value=[
            _make_metadata('sfr-1', 'i-1', weight=2)
        ])
        mock_pool_manager.max_tasks_to_kill = 100
        assert mock_pool_manager._choose_nodes_to_prune(100, None)['sfr-1'][0].instance.instance_id == 'i-1'


def test_compute_new_resource_group_targets_no_unfilled_capacity(mock_pool_manager):
    target_capacity = mock_pool_manager.target_capacity
    assert sorted(list(mock_pool_manager._compute_new_resource_group_targets(target_capacity).values())) == [
        group.target_capacity
        for group in (mock_pool_manager.resource_groups.values())
    ]


@pytest.mark.parametrize('orig_targets', [10, 17])
def test_compute_new_resource_group_targets_all_equal(orig_targets, mock_pool_manager):
    for group in mock_pool_manager.resource_groups.values():
        group.target_capacity = orig_targets

    num_groups = len(mock_pool_manager.resource_groups)
    new_targets = mock_pool_manager._compute_new_resource_group_targets(105)
    assert sorted(list(new_targets.values())) == [15] * num_groups


@pytest.mark.parametrize('orig_targets', [10, 17])
def test_compute_new_resource_group_targets_all_equal_with_remainder(orig_targets, mock_pool_manager):
    for group in mock_pool_manager.resource_groups.values():
        group.target_capacity = orig_targets

    new_targets = mock_pool_manager._compute_new_resource_group_targets(107)
    assert sorted(list(new_targets.values())) == [15, 15, 15, 15, 15, 16, 16]


def test_compute_new_resource_group_targets_uneven_scale_up(mock_pool_manager):
    new_targets = mock_pool_manager._compute_new_resource_group_targets(304)
    assert sorted(list(new_targets.values())) == [43, 43, 43, 43, 44, 44, 44]


def test_compute_new_resource_group_targets_uneven_scale_down(mock_pool_manager):
    for group in mock_pool_manager.resource_groups.values():
        group.target_capacity += 20

    new_targets = mock_pool_manager._compute_new_resource_group_targets(10)
    assert sorted(list(new_targets.values())) == [1, 1, 1, 1, 2, 2, 2]


def test_compute_new_resource_group_targets_above_delta_scale_up(mock_pool_manager):
    new_targets = mock_pool_manager._compute_new_resource_group_targets(62)
    assert sorted(list(new_targets.values())) == [7, 7, 7, 8, 9, 11, 13]


def test_compute_new_resource_group_targets_below_delta_scale_down(mock_pool_manager):
    new_targets = mock_pool_manager._compute_new_resource_group_targets(30)
    assert sorted(list(new_targets.values())) == [1, 3, 5, 5, 5, 5, 6]


def test_compute_new_resource_group_targets_above_delta_equal_scale_up(mock_pool_manager):
    for group in list(mock_pool_manager.resource_groups.values())[3:]:
        group.target_capacity = 20

    new_targets = mock_pool_manager._compute_new_resource_group_targets(100)
    assert sorted(list(new_targets.values())) == [6, 7, 7, 20, 20, 20, 20]


def test_compute_new_resource_group_targets_below_delta_equal_scale_down(mock_pool_manager):
    for group in list(mock_pool_manager.resource_groups.values())[:3]:
        group.target_capacity = 1

    new_targets = mock_pool_manager._compute_new_resource_group_targets(20)
    assert sorted(list(new_targets.values())) == [1, 1, 1, 4, 4, 4, 5]


def test_compute_new_resource_group_targets_above_delta_equal_scale_up_2(mock_pool_manager):
    for group in list(mock_pool_manager.resource_groups.values())[3:]:
        group.target_capacity = 20

    new_targets = mock_pool_manager._compute_new_resource_group_targets(145)
    assert sorted(list(new_targets.values())) == [20, 20, 21, 21, 21, 21, 21]


def test_compute_new_resource_group_targets_below_delta_equal_scale_down_2(mock_pool_manager):
    for group in list(mock_pool_manager.resource_groups.values())[:3]:
        group.target_capacity = 1

    new_targets = mock_pool_manager._compute_new_resource_group_targets(9)
    assert sorted(list(new_targets.values())) == [1, 1, 1, 1, 1, 2, 2]


def test_compute_new_resource_group_targets_all_rgs_are_stale(mock_pool_manager):
    for group in mock_pool_manager.resource_groups.values():
        group.is_stale = True

    with pytest.raises(AllResourceGroupsAreStaleError):
        mock_pool_manager._compute_new_resource_group_targets(9)


@pytest.mark.parametrize('non_stale_capacity', [1, 5])
def test_compute_new_resource_group_targets_scale_up_stale_pools_0(non_stale_capacity, mock_pool_manager):
    for group in list(mock_pool_manager.resource_groups.values())[:3]:
        group.target_capacity = non_stale_capacity
    for group in list(mock_pool_manager.resource_groups.values())[3:]:
        group.target_capacity = 3
        group.is_stale = True

    new_targets = mock_pool_manager._compute_new_resource_group_targets(6)
    assert new_targets == {'sfr-0': 2, 'sfr-1': 2, 'sfr-2': 2, 'sfr-3': 0, 'sfr-4': 0, 'sfr-5': 0, 'sfr-6': 0}


def test_get_market_capacities(mock_pool_manager):
    assert mock_pool_manager.get_market_capacities() == {
        'market-1': sum(i for i in range(7)),
        'market-2': sum(i * 2 for i in range(7)),
        'market-3': sum(i * 3 for i in range(7)),
    }
    assert mock_pool_manager.get_market_capacities(market_filter='market-2') == {
        'market-2': sum(i * 2 for i in range(7)),
    }


def test_target_capacity(mock_pool_manager):
    assert mock_pool_manager.target_capacity == sum(2 * i + 1 for i in range(7))


def test_fulfilled_capacity(mock_pool_manager):
    assert mock_pool_manager.fulfilled_capacity == sum(i * 6 for i in range(7))


def test_instance_kill_order(mock_pool_manager):
    mock_pool_manager.get_node_metadatas = mock.Mock(return_value=[
        _make_metadata('sfr-0', 'i-7', batch_tasks=100),
        _make_metadata('sfr-0', 'i-0', agent_state=AgentState.ORPHANED),
        _make_metadata('sfr-0', 'i-2', tasks=1, is_stale=True),
        _make_metadata('sfr-0', 'i-1', agent_state=AgentState.IDLE),
        _make_metadata('sfr-0', 'i-4', tasks=1),
        _make_metadata('sfr-0', 'i-5', tasks=100),
        _make_metadata('sfr-0', 'i-3', tasks=100, is_stale=True),
        _make_metadata('sfr-0', 'i-6', batch_tasks=1),
        _make_metadata('sfr-0', 'i-8', agent_state=AgentState.UNKNOWN),
        _make_metadata('sfr-0', 'i-9', tasks=100000),
    ])
    mock_pool_manager.max_tasks_to_kill = 1000
    killable_nodes = mock_pool_manager._get_prioritized_killable_nodes()
    killable_instance_ids = [node_metadata.instance.instance_id for node_metadata in killable_nodes]
    assert killable_instance_ids == [f'i-{i}' for i in range(8)]
