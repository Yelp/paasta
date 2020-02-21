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
import behave
import mock
from hamcrest import assert_that
from hamcrest import equal_to
from hamcrest import only_contains

from clusterman.aws.client import ec2
from clusterman.aws.client import ec2_describe_instances


@behave.fixture
def mock_rg_is_stale(context):
    response = ec2.describe_spot_fleet_requests(SpotFleetRequestIds=context.rg_ids)
    for config in response['SpotFleetRequestConfigs']:
        if config['SpotFleetRequestId'] == context.stale_rg_id:
            config['SpotFleetRequestState'] = 'cancelled_running'

    def mock_describe_sfrs(SpotFleetRequestIds):
        return {'SpotFleetRequestConfigs': [
            c
            for c in response['SpotFleetRequestConfigs']
            if c['SpotFleetRequestId'] in SpotFleetRequestIds
        ]}

    with mock.patch(
        'clusterman.aws.spot_fleet_resource_group.ec2.describe_spot_fleet_requests',
        side_effect=mock_describe_sfrs,
    ):
        yield


@behave.given('resource group (?P<rg_index>\d+) is stale')
def resource_group_is_stale(context, rg_index):
    context.stale_rg_id = context.rg_ids[int(rg_index) - 1]
    behave.use_fixture(mock_rg_is_stale, context)


@behave.given('we can kill at most (?P<max_tasks_to_kill>\d+) tasks?')
def killed_tasks(context, max_tasks_to_kill):
    context.pool_manager.max_tasks_to_kill = int(max_tasks_to_kill)


@behave.given('there are no killable instances')
def no_killable_instances(context):
    context.pool_manager._get_prioritized_killable_nodes = mock.Mock(return_value=[])


@behave.given('the killable instance has weight (?P<weight>\d+)')
def killable_instance_with_weight(context, weight):
    context.pool_manager.resource_groups[context.rg_ids[0]].market_weight = mock.Mock(return_value=int(weight))
    context.pool_manager._get_prioritized_killable_nodes = mock.Mock(return_value=[
        context.pool_manager.get_node_metadatas()[0],
    ])


@behave.given('the max weight to remove is (?P<weight>\d+)')
def max_weight_to_remove(context, weight):
    context.pool_manager.max_weight_to_remove = int(weight)


@behave.given('the killable instance has (?P<tasks>\d+) tasks')
def killable_instance_with_tasks(context, tasks):
    def get_tasks_and_frameworks():
        rg = context.pool_manager.resource_groups[context.rg_ids[0]]
        instances = ec2_describe_instances(instance_ids=rg.instance_ids[:1])
        return (
            [
                {'slave_id': instances[0]['InstanceId'], 'state': 'TASK_RUNNING', 'framework_id': 'framework_a'}
            ] * int(tasks),
            {'framework_a': {'name': 'framework_a_name'}},
            {},
        )

    context.pool_manager.cluster_connector._get_tasks_and_frameworks.side_effect = get_tasks_and_frameworks
    context.pool_manager.cluster_connector.reload_state()
    context.pool_manager.cluster_connector._batch_tasks_per_mesos_agent = {
        i['id']: 0 for i in context.pool_manager.cluster_connector._agents_by_ip.values()
    }
    context.pool_manager._get_prioritized_killable_nodes = mock.Mock(return_value=[
        context.pool_manager.get_node_metadatas()[0],
    ])


@behave.given('the non-orphaned fulfilled capacity is (?P<nofc>\d+)')
def set_non_orphaned_fulfilled_capacity(context, nofc):
    context.nofc = int(nofc)
    context.pool_manager.non_orphan_fulfilled_capacity = context.nofc


@behave.when('we prune excess fulfilled capacity to (?P<target>\d+)')
def prune_excess_fulfilled_capacity(context, target):
    context.original_agents = context.pool_manager.get_node_metadatas()
    with mock.patch(
        'clusterman.aws.spot_fleet_resource_group.SpotFleetResourceGroup.target_capacity',
        mock.PropertyMock(side_effect=[int(target)] + [0] * (len(context.rg_ids) - 1)),
    ):
        context.pool_manager.prune_excess_fulfilled_capacity(new_target_capacity=int(target))


@behave.then('(?P<num>\d+) instances? should be killed')
def check_n_instances_killed(context, num):
    running_instances = [
        i
        for reservation in ec2.describe_instances()['Reservations']
        for i in reservation['Instances']
        if i['State']['Name'] == 'running'
    ]
    context.killed_nodes = [
        n
        for n in context.original_agents
        if n.instance.instance_id not in [i['InstanceId'] for i in running_instances]
    ]
    assert_that(len(context.killed_nodes), equal_to(int(num)))


@behave.then('the killed instances are from resource group (?P<rg_index>\d+)')
def check_killed_instance_group(context, rg_index):
    assert_that(
        [n.instance.group_id for n in context.killed_nodes],
        only_contains(context.rg_ids[int(rg_index) - 1]),
    )


@behave.then('the killed instances should be stale')
def check_killed_instances_stale(context):
    assert_that(
        [n.instance.is_stale for n in context.killed_nodes],
        only_contains(True)
    )
