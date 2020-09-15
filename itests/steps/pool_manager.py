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
import staticconf.testing
from hamcrest import assert_that
from hamcrest import close_to
from hamcrest import contains
from hamcrest import equal_to

from clusterman.autoscaler.pool_manager import PoolManager
from clusterman.aws.auto_scaling_resource_group import AutoScalingResourceGroup
from clusterman.aws.client import autoscaling
from clusterman.aws.client import ec2
from clusterman.aws.spot_fleet_resource_group import SpotFleetResourceGroup
from clusterman.exceptions import ResourceGroupError
from itests.environment import boto_patches
from itests.environment import make_asg
from itests.environment import make_sfr


def mock_asgs(num, subnet_id):
    asgs = {}
    for i in range(num):
        asg_id = f'fake-asg-{i}'
        make_asg(asg_id, subnet_id)
        asgs[asg_id] = AutoScalingResourceGroup(asg_id)
    return asgs


def mock_sfrs(num, subnet_id):
    sfrgs = {}
    for _ in range(num):
        sfr = make_sfr(subnet_id)
        sfrid = sfr['SpotFleetRequestId']
        sfrgs[sfrid] = SpotFleetResourceGroup(sfrid)
    return sfrgs


@behave.fixture
def mock_agents_by_ip_and_tasks(context):
    def get_agents_by_ip():
        agents = {}
        for reservation in ec2.describe_instances()['Reservations']:
            for instance in reservation['Instances']:
                ip_addr = instance['PrivateIpAddress']
                agents[ip_addr] = {
                    'pid': f'slave(1)@{ip_addr}:1',
                    'id': f'{instance["InstanceId"]}',
                    'hostname': 'host1',
                }
        return agents

    with mock.patch(
        'clusterman.mesos.mesos_cluster_connector.MesosClusterConnector._get_agents_by_ip',
        side_effect=get_agents_by_ip,
    ), mock.patch(
        'clusterman.mesos.mesos_cluster_connector.MesosClusterConnector._get_tasks_and_frameworks',
        return_value=([], []),
    ), staticconf.testing.PatchConfiguration(
        {'scaling_limits': {'max_weight_to_remove': 1000}},
        namespace='bar.mesos_config',
    ), mock.patch(
        'clusterman.aws.aws_resource_group.gethostbyaddr',
        return_value=('the-host', '', ''),
    ):
        yield


@behave.given('a pool manager with (?P<num>\d+) (?P<rg_type>asg|sfr) resource groups?')
def make_pool_manager(context, num, rg_type):
    behave.use_fixture(boto_patches, context)
    behave.use_fixture(mock_agents_by_ip_and_tasks, context)
    context.rg_type = rg_type
    with mock.patch(
        'clusterman.aws.auto_scaling_resource_group.AutoScalingResourceGroup.load',
        return_value={},
    ) as mock_asg_load, mock.patch(
        'clusterman.aws.spot_fleet_resource_group.SpotFleetResourceGroup.load',
        return_value={},
    ) as mock_sfr_load:
        if context.rg_type == 'asg':
            mock_asg_load.return_value = mock_asgs(int(num), context.subnet_id)
        elif context.rg_type == 'sfr':
            mock_sfr_load.return_value = mock_sfrs(int(num), context.subnet_id)
        context.pool_manager = PoolManager('mesos-test', 'bar', 'mesos')
    context.rg_ids = [i for i in context.pool_manager.resource_groups]
    context.pool_manager.max_capacity = 101


@behave.given('the fulfilled capacity of resource group (?P<rg_index>\d+) is (?P<capacity>\d+)')
def external_target_capacity(context, rg_index, capacity):
    rg_index = int(rg_index) - 1
    if context.rg_type == 'asg':
        autoscaling.set_desired_capacity(
            AutoScalingGroupName=f'fake-asg-{rg_index}',
            DesiredCapacity=int(capacity),
            HonorCooldown=True,
        )
    elif context.rg_type == 'sfr':
        ec2.modify_spot_fleet_request(
            SpotFleetRequestId=context.rg_ids[rg_index],
            TargetCapacity=int(capacity),
        )

    # make sure our non orphan fulfilled capacity is up-to-date
    with mock.patch('clusterman.autoscaler.pool_manager.PoolManager._reload_resource_groups'):
        context.pool_manager.reload_state()


@behave.given('we request (?P<capacity>\d+) capacity')
@behave.when('we request (?P<capacity>\d+) capacity(?P<dry_run> and dry-run is active)?')
def modify_capacity(context, capacity, dry_run=False):
    dry_run = True if dry_run else False
    context.pool_manager.prune_excess_fulfilled_capacity = mock.Mock()
    context.original_capacities = [rg.target_capacity for rg in context.pool_manager.resource_groups.values()]
    context.pool_manager.modify_target_capacity(int(capacity), dry_run=dry_run)


@behave.when('resource group (?P<rgid>\d+) is broken')
def broken_resource_group(context, rgid):
    rg = list(context.pool_manager.resource_groups.values())[0]
    rg.modify_target_capacity = mock.Mock(side_effect=ResourceGroupError('resource group is broken'))


@behave.given('we mark resource group (?P<rgid>\d+) as stale')
@behave.when('we mark resource group (?P<rgid>\d+) as stale')
def stale_resource_group(context, rgid):
    rg = list(context.pool_manager.resource_groups.values())[0]
    rg.mark_stale(False)
    context.stale_instances = rg.instance_ids


@behave.then('the resource groups should be at minimum capacity')
def check_at_min_capacity(context):
    for rg in context.pool_manager.resource_groups.values():
        assert_that(rg.target_capacity, equal_to(1))


@behave.then('the resource group capacities should not change')
def check_unchanged_capacity(context):
    assert_that(
        [rg.target_capacity for rg in context.pool_manager.resource_groups.values()],
        contains(*context.original_capacities),
    )


@behave.then("the first resource group's capacity should not change")
def check_first_rg_capacity_unchanged(context):
    assert_that(
        context.pool_manager.resource_groups[context.rg_ids[0]].target_capacity,
        equal_to(context.original_capacities[0]),
    )


@behave.then('the(?P<remaining> remaining)? resource groups should have evenly-balanced capacity')
def check_target_capacity(context, remaining):
    target_capacity = 0
    if remaining:
        desired_capacity = (
            (context.pool_manager.target_capacity - context.original_capacities[0]) / (len(context.rg_ids) - 1)
        )
    else:
        desired_capacity = context.pool_manager.target_capacity / len(context.rg_ids)

    for i, rg in enumerate(context.pool_manager.resource_groups.values()):
        target_capacity += rg.target_capacity
        if remaining and i == 0:
            continue
        assert_that(
            rg.target_capacity,
            close_to(desired_capacity, 1.0),
        )
    assert_that(target_capacity, equal_to(context.pool_manager.target_capacity))


@behave.then('resource group (?P<rgid>\d+) should have (?P<capacity>\d+) instances')
def target_capacity_equals(context, rgid, capacity):
    assert_that(
        len(context.pool_manager.resource_groups[context.rg_ids[int(rgid) - 1]].instance_ids),
        equal_to(int(capacity)),
    )
