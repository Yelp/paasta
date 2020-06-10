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
import time
from decimal import Decimal

import behave
import mock
import staticconf.testing
from hamcrest import assert_that
from hamcrest import contains
from hamcrest import equal_to
from moto import mock_dynamodb2

from clusterman.autoscaler.autoscaler import Autoscaler
from clusterman.autoscaler.pool_manager import PoolManager
from clusterman.autoscaler.signals import ACK
from clusterman.aws.client import dynamodb
from clusterman.aws.spot_fleet_resource_group import SpotFleetResourceGroup
from clusterman.util import AUTOSCALER_PAUSED
from clusterman.util import CLUSTERMAN_STATE_TABLE
from itests.environment import boto_patches


@behave.fixture
def autoscaler_patches(context):
    behave.use_fixture(boto_patches, context)
    rg1 = mock.Mock(spec=SpotFleetResourceGroup, id='rg1', target_capacity=10,
                    fulfilled_capacity=10, is_stale=False, min_capacity=0, max_capacity=float('inf'))
    rg2 = mock.Mock(spec=SpotFleetResourceGroup, id='rg2', target_capacity=10,
                    fulfilled_capacity=10, is_stale=False, min_capacity=0, max_capacity=float('inf'))

    resource_totals = {'cpus': 80, 'mem': 1000, 'disk': 1000, 'gpus': 0}

    with staticconf.testing.PatchConfiguration(
        {'autoscaling': {'default_signal_role': 'bar'}},
    ), mock.patch(
        'clusterman.autoscaler.autoscaler.get_monitoring_client',
    ), mock.patch(
        'clusterman.aws.util.SpotFleetResourceGroup.load',
        return_value={rg1.id: rg1, rg2.id: rg2},
    ), mock.patch(
        'clusterman.autoscaler.pool_manager.PoolManager',
        wraps=PoolManager,
    ), mock.patch(
        'clusterman.autoscaler.autoscaler.PoolManager.prune_excess_fulfilled_capacity',
    ), mock.patch(
        'clusterman.autoscaler.pool_manager.ClusterConnector.load',
    ) as mock_cluster_connector, mock.patch(
        'clusterman.autoscaler.autoscaler.PoolManager._calculate_non_orphan_fulfilled_capacity',
        return_value=20,
    ), mock.patch(
        'clusterman.autoscaler.signals.Signal._connect_to_signal_process',
    ), mock.patch(
        'clusterman.autoscaler.autoscaler.Signal._get_metrics',
    ) as mock_metrics, mock_dynamodb2():
        dynamodb.create_table(
            TableName=CLUSTERMAN_STATE_TABLE,
            KeySchema=[
                {'AttributeName': 'state', 'KeyType': 'HASH'},
                {'AttributeName': 'entity', 'KeyType': 'SORT'},
            ],
            AttributeDefinitions=[
                {'AttributeName': 'state', 'AttributeType': 'S'},
                {'AttributeName': 'entity', 'AttributeType': 'S'},
            ],
        )
        mock_metrics.return_value = {}  # don't know why this is necessary but we get flaky tests if it's not set
        mock_cluster_connector.return_value.get_resource_total.side_effect = resource_totals.__getitem__
        yield


def mock_historical_metrics(metric_name, metric_type, time_start, time_end, extra_dimensions):
    if metric_name == 'non_orphan_fulfilled_capacity':
        return {'non_orphan_fulfilled_capacity': [
            (Decimal('100'), Decimal('20')),
            (Decimal('110'), Decimal('25')),
            (Decimal('130'), Decimal('23')),
            (Decimal('140'), Decimal('0')),
            (Decimal('150'), Decimal('27')),
            (Decimal('160'), Decimal('0')),
        ]}
    elif metric_name == 'cpus_total':
        return {'cpus_total': [
            (Decimal('100'), Decimal('15')),
            (Decimal('110'), Decimal('17')),
            (Decimal('130'), Decimal('16')),
            (Decimal('140'), Decimal('0')),
            (Decimal('150'), Decimal('19')),
            (Decimal('160'), Decimal('0')),
        ]}
    elif metric_name == 'mem_total':
        return {'mem_total': [
            (Decimal('100'), Decimal('0')),
            (Decimal('110'), Decimal('0')),
            (Decimal('130'), Decimal('0')),
            (Decimal('140'), Decimal('0')),
            (Decimal('150'), Decimal('0')),
            (Decimal('160'), Decimal('0')),
        ]}
    elif metric_name == 'disk_total':
        return {'disk_total': [
            (Decimal('100'), Decimal('1000')),
            (Decimal('110'), Decimal('1000')),
            (Decimal('130'), Decimal('1000')),
            (Decimal('140'), Decimal('1000')),
            (Decimal('150'), Decimal('1000')),
            (Decimal('160'), Decimal('1000')),
        ]}
    elif metric_name == 'gpus_total':
        return {'gpus_total': [
            (Decimal('100'), Decimal('1')),
            (Decimal('110'), Decimal('1')),
            (Decimal('130'), Decimal('1')),
            (Decimal('140'), Decimal('1')),
            (Decimal('150'), Decimal('1')),
            (Decimal('160'), Decimal('1')),
        ]}


@behave.given('an autoscaler object')
def autoscaler(context):
    behave.use_fixture(autoscaler_patches, context)
    context.autoscaler = Autoscaler(
        cluster='mesos-test',
        pool='bar',
        apps=['bar'],
        scheduler='mesos',
        metrics_client=mock.Mock(),
        monitoring_enabled=False,
    )


@behave.when('the autoscaler is paused')
def pause_autoscaler(context):
    dynamodb.put_item(
        TableName=CLUSTERMAN_STATE_TABLE,
        Item={
            'state': {'S': AUTOSCALER_PAUSED},
            'entity': {'S': 'mesos-test.bar.mesos'},
            'expiration_timestamp': {'N': str(time.time() + 100000)}
        }
    )


@behave.when('the pool is empty')
def empty_pool(context):
    manager = context.autoscaler.pool_manager
    groups = list(manager.resource_groups.values())
    groups[0].target_capacity = 0
    groups[1].target_capacity = 0
    groups[0].fulfilled_capacity = 0
    groups[1].fulfilled_capacity = 0
    manager.min_capacity = 0
    manager.cluster_connector.get_resource_capacity = mock.Mock(return_value=0)
    manager.non_orphan_fulfilled_capacity = 0


@behave.when('metrics history (?P<exists>yes|no)')
def populate_metrics_history(context, exists):
    if exists == 'yes':
        context.autoscaler.metrics_client.get_metric_values.side_effect = mock_historical_metrics
    else:
        context.autoscaler.metrics_client.get_metric_values.side_effect = lambda name, *args, **kwargs: {name: []}


@behave.when('the signal resource request is (?P<value>\d+ cpus|\d+ gpus|empty)')
def signal_resource_request(context, value):
    if value == 'empty':
        resources = '{}' if value == 'empty' else '{'
    else:
        n, t = value.split(' ')
        resources = '{"' + t + '":' + n + '}'
    context.autoscaler.signal._signal_conn.recv.side_effect = [ACK, ACK, '{"Resources": ' + resources + '}'] * 2
    try:
        context.autoscaler.run()
    except Exception as e:
        context.exception = e
    else:
        try:
            del(context.exception)
        except AttributeError:
            pass

    if not hasattr(context, 'exception'):
        # run it a second time to make sure nothing's changed
        context.autoscaler.run()


@behave.then('the autoscaler should scale rg(?P<rg>[12]) to (?P<target>\d+) capacity')
def rg_capacity_change(context, rg, target):
    groups = list(context.autoscaler.pool_manager.resource_groups.values())
    if int(target) != groups[int(rg) - 1].target_capacity:
        assert_that(
            groups[int(rg) - 1].modify_target_capacity.call_args_list,
            contains(
                mock.call(int(target), dry_run=False),
                mock.call(int(target), dry_run=False),
            ),
        )
    else:
        assert_that(groups[int(rg) - 1].modify_target_capacity.call_count, equal_to(0))


@behave.then('the autoscaler should do nothing')
def rg_do_nothing(context):
    groups = list(context.autoscaler.pool_manager.resource_groups.values())
    for g in groups:
        assert_that(g.modify_target_capacity.call_count, equal_to(0))
