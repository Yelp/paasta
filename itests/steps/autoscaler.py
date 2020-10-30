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
# import pdb
import time
from decimal import Decimal

import behave
import mock
import staticconf.testing
from hamcrest import assert_that
from hamcrest import equal_to
from hamcrest import has_item
from kubernetes.client import V1Container
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V1Pod
from kubernetes.client import V1PodCondition
from kubernetes.client import V1PodSpec
from kubernetes.client import V1PodStatus
from kubernetes.client import V1ResourceRequirements
from kubernetes.client.models.v1_node import V1Node as KubernetesNode
from moto import mock_dynamodb2

from clusterman.autoscaler.autoscaler import Autoscaler
from clusterman.autoscaler.config import AutoscalingConfig
from clusterman.autoscaler.pool_manager import PoolManager
from clusterman.aws.client import dynamodb
from clusterman.aws.spot_fleet_resource_group import SpotFleetResourceGroup
from clusterman.kubernetes.kubernetes_cluster_connector import KubernetesClusterConnector
from clusterman.kubernetes.util import PodUnschedulableReason
from clusterman.signals.external_signal import ACK
from clusterman.util import AUTOSCALER_PAUSED
from clusterman.util import CLUSTERMAN_STATE_TABLE
from clusterman.util import ClustermanResources
from itests.environment import boto_patches


@behave.fixture
def autoscaler_patches(context):
    behave.use_fixture(boto_patches, context)
    resource_groups = {}
    for i in range(context.rgnum):
        resource_groups[f'rg{i}'] = mock.Mock(
            spec=SpotFleetResourceGroup,
            id=f'rg{i}',
            target_capacity=context.target_capacity / context.rgnum,
            fulfilled_capacity=context.target_capacity / context.rgnum,
            is_stale=False,
            min_capacity=0,
            max_capacity=float('inf'),
        )

    resource_totals = ClustermanResources(cpus=context.cpus, mem=context.mem, disk=context.disk, gpus=context.gpus)

    with staticconf.testing.PatchConfiguration(
        {'autoscaling': {'default_signal_role': 'bar'}},
    ), mock.patch(
        'clusterman.autoscaler.autoscaler.get_monitoring_client',
    ), mock.patch(
        'clusterman.aws.util.SpotFleetResourceGroup.load',
        return_value=resource_groups,
    ), mock.patch(
        'clusterman.autoscaler.pool_manager.PoolManager',
        wraps=PoolManager,
    ), mock.patch(
        'clusterman.autoscaler.autoscaler.PoolManager.prune_excess_fulfilled_capacity',
    ), mock.patch(
        'clusterman.autoscaler.pool_manager.ClusterConnector.load',
    ) as mock_cluster_connector, mock.patch(
        'clusterman.autoscaler.autoscaler.PoolManager._calculate_non_orphan_fulfilled_capacity',
        return_value=context.target_capacity,
    ), mock.patch(
        'clusterman.signals.external_signal.ExternalSignal._connect_to_signal_process',
    ), mock.patch(
        'clusterman.signals.external_signal.get_metrics_for_signal',
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
        mock_cluster_connector.return_value.get_cluster_total_resources.return_value = resource_totals
        context.mock_cluster_connector = mock_cluster_connector
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


def make_mock_scaling_metrics(allocated_cpus, boost_factor):
    def mock_scaling_metrics(
        metric_name,
        metric_type,
        time_start,
        time_end,
        extra_dimensions,
        is_regex,
        app_identifier,
    ):
        if metric_name == 'cpus_allocated':
            return {'cpus_allocated': [
                (Decimal('100'), Decimal('10')),
                (Decimal('110'), Decimal('12')),
                (Decimal('130'), Decimal(allocated_cpus)),
            ]}
        elif metric_name == 'mem_allocated':
            return {'mem_allocated': [
                (Decimal('100'), Decimal('15')),
                (Decimal('110'), Decimal('17')),
                (Decimal('130'), Decimal('16')),
            ]}
        elif metric_name == 'disk_allocated':
            return {'disk_allocated': [
                (Decimal('100'), Decimal('10')),
                (Decimal('110'), Decimal('10')),
                (Decimal('130'), Decimal('10')),
            ]}
        elif metric_name == 'boost_factor|cluster=kube-test,pool=bar.kubernetes' and boost_factor:
            return {'boost_factor|cluster=kube-test,pool=bar.kubernetes': [
                (Decimal('135'), Decimal(boost_factor))
            ]}
        else:
            return {metric_name: []}

    return mock_scaling_metrics


@behave.given('a cluster with (?P<rgnum>\d+) resource groups')
def rgnum(context, rgnum):
    context.rgnum = int(rgnum)


@behave.given('(?P<target_capacity>\d+) target capacity')
def fulfilled_capacity(context, target_capacity):
    context.target_capacity = int(target_capacity)


@behave.given('(?P<cpus>\d+) CPUs, (?P<mem>\d+) MB mem, (?P<disk>\d+) MB disk, and (?P<gpus>\d+) GPUs')
def resources(context, cpus, mem, disk, gpus):
    context.cpus = int(cpus)
    context.mem = int(mem)
    context.disk = int(disk)
    context.gpus = int(gpus)


@behave.given('(?P<allocated_cpus>\d+) CPUs allocated and (?P<pending_cpus>\d+) CPUs pending')
def resources_requested(context, allocated_cpus, pending_cpus):
    context.allocated_cpus = float(allocated_cpus)
    context.pending_cpus = pending_cpus


@behave.given('a mesos autoscaler object')
def mesos_autoscaler(context):
    behave.use_fixture(autoscaler_patches, context)
    if hasattr(context, 'allocated_cpus'):
        context.autoscaler.metrics_client.get_metric_values.side_effect = make_mock_scaling_metrics(
            context.allocated_cpus,
            context.boost,
        )
    context.autoscaler = Autoscaler(
        cluster='mesos-test',
        pool='bar',
        apps=['bar'],
        scheduler='mesos',
        metrics_client=mock.Mock(),
        monitoring_enabled=False,
    )


@behave.given('a kubernetes autoscaler object')
def k8s_autoscaler(context):
    create_k8s_autoscaler(context)


@behave.given('a kubernetes autoscaler object with prevent_scale_down_after_capacity_loss enabled')
def k8s_autoscaler_prevent_scale_dpwn(context):
    create_k8s_autoscaler(context, prevent_scale_down_after_capacity_loss=True)


def create_k8s_autoscaler(context, prevent_scale_down_after_capacity_loss=False):
    behave.use_fixture(autoscaler_patches, context)
    context.mock_cluster_connector.return_value.__class__ = KubernetesClusterConnector
    context.mock_cluster_connector.return_value.get_cluster_allocated_resources.return_value = ClustermanResources(
        cpus=context.allocated_cpus,
    )
    context.mock_cluster_connector.return_value.get_unschedulable_pods.return_value = (
        [] if float(context.pending_cpus) == 0
        else [(
                V1Pod(
                    metadata=V1ObjectMeta(name='pod1'),
                    status=V1PodStatus(
                        phase='Pending',
                        conditions=[
                            V1PodCondition(status='False', type='PodScheduled', reason='Unschedulable')
                        ],
                    ),
                    spec=V1PodSpec(containers=[
                        V1Container(
                            name='container1',
                            resources=V1ResourceRequirements(requests={'cpu': context.pending_cpus})
                        ),
                    ]),
                ),
                PodUnschedulableReason.InsufficientResources,
            ),
            (
                V1Pod(
                    metadata=V1ObjectMeta(name='pod2'),
                    status=V1PodStatus(
                        phase='Pending',
                        conditions=[
                            V1PodCondition(status='False', type='PodScheduled', reason='Unschedulable')
                        ],
                    ),
                    spec=V1PodSpec(containers=[
                        V1Container(
                            name='container1',
                            resources=V1ResourceRequirements(requests={'cpu': context.pending_cpus})
                        ),
                    ]),
                ),
                PodUnschedulableReason.Unknown,
            ),
        ]
    )

    context.autoscaler = Autoscaler(
        cluster='kube-test',
        pool='bar',
        apps=['bar'],
        scheduler='kubernetes',
        metrics_client=mock.Mock(),
        monitoring_enabled=False,
    )

    if prevent_scale_down_after_capacity_loss:
        context.autoscaler.autoscaling_config = AutoscalingConfig(
            excluded_resources=[],
            setpoint=0.7,
            target_capacity_margin=0.1,
            prevent_scale_down_after_capacity_loss=True,
            instance_loss_threshold=0
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


@behave.when('the autoscaler runs')
def autoscaler_runs(context):
    run_autoscaler(context, once_only=False)

# usually we run the autoscaler twice to make sure nothing changes,
# but certain test cases require it to only run once


@behave.when('the autoscaler runs only once')
def autoscaler_runs_once(context):
    run_autoscaler(context, once_only=True)


def run_autoscaler(context, once_only=False):
    try:
        context.autoscaler.run()
    except Exception as e:
        context.exception = e
    else:
        try:
            del(context.exception)
        except AttributeError:
            pass

    if not once_only and not hasattr(context, 'exception'):
        # run it a second time to make sure nothing's changed
        context.autoscaler.run()


@behave.when('the cluster has recently lost capacity')
def lost_capacity(context):
    context.mock_cluster_connector.return_value.get_removed_nodes_before_last_reload.return_value = [
        KubernetesNode()
    ]


@behave.then('the autoscaler should scale rg(?P<rg>[12]) to (?P<target>\d+) capacity')
def rg_capacity_change(context, rg, target):
    groups = list(context.autoscaler.pool_manager.resource_groups.values())
    if int(target) != groups[int(rg) - 1].target_capacity:
        assert_that(
            groups[int(rg) - 1].modify_target_capacity.call_args_list,
            has_item(
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
