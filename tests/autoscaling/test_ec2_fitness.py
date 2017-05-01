from __future__ import absolute_import
from __future__ import unicode_literals

from datetime import datetime

import mock
from mock import Mock

from paasta_tools.autoscaling import ec2_fitness
from paasta_tools.mesos_tools import SlaveTaskCount


def test_sort_by_total_tasks():
    mock_slave_1 = Mock(task_counts=SlaveTaskCount(count=3, slave=Mock(), chronos_count=0))
    mock_slave_2 = Mock(task_counts=SlaveTaskCount(count=2, slave=Mock(), chronos_count=1))
    mock_slave_3 = Mock(task_counts=SlaveTaskCount(count=5, slave=Mock(), chronos_count=0))
    ret = ec2_fitness.sort_by_total_tasks([mock_slave_1, mock_slave_2, mock_slave_3])
    assert ret == [mock_slave_2, mock_slave_1, mock_slave_3]


def test_sort_by_running_batch_count():
    mock_slave_1 = Mock(task_counts=SlaveTaskCount(count=3, slave=Mock(), chronos_count=1))
    mock_slave_2 = Mock(task_counts=SlaveTaskCount(count=2, slave=Mock(), chronos_count=2))
    mock_slave_3 = Mock(task_counts=SlaveTaskCount(count=5, slave=Mock(), chronos_count=3))
    ret = ec2_fitness.sort_by_running_batch_count([mock_slave_1, mock_slave_2, mock_slave_3])
    assert ret == [mock_slave_1, mock_slave_2, mock_slave_3]


def test_sort_by_health_system_instance_health_system_status_failed():
    mock_slave_1 = Mock(
        task_counts=SlaveTaskCount(
            count=3,
            slave=Mock(),
            chronos_count=1,
        ),
        instance_status={
            'Events': [
                {
                    'Code': 'instance-reboot',
                    'Description': 'string',
                    'NotBefore': datetime(2015, 1, 1),
                    'NotAfter': datetime(2015, 1, 1)
                },
            ],
            'SystemStatus': {
                'Status': 'impaired',
            },
            'InstanceStatus': {
                'Status': 'ok',
            }
        },
    )
    mock_slave_2 = Mock(
        task_counts=SlaveTaskCount(
            count=3,
            slave=Mock(),
            chronos_count=1,
        ),
        instance_status={
            'Events': [
                {
                    'Code': 'instance-reboot',
                    'Description': 'string',
                    'NotBefore': datetime(2015, 1, 1),
                    'NotAfter': datetime(2015, 1, 1)
                },
            ],
            'SystemStatus': {
                'Status': 'ok',
            },
            'InstanceStatus': {
                'Status': 'ok',
            }
        },
    )
    mock_slave_2 = Mock(task_counts=SlaveTaskCount(count=2, slave=Mock(), chronos_count=2))
    ret = ec2_fitness.sort_by_running_batch_count([mock_slave_1, mock_slave_2])
    assert ret == [mock_slave_1, mock_slave_2]


def test_sort_by_upcoming_events():
    mock_slave_1 = Mock(
        task_counts=SlaveTaskCount(
            count=3,
            slave=Mock(),
            chronos_count=1,
        ),
        instance_status={
            'Events': [],
            'SystemStatus': {
                'Status': 'ok',
            },
            'InstanceStatus': {
                'Status': 'ok',
            }
        },
    )
    mock_slave_2 = Mock(
        task_counts=SlaveTaskCount(
            count=3,
            slave=Mock(),
            chronos_count=1,
        ),
        instance_status={
            'Events': [
                {
                    'Code': 'instance-reboot',
                    'Description': 'string',
                    'NotBefore': datetime(2015, 1, 1),
                    'NotAfter': datetime(2015, 1, 1)
                },
            ],
            'SystemStatus': {
                'Status': 'ok',
            },
            'InstanceStatus': {
                'Status': 'ok',
            }
        },
    )
    ret = ec2_fitness.sort_by_upcoming_events([mock_slave_1, mock_slave_2])
    assert ret == [mock_slave_1, mock_slave_2]


def test_sort_by_fitness_calls_all_sorting_funcs():
    with mock.patch(
        'paasta_tools.autoscaling.ec2_fitness.sort_by_system_instance_health',
        autospec=True,
    ) as mock_sort_by_system_instance_health, mock.patch(
        'paasta_tools.autoscaling.ec2_fitness.sort_by_upcoming_events',
        autospec=True,
    ) as mock_sort_by_upcoming_events, mock.patch(
        'paasta_tools.autoscaling.ec2_fitness.sort_by_running_batch_count',
        autospec=True,
    ) as mock_sort_by_running_batch_count, mock.patch(
        'paasta_tools.autoscaling.ec2_fitness.sort_by_total_tasks',
        autospec=True
    ) as mock_sort_by_total_tasks:
        instances = []
        ec2_fitness.sort_by_ec2_fitness(instances)
        assert mock_sort_by_total_tasks.called
        assert mock_sort_by_running_batch_count.called
        assert mock_sort_by_upcoming_events.called
        assert mock_sort_by_system_instance_health.called


def test_sort_by_fitness():
    mock_slave_1 = Mock(
        task_counts=SlaveTaskCount(
            count=3,
            slave=Mock(),
            chronos_count=1,
        ),
        instance_status={
            'Events': [],
            'SystemStatus': {'Status': 'impaired', },
            'InstanceStatus': {'Status': 'ok', }
        },
    )
    mock_slave_2 = Mock(
        task_counts=SlaveTaskCount(
            count=3,
            slave=Mock(),
            chronos_count=1,
        ),
        instance_status={
            'Events': [
                {
                    'Code': 'instance-reboot',
                    'Description': 'foo',
                    'NotBefore': datetime(2015, 1, 1),
                    'NotAfter': datetime(2015, 1, 1)
                },
            ],
            'SystemStatus': {'Status': 'ok', },
            'InstanceStatus': {'Status': 'ok', }
        },
    )
    mock_slave_3 = Mock(
        task_counts=SlaveTaskCount(
            count=2,
            slave=Mock(),
            chronos_count=3,
        ),
        instance_status={
            'Events': [],
            'SystemStatus': {'Status': 'ok', },
            'InstanceStatus': {'Status': 'ok', }
        },
    )
    mock_slave_4 = Mock(
        task_counts=SlaveTaskCount(
            count=3,
            slave=Mock(),
            chronos_count=1,
        ),
        instance_status={
            'Events': [],
            'SystemStatus': {'Status': 'ok', },
            'InstanceStatus': {'Status': 'ok', }
        },
    )
    mock_slave_5 = Mock(
        task_counts=SlaveTaskCount(
            count=1,
            slave=Mock(),
            chronos_count=1,
        ),
        instance_status={
            'Events': [],
            'SystemStatus': {'Status': 'ok', },
            'InstanceStatus': {'Status': 'ok', }
        },
    )
    ret = ec2_fitness.sort_by_ec2_fitness([mock_slave_1, mock_slave_2, mock_slave_3, mock_slave_4, mock_slave_5])
    assert ret == [mock_slave_5, mock_slave_4, mock_slave_3, mock_slave_2, mock_slave_1]
