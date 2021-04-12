from datetime import datetime
from unittest import mock
from unittest.mock import Mock

from paasta_tools.autoscaling import ec2_fitness
from paasta_tools.mesos_tools import SlaveTaskCount


def test_sort_by_total_tasks():
    mock_slave_1 = Mock(
        task_counts=SlaveTaskCount(count=3, slave=Mock(), batch_count=0)
    )
    mock_slave_2 = Mock(
        task_counts=SlaveTaskCount(count=2, slave=Mock(), batch_count=1)
    )
    mock_slave_3 = Mock(
        task_counts=SlaveTaskCount(count=5, slave=Mock(), batch_count=0)
    )
    ret = ec2_fitness.sort_by_total_tasks([mock_slave_1, mock_slave_2, mock_slave_3])
    assert ret == [mock_slave_3, mock_slave_1, mock_slave_2]


def test_sort_by_running_batch_count():
    mock_slave_1 = Mock(
        task_counts=SlaveTaskCount(count=3, slave=Mock(), batch_count=1)
    )
    mock_slave_2 = Mock(
        task_counts=SlaveTaskCount(count=2, slave=Mock(), batch_count=2)
    )
    mock_slave_3 = Mock(
        task_counts=SlaveTaskCount(count=5, slave=Mock(), batch_count=3)
    )
    ret = ec2_fitness.sort_by_running_batch_count(
        [mock_slave_1, mock_slave_2, mock_slave_3]
    )
    assert ret == [mock_slave_3, mock_slave_2, mock_slave_1]


def test_sort_by_health_system_instance_health_system_status_failed():
    mock_slave_1 = Mock(name="slave1")
    mock_slave_1.task_counts = SlaveTaskCount(count=3, slave=Mock(), batch_count=1)
    mock_slave_1.instance_status = {
        "Events": [
            {
                "Code": "instance-reboot",
                "Description": "string",
                "NotBefore": datetime(2015, 1, 1),
                "NotAfter": datetime(2015, 1, 1),
            }
        ],
        "SystemStatus": {"Status": "impaired"},
        "InstanceStatus": {"Status": "ok"},
    }
    mock_slave_2 = Mock(name="slave2")
    mock_slave_2.task_counts = (SlaveTaskCount(count=3, slave=Mock(), batch_count=1),)
    mock_slave_2.instance_status = {
        "Events": [
            {
                "Code": "instance-reboot",
                "Description": "string",
                "NotBefore": datetime(2015, 1, 1),
                "NotAfter": datetime(2015, 1, 1),
            }
        ],
        "SystemStatus": {"Status": "ok"},
        "InstanceStatus": {"Status": "ok"},
    }
    ret = ec2_fitness.sort_by_system_instance_health([mock_slave_1, mock_slave_2])
    assert ret == [mock_slave_2, mock_slave_1]


def test_sort_by_upcoming_events():
    mock_slave_1 = Mock()
    mock_slave_1.task_counts = SlaveTaskCount(count=3, slave=Mock(), batch_count=1)
    mock_slave_1.instance_status = {
        "Events": [],
        "SystemStatus": {"Status": "ok"},
        "InstanceStatus": {"Status": "ok"},
    }
    mock_slave_2 = Mock()
    mock_slave_2.task_counts = SlaveTaskCount(count=3, slave=Mock(), batch_count=1)
    mock_slave_2.instance_status = {
        "Events": [
            {
                "Code": "instance-reboot",
                "Description": "string",
                "NotBefore": datetime(2015, 1, 1),
                "NotAfter": datetime(2015, 1, 1),
            }
        ],
        "SystemStatus": {"Status": "ok"},
        "InstanceStatus": {"Status": "ok"},
    }
    ret = ec2_fitness.sort_by_upcoming_events([mock_slave_1, mock_slave_2])
    assert ret == [mock_slave_1, mock_slave_2]


def test_sort_by_fitness_calls_all_sorting_funcs():
    with mock.patch(
        "paasta_tools.autoscaling.ec2_fitness.sort_by_system_instance_health",
        autospec=True,
    ) as mock_sort_by_system_instance_health, mock.patch(
        "paasta_tools.autoscaling.ec2_fitness.sort_by_upcoming_events", autospec=True
    ) as mock_sort_by_upcoming_events, mock.patch(
        "paasta_tools.autoscaling.ec2_fitness.sort_by_running_batch_count",
        autospec=True,
    ) as mock_sort_by_running_batch_count, mock.patch(
        "paasta_tools.autoscaling.ec2_fitness.sort_by_total_tasks", autospec=True
    ) as mock_sort_by_total_tasks:
        instances = []
        ec2_fitness.sort_by_ec2_fitness(instances)
        assert mock_sort_by_total_tasks.called
        assert mock_sort_by_running_batch_count.called
        assert mock_sort_by_upcoming_events.called
        assert mock_sort_by_system_instance_health.called


def test_sort_by_fitness():
    mock_slave_1 = Mock(name="slave1")
    mock_slave_1.task_counts = SlaveTaskCount(count=3, slave=Mock(), batch_count=1)
    mock_slave_1.instance_status = {
        "Events": [],
        "SystemStatus": {"Status": "impaired"},
        "InstanceStatus": {"Status": "ok"},
    }
    mock_slave_2 = Mock(name="slave2")
    mock_slave_2.task_counts = SlaveTaskCount(count=3, slave=Mock(), batch_count=1)
    mock_slave_2.instance_status = {
        "Events": [
            {
                "Code": "instance-reboot",
                "Description": "foo",
                "NotBefore": datetime(2015, 1, 1),
                "NotAfter": datetime(2015, 1, 1),
            }
        ],
        "SystemStatus": {"Status": "ok"},
        "InstanceStatus": {"Status": "ok"},
    }
    mock_slave_3 = Mock(name="slave3")
    mock_slave_3.task_counts = SlaveTaskCount(count=2, slave=Mock(), batch_count=3)
    mock_slave_3.instance_status = {
        "Events": [],
        "SystemStatus": {"Status": "ok"},
        "InstanceStatus": {"Status": "ok"},
    }
    mock_slave_4 = Mock(name="slave4")
    mock_slave_4.task_counts = SlaveTaskCount(count=3, slave=Mock(), batch_count=1)
    mock_slave_4.instance_status = {
        "Events": [],
        "SystemStatus": {"Status": "ok"},
        "InstanceStatus": {"Status": "ok"},
    }
    mock_slave_5 = Mock(name="slave5")
    mock_slave_5.task_counts = SlaveTaskCount(count=1, slave=Mock(), batch_count=1)
    mock_slave_5.instance_status = {
        "Events": [],
        "SystemStatus": {"Status": "ok"},
        "InstanceStatus": {"Status": "ok"},
    }
    ret = ec2_fitness.sort_by_ec2_fitness(
        [mock_slave_1, mock_slave_2, mock_slave_3, mock_slave_4, mock_slave_5]
    )

    # we expect this order for the following reason:
    # mock_slave_1 is impaired and so should be killed asap
    # mock_slave_2 has an upcoming event
    # mock_slave_5 and mock_slave_4 have the fewest batch tasks, and so should be killed before
    # mock_slave_3 (we cant drain batch tasks, so try and save them)
    # mock_slave_5 has fewer tasks than mock_slave_4, and so is a better candidate for killing
    assert ret == [mock_slave_3, mock_slave_4, mock_slave_5, mock_slave_2, mock_slave_1]
