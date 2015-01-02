import mock
import paasta_tools.mesos_tools as mesos_tools


def test_get_running_mesos_tasks_for_service():
    mock_tasks = [
        {'id': 1, 'state': 'TASK_RUNNING'},
        {'id': 2, 'state': 'TASK_RUNNING'},
        {'id': 3, 'state': 'TASK_FAILED'},
        {'id': 4, 'state': 'TASK_FAILED'},
    ]
    expected = [
        {'id': 1, 'state': 'TASK_RUNNING'},
        {'id': 2, 'state': 'TASK_RUNNING'},
    ]
    with mock.patch('paasta_tools.mesos_tools.get_mesos_tasks_for_service', autospec=True) as mesos_tasks_patch:
        mesos_tasks_patch.return_value = mock_tasks
        actual = mesos_tools.get_running_mesos_tasks_for_service('unused', 'unused')
        assert actual == expected
