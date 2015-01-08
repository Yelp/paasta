import contextlib
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


def test_fetch_mesos_stats():
    with contextlib.nested(
        mock.patch('socket.getfqdn'),
        mock.patch('requests.get'),
    ) as (
        mock_getfqdn,
        mock_requests_get,
    ):
        mock_getfqdn.return_value = 'fake_fqdn'
        fake_json = """{"stat1": 0.1,"stat2": null}"""
        mock_requests_get.return_value = mock_response = mock.Mock()
        mock_response.text = fake_json
        expected = {'stat1': .1, 'stat2': None}
        actual = mesos_tools.fetch_mesos_stats()
        assert actual == expected
