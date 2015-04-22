import contextlib
import mock
import paasta_tools.mesos_tools as mesos_tools

from mock import patch


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


def test_get_zookeeper_config():
    zk_hosts = '1.1.1.1:1111,2.2.2.2:2222,3.3.3.3:3333'
    zk_path = 'fake_path'
    fake_state = {'flags': {'zk': 'zk://%s/%s' % (zk_hosts, zk_path)}}
    expected = {'hosts': zk_hosts, 'path': zk_path}
    assert mesos_tools.get_zookeeper_config(fake_state) == expected


@patch('paasta_tools.mesos_tools.KazooClient')
def test_get_number_of_mesos_masters(
    mock_kazoo,
):
    fake_zk_config = {'hosts': '1.1.1.1', 'path': 'fake_path'}

    zk = mock_kazoo.return_value
    zk.get_children.return_value = ['log_11', 'state', 'info_1', 'info_2']
    assert mesos_tools.get_number_of_mesos_masters(fake_zk_config) == 2
