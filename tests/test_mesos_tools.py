import mesos.cli.master
import mock
import requests
from pytest import raises

import paasta_tools.mesos_tools as mesos_tools


def test_filter_running_tasks():
    tasks = [
        {'id': 1, 'state': 'TASK_RUNNING', 'framework': {'active': True}},
        {'id': 2, 'state': 'TASK_FAILED', 'framework': {'active': True}},
    ]
    running = mesos_tools.filter_running_tasks(tasks)
    assert len(running) == 1
    assert running[0]['id'] == 1


def test_filter_not_running_tasks():
    tasks = [
        {'id': 1, 'state': 'TASK_RUNNING'},
        {'id': 2, 'state': 'TASK_FAILED'},
    ]
    not_running = mesos_tools.filter_not_running_tasks(tasks)
    assert len(not_running) == 1
    assert not_running[0]['id'] == 2


def test_get_zookeeper_config():
    zk_hosts = '1.1.1.1:1111,2.2.2.2:2222,3.3.3.3:3333'
    zk_path = 'fake_path'
    fake_state = {'flags': {'zk': 'zk://%s/%s' % (zk_hosts, zk_path)}}
    expected = {'hosts': zk_hosts, 'path': zk_path}
    assert mesos_tools.get_zookeeper_config(fake_state) == expected


def test_get_mesos_leader():
    expected = 'mesos.master.yelpcorp.com'
    fake_master = 'false.authority.yelpcorp.com'
    with mock.patch('requests.get', autospec=True) as mock_requests_get:
        mock_requests_get.return_value = mock_response = mock.Mock()
        mock_response.return_code = 307
        mock_response.url = 'http://%s:999' % expected
        assert mesos_tools.get_mesos_leader(fake_master) == expected
        mock_requests_get.assert_called_once_with('http://%s:5050/redirect' % fake_master, timeout=10)


def test_get_mesos_leader_connection_error():
    fake_master = 'false.authority.yelpcorp.com'
    with mock.patch(
        'requests.get',
        autospec=True,
        side_effect=requests.exceptions.ConnectionError,
    ):
        with raises(mesos_tools.MesosMasterConnectionError):
            mesos_tools.get_mesos_leader(fake_master)


@mock.patch('paasta_tools.mesos_tools.get_mesos_leader')
def test_is_mesos_leader(mock_get_mesos_leader):
    fake_host = 'toast.host.roast'
    mock_get_mesos_leader.return_value = fake_host
    assert mesos_tools.is_mesos_leader(fake_host)
    mock_get_mesos_leader.assert_called_once_with(fake_host)


@mock.patch('paasta_tools.mesos_tools.KazooClient')
def test_get_number_of_mesos_masters(
    mock_kazoo,
):
    fake_zk_config = {'hosts': '1.1.1.1', 'path': 'fake_path'}

    zk = mock_kazoo.return_value
    zk.get_children.return_value = ['log_11', 'state', 'info_1', 'info_2']
    assert mesos_tools.get_number_of_mesos_masters(fake_zk_config) == 2


@mock.patch('requests.get')
@mock.patch('socket.getfqdn')
def test_fetch_local_slave_state_connection_error(
    mock_getfqdn,
    mock_requests_get,
):
    fake_request = requests.Request('GET', url='doesnt_matter')
    mock_getfqdn.return_value = 'fake_hostname'
    mock_requests_get.side_effect = requests.ConnectionError(
        'fake_message',
        request=fake_request,
    )

    with raises(mesos_tools.MesosSlaveConnectionError):
        mesos_tools.fetch_local_slave_state()


@mock.patch('paasta_tools.mesos_tools.fetch_mesos_state_from_leader', autospec=True)
def test_get_mesos_slaves_grouped_by_attribute(mock_fetch_state):
    fake_attribute = 'fake_attribute'
    fake_value_1 = 'fake_value_1'
    fake_value_2 = 'fake_value_2'
    mock_fetch_state.return_value = {
        'slaves': [
            {
                'hostname': 'fake_host_1',
                'attributes': {
                    'fake_attribute': fake_value_1,
                }
            },
            {
                'hostname': 'fake_host_2',
                'attributes': {
                    'fake_attribute': fake_value_2,
                }
            },
            {
                'hostname': 'fake_host_3',
                'attributes': {
                    'fake_attribute': fake_value_1,
                }
            },
            {
                'hostname': 'fake_host_4',
                'attributes': {
                    'fake_attribute': 'fake_other_value',
                }
            }
        ]
    }
    expected = {
        'fake_value_1': ['fake_host_1', 'fake_host_3'],
        'fake_value_2': ['fake_host_2'],
        'fake_other_value': ['fake_host_4'],
    }
    actual = mesos_tools.get_mesos_slaves_grouped_by_attribute(fake_attribute)
    assert actual == expected


@mock.patch('paasta_tools.mesos_tools.fetch_mesos_state_from_leader', autospec=True)
def test_get_mesos_slaves_grouped_by_attribute_bombs_out_with_no_slaves(mock_fetch_state):
    mock_fetch_state.return_value = {
        'slaves': []
    }
    with raises(mesos_tools.NoSlavesAvailable):
        mesos_tools.get_mesos_slaves_grouped_by_attribute('fake_attribute')


@mock.patch('paasta_tools.mesos_tools.slave_passes_blacklist', autospec=True)
def test_filter_mesos_slaves_by_blacklist_when_unfiltered(mock_slave_passes_blacklist):
    mock_slave_passes_blacklist.return_value = True
    slaves = [
        {
            'hostname': 'fake_host_1',
            'attributes': {
                'fake_attribute': 'fake_value_1',
            }
        },
        {
            'hostname': 'fake_host_2',
            'attributes': {
                'fake_attribute': 'fake_value_1',
            }
        }
    ]
    blacklist = []
    actual = mesos_tools.filter_mesos_slaves_by_blacklist(slaves=slaves, blacklist=blacklist)
    assert mock_slave_passes_blacklist.call_count == 2
    assert actual == slaves


@mock.patch('paasta_tools.mesos_tools.slave_passes_blacklist', autospec=True)
def test_filter_mesos_slaves_by_blacklist_when_filtered(mock_slave_passes_blacklist):
    mock_slave_passes_blacklist.return_value = False
    slaves = [
        {
            'hostname': 'fake_host_1',
            'attributes': {
                'fake_attribute': 'fake_value_1',
            }
        },
        {
            'hostname': 'fake_host_2',
            'attributes': {
                'fake_attribute': 'fake_value_1',
            }
        }
    ]
    blacklist = []
    actual = mesos_tools.filter_mesos_slaves_by_blacklist(slaves=slaves, blacklist=blacklist)
    assert mock_slave_passes_blacklist.call_count == 2
    assert actual == []


def slave_passes_blacklist_passes():
    slave = {
        'hostname': 'fake_host_3',
        'attributes': {
            'fake_attribute': 'fake_value_1',
        }
    }
    blacklist = [("fake_attribute", "No what we have here")]
    actual = mesos_tools.slave_passes_blacklist(slave=slave, blacklist=blacklist)
    assert actual is True


def slave_passes_blacklist_blocks_blacklisted_locations():
    slave = {
        'hostname': 'fake_host_3',
        'attributes': {
            'fake_attribute': 'fake_value_1',
        }
    }
    blacklist = [("fake_attribute", "fake_value_1")]
    actual = mesos_tools.slave_passes_blacklist(slave=slave, blacklist=blacklist)
    assert actual is False


def test_fetch_mesos_state_from_leader_works_on_elected_leader():
    # Elected leaders return 'elected_time' to indicate when
    # they were elected.
    good_fake_state = {
        "activated_slaves": 3,
        "cluster": "test",
        "completed_frameworks": [],
        "deactivated_slaves": 0,
        "elected_time": 1439503288.00787,
        "failed_tasks": 1,
    }
    mesos.cli.master.CURRENT.state = good_fake_state
    assert mesos_tools.fetch_mesos_state_from_leader() == good_fake_state


def test_fetch_mesos_state_from_leader_raises_on_non_elected_leader():
    # Non-elected leaders do not return 'elected_time' in their state
    # because they were not elected.
    un_elected_fake_state = {
        "activated_slaves": 3,
        "cluster": "test",
        "completed_frameworks": [],
        "deactivated_slaves": 0,
        "failed_tasks": 1,
    }
    mesos.cli.master.CURRENT.state = un_elected_fake_state
    with raises(mesos_tools.MasterNotAvailableException):
        assert mesos_tools.fetch_mesos_state_from_leader() == un_elected_fake_state
