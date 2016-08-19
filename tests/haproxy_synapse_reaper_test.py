import mock

from paasta_tools import haproxy_synapse_reaper


def test_parse_args():
    mock_argv = ['haproxy_synapse_reaper']
    with mock.patch('sys.argv', mock_argv):
        args = haproxy_synapse_reaper.parse_args()

    assert args.state_dir == '/var/run/synapse_alumni'
    assert args.reap_age == 3600
    assert args.username == 'nobody'


def test_parse_args_state_dir():
    mock_argv = ['haproxy_synapse_reaper', '--state-dir', 'foo']
    with mock.patch('sys.argv', mock_argv):
        args = haproxy_synapse_reaper.parse_args()

    assert args.state_dir == 'foo'


def test_parse_args_reap_age():
    mock_argv = ['haproxy_synapse_reaper', '--reap-age', '42']
    with mock.patch('sys.argv', mock_argv):
        args = haproxy_synapse_reaper.parse_args()

    assert args.reap_age == 42


def test_parse_args_username():
    mock_argv = ['haproxy_synapse_reaper', '--username', 'bar']
    with mock.patch('sys.argv', mock_argv):
        args = haproxy_synapse_reaper.parse_args()

    assert args.username == 'bar'


def create_mock_process(pid, name='haproxy-synapse', create_time=0):
    proc = mock.Mock(pid=pid)
    proc.name.return_value = name
    proc.username.return_value = 'nobody'
    proc.create_time.return_value = create_time
    return proc


@mock.patch('paasta_tools.haproxy_synapse_reaper.psutil.process_iter')
@mock.patch('paasta_tools.haproxy_synapse_reaper.get_main_pid')
def test_get_alumni(mock_get_main_pid, mock_process_iter):
    # main instance
    proc_0 = create_mock_process(pid=0)

    # Some alumni
    proc_1 = create_mock_process(pid=1)
    proc_2 = create_mock_process(pid=2)

    # Some other process that should not be killed
    proc_3 = create_mock_process(pid=5, name='some-other-proc')

    mock_process_iter.return_value = [proc_0, proc_1, proc_2, proc_3]
    mock_get_main_pid.return_value = 0

    expected = [proc_1, proc_2]
    actual = haproxy_synapse_reaper.get_alumni('nobody')

    assert expected == list(actual)


@mock.patch('paasta_tools.haproxy_synapse_reaper.time.time')
@mock.patch('paasta_tools.haproxy_synapse_reaper.os.path.getctime')
@mock.patch('paasta_tools.haproxy_synapse_reaper.os.path.exists')
@mock.patch('__builtin__.open')
def test_kill_alumni_if_too_old(mock_open, mock_exists, mock_getctime, mock_time):
    alumni = [
        # This process has no pidfile in the state_dir and exceeds the reap age
        # (specified via mock_getctime)
        create_mock_process(pid=42),

        # This process does have a pidfile in the state_dir and does not yet
        # exceeed the reap age
        create_mock_process(pid=43)
    ]

    mock_exists.side_effect = [False, True]
    mock_time.return_value = 3600
    mock_getctime.side_effect = [0, 1]

    reap_count = haproxy_synapse_reaper.kill_alumni(
        alumni=alumni, state_dir='/state/dir', reap_age=3600, max_procs=10)

    assert reap_count == 1
    assert alumni[0].kill.call_count == 1
    assert alumni[1].kill.call_count == 0
    mock_open.assert_called_once_with('/state/dir/42', 'w')


@mock.patch('paasta_tools.haproxy_synapse_reaper.time.time')
@mock.patch('paasta_tools.haproxy_synapse_reaper.os.path.getctime')
@mock.patch('paasta_tools.haproxy_synapse_reaper.os.path.exists')
@mock.patch('__builtin__.open')
def test_kill_alumni_if_too_many(mock_open, mock_exists, mock_getctime, mock_time):
    alumni = [
        create_mock_process(pid=42, create_time=124),
        create_mock_process(pid=43, create_time=123),
        create_mock_process(pid=44, create_time=125),
    ]

    mock_exists.return_value = True
    mock_time.return_value = 0
    mock_getctime.return_value = 0

    reap_count = haproxy_synapse_reaper.kill_alumni(
        alumni=alumni, state_dir='/state/dir', reap_age=3600, max_procs=2)

    assert reap_count == 1
    assert alumni[0].kill.call_count == 0
    assert alumni[1].kill.call_count == 1
    assert alumni[2].kill.call_count == 0


@mock.patch('paasta_tools.haproxy_synapse_reaper.os.listdir')
@mock.patch('paasta_tools.haproxy_synapse_reaper.os.remove')
def test_remove_stale_alumni_pidfiles(mock_remove, mock_listdir):
    alumni = [
        create_mock_process(pid=42),
        create_mock_process(pid=43)
    ]

    # The pidfile '41' has no associated alumnus so should be removed
    mock_listdir.return_value = ['41', '42']

    haproxy_synapse_reaper.remove_stale_alumni_pidfiles(alumni, '/state/dir')

    mock_listdir.assert_called_once_with('/state/dir')
    mock_remove.assert_called_once_with('/state/dir/41')
