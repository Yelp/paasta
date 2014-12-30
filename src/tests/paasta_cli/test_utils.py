from mock import patch
from socket import error
from socket import gaierror

from paasta_tools.paasta_cli import utils


@patch('paasta_tools.paasta_cli.utils.gethostbyname_ex', autospec=True)
def test_calculate_remote_masters_happy_path(mock_gethostbyname_ex):
    cluster_name = 'fake_cluster_name'
    ips = [
        '192.0.2.1',
        '192.0.2.2',
        '192.0.2.3',
    ]
    mock_gethostbyname_ex.return_value = (
        'unused',
        'unused',
        ips,
    )

    actual = utils.calculate_remote_masters(cluster_name)
    mock_gethostbyname_ex.assert_called_once_with("mesos-%s.yelpcorp.com" % cluster_name)
    assert actual == ips


@patch('paasta_tools.paasta_cli.utils.gethostbyname_ex', autospec=True)
def test_calculate_remote_masters_dns_lookup_fails(mock_gethostbyname_ex):
    cluster_name = 'fake_cluster_name'
    mock_gethostbyname_ex.side_effect = gaierror('fake omg ur dns does not exist')

    actual = utils.calculate_remote_masters(cluster_name)
    mock_gethostbyname_ex.assert_called_once_with("mesos-%s.yelpcorp.com" % cluster_name)
    assert actual == []


@patch('paasta_tools.paasta_cli.utils.create_connection', autospec=True)
def test_find_connectable_master_happy_path(mock_create_connection):
    masters = [
        '192.0.2.1',
        '192.0.2.2',
        '192.0.2.3',
    ]
    port = 22
    timeout = 1.0
    mock_create_connection.return_value = True

    actual = utils.find_connectable_master(masters)
    assert mock_create_connection.call_count == 1
    mock_create_connection.assert_called_once_with((masters[0], port), timeout)
    assert actual == masters[0]


@patch('paasta_tools.paasta_cli.utils.create_connection', autospec=True)
def test_find_connectable_master_one_failure(mock_create_connection):
    masters = [
        '192.0.2.1',
        '192.0.2.2',
        '192.0.2.3',
    ]
    port = 22
    timeout = 1.0
    # iter() is a workaround
    # (http://lists.idyll.org/pipermail/testing-in-python/2013-April/005527.html)
    # for a bug in mock (http://bugs.python.org/issue17826)
    create_connection_side_effects = iter([
        error('fake socket.error'),
        'unused',
        'unused',
    ])
    mock_create_connection.side_effect = create_connection_side_effects
    mock_create_connection.return_value = True

    actual = utils.find_connectable_master(masters)
    assert mock_create_connection.call_count == 2
    mock_create_connection.assert_any_call((masters[0], port), timeout)
    mock_create_connection.assert_any_call((masters[1], port), timeout)
    assert actual == '192.0.2.2'


@patch('paasta_tools.paasta_cli.utils.create_connection', autospec=True)
def test_find_connectable_master_all_failures(mock_create_connection):
    masters = [
        '192.0.2.1',
        '192.0.2.2',
        '192.0.2.3',
    ]
    port = 22
    timeout = 1.0
    create_connection_side_effects = error('fake socket.error')
    mock_create_connection.side_effect = create_connection_side_effects

    actual = utils.find_connectable_master(masters)
    assert mock_create_connection.call_count == 3
    mock_create_connection.assert_any_call((masters[0], port), timeout)
    mock_create_connection.assert_any_call((masters[1], port), timeout)
    mock_create_connection.assert_any_call((masters[2], port), timeout)
    assert actual is None


@patch('paasta_tools.paasta_cli.utils._run', autospec=True)
def test_check_ssh_and_sudo_on_master_check_successful(mock_run):
    master = 'fake_master'
    mock_run.return_value = (0, 'fake_output')
    expected_command = 'ssh -A -n %s sudo paasta_serviceinit -h' % master

    actual = utils.check_ssh_and_sudo_on_master(master)
    mock_run.assert_called_once_with(expected_command)
    assert actual is True


@patch('paasta_tools.paasta_cli.utils._run', autospec=True)
def test_check_ssh_and_sudo_on_master_check_ssh_failure(mock_run):
    master = 'fake_master'
    mock_run.return_value = (255, 'fake_output')

    actual = utils.check_ssh_and_sudo_on_master(master)
    assert actual is False


@patch('paasta_tools.paasta_cli.utils._run', autospec=True)
def test_check_ssh_and_sudo_on_master_check_sudo_failure(mock_run):
    master = 'fake_master'
    mock_run.return_value = (1, 'fake_output')

    actual = utils.check_ssh_and_sudo_on_master(master)
    assert actual is False


@patch('paasta_tools.paasta_cli.utils._run', autospec=True)
def test_run_paasta_serviceinit_status(mock_run):
    mock_run.return_value = ('unused', 'fake_output')
    expected_command = 'ssh -A -n fake_master sudo paasta_serviceinit fake_service_name.fake_instancename status'

    actual = utils.run_paasta_serviceinit_status('fake_master', 'fake_service_name', 'fake_instancename')
    mock_run.assert_called_once_with(expected_command)
    assert actual == mock_run.return_value[1]


@patch('paasta_tools.paasta_cli.utils.calculate_remote_masters', autospec=True)
@patch('paasta_tools.paasta_cli.utils.find_connectable_master', autospec=True)
@patch('paasta_tools.paasta_cli.utils.check_ssh_and_sudo_on_master', autospec=True)
@patch('paasta_tools.paasta_cli.utils.run_paasta_serviceinit_status', autospec=True)
def test_execute_paasta_serviceinit_on_remote_master_happy_path(
    mock_run_paasta_serviceinit_status,
    mock_check_ssh_and_sudo_on_master,
    mock_find_connectable_master,
    mock_calculate_remote_masters,
):
    cluster_name = 'fake_cluster_name'
    service_name = 'fake_service'
    instancename = 'fake_instance'
    remote_masters = (
        'fake_master1',
        'fake_master2',
        'fake_master3',
    )
    mock_calculate_remote_masters.return_value = remote_masters
    mock_find_connectable_master.return_value = 'fake_connectable_master'

    actual = utils.execute_paasta_serviceinit_on_remote_master(cluster_name, service_name, instancename)
    mock_calculate_remote_masters.assert_called_once_with(cluster_name)
    mock_find_connectable_master.assert_called_once_with(remote_masters)
    mock_check_ssh_and_sudo_on_master.assert_called_once_with('fake_connectable_master')
    mock_run_paasta_serviceinit_status.assert_called_once_with('fake_connectable_master', service_name, instancename)
    assert actual == mock_run_paasta_serviceinit_status.return_value


@patch('paasta_tools.paasta_cli.utils.calculate_remote_masters', autospec=True)
@patch('paasta_tools.paasta_cli.utils.find_connectable_master', autospec=True)
@patch('paasta_tools.paasta_cli.utils.check_ssh_and_sudo_on_master', autospec=True)
@patch('paasta_tools.paasta_cli.utils.run_paasta_serviceinit_status', autospec=True)
def test_execute_paasta_serviceinit_on_remote_no_connectable_master(
    mock_run_paasta_serviceinit_status,
    mock_check_ssh_and_sudo_on_master,
    mock_find_connectable_master,
    mock_calculate_remote_masters,
):
    cluster_name = 'fake_cluster_name'
    service_name = 'fake_service'
    instancename = 'fake_instance'
    mock_find_connectable_master.return_value = None

    actual = utils.execute_paasta_serviceinit_on_remote_master(cluster_name, service_name, instancename)
    assert mock_check_ssh_and_sudo_on_master.call_count == 0
    assert actual == 'ERROR could not find connectable master in cluster %s' % cluster_name


@patch('paasta_tools.paasta_cli.utils.calculate_remote_masters', autospec=True)
@patch('paasta_tools.paasta_cli.utils.find_connectable_master', autospec=True)
@patch('paasta_tools.paasta_cli.utils.check_ssh_and_sudo_on_master', autospec=True)
@patch('paasta_tools.paasta_cli.utils.run_paasta_serviceinit_status', autospec=True)
def test_execute_paasta_serviceinit_on_remote_check_ssh_and_sudo_failed(
    mock_run_paasta_serviceinit_status,
    mock_check_ssh_and_sudo_on_master,
    mock_find_connectable_master,
    mock_calculate_remote_masters,
):
    cluster_name = 'fake_cluster_name'
    service_name = 'fake_service'
    instancename = 'fake_instance'
    mock_check_ssh_and_sudo_on_master.return_value = False

    actual = utils.execute_paasta_serviceinit_on_remote_master(cluster_name, service_name, instancename)
    assert mock_run_paasta_serviceinit_status.call_count == 0
    assert actual == 'ERROR ssh or sudo check failed for master %s' % mock_find_connectable_master.return_value
