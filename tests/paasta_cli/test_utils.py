import mock
from mock import patch
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
    assert actual == (ips, None)


@patch('paasta_tools.paasta_cli.utils.gethostbyname_ex', autospec=True)
def test_calculate_remote_masters_dns_lookup_fails(mock_gethostbyname_ex):
    cluster_name = 'fake_cluster_name'
    err_msg = 'fake omg ur dns does not exist'
    mock_gethostbyname_ex.side_effect = gaierror('EAI_NONE', err_msg)

    actual = utils.calculate_remote_masters(cluster_name)
    mock_gethostbyname_ex.assert_called_once_with("mesos-%s.yelpcorp.com" % cluster_name)
    assert actual[0] == []
    assert err_msg in actual[1]


@patch('paasta_tools.paasta_cli.utils.check_ssh_and_sudo_on_master', autospec=True)
def test_find_connectable_master_happy_path(mock_check_ssh_and_sudo_on_master):
    masters = [
        '192.0.2.1',
        '192.0.2.2',
        '192.0.2.3',
    ]
    timeout = 3.0
    mock_check_ssh_and_sudo_on_master.return_value = (True, None)

    actual = utils.find_connectable_master(masters)
    expected = (masters[0], None)
    assert mock_check_ssh_and_sudo_on_master.call_count == 1
    mock_check_ssh_and_sudo_on_master.assert_called_once_with(masters[0], timeout=timeout)
    assert actual == expected


@patch('paasta_tools.paasta_cli.utils.check_ssh_and_sudo_on_master', autospec=True)
def test_find_connectable_master_one_failure(mock_check_ssh_and_sudo_on_master):
    masters = [
        '192.0.2.1',
        '192.0.2.2',
        '192.0.2.3',
    ]
    timeout = 3.0
    # iter() is a workaround
    # (http://lists.idyll.org/pipermail/testing-in-python/2013-April/005527.html)
    # for a bug in mock (http://bugs.python.org/issue17826)
    create_connection_side_effects = iter([
        (False, "something bad"),
        (True, 'unused'),
        (True, 'unused'),
    ])
    mock_check_ssh_and_sudo_on_master.side_effect = create_connection_side_effects
    mock_check_ssh_and_sudo_on_master.return_value = True

    actual = utils.find_connectable_master(masters)
    assert mock_check_ssh_and_sudo_on_master.call_count == 2
    mock_check_ssh_and_sudo_on_master.assert_any_call(masters[0], timeout=timeout)
    mock_check_ssh_and_sudo_on_master.assert_any_call(masters[1], timeout=timeout)
    assert actual == ('192.0.2.2', None)


@patch('paasta_tools.paasta_cli.utils.check_ssh_and_sudo_on_master', autospec=True)
def test_find_connectable_master_all_failures(mock_check_ssh_and_sudo_on_master):
    masters = [
        '192.0.2.1',
        '192.0.2.2',
        '192.0.2.3',
    ]
    timeout = 3.0
    mock_check_ssh_and_sudo_on_master.return_value = (255, "timeout")

    actual = utils.find_connectable_master(masters)
    assert mock_check_ssh_and_sudo_on_master.call_count == 3
    mock_check_ssh_and_sudo_on_master.assert_any_call((masters[0]), timeout=timeout)
    mock_check_ssh_and_sudo_on_master.assert_any_call((masters[1]), timeout=timeout)
    mock_check_ssh_and_sudo_on_master.assert_any_call((masters[2]), timeout=timeout)
    assert actual[0] is None
    assert 'timeout' in actual[1]


@patch('paasta_tools.paasta_cli.utils._run', autospec=True)
def test_check_ssh_and_sudo_on_master_check_successful(mock_run):
    master = 'fake_master'
    mock_run.return_value = (0, 'fake_output')
    expected_command = 'ssh -A -n %s sudo paasta_serviceinit -h' % master

    actual = utils.check_ssh_and_sudo_on_master(master)
    mock_run.assert_called_once_with(expected_command, timeout=mock.ANY)
    assert actual == (True, None)


@patch('paasta_tools.paasta_cli.utils._run', autospec=True)
def test_check_ssh_and_sudo_on_master_check_ssh_failure(mock_run):
    master = 'fake_master'
    mock_run.return_value = (255, 'fake_output')

    actual = utils.check_ssh_and_sudo_on_master(master)
    assert actual[0] is False
    assert 'fake_output' in actual[1]
    assert '255' in actual[1]


@patch('paasta_tools.paasta_cli.utils._run', autospec=True)
def test_check_ssh_and_sudo_on_master_check_sudo_failure(mock_run):
    master = 'fake_master'
    mock_run.return_value = (1, 'fake_output')

    actual = utils.check_ssh_and_sudo_on_master(master)
    assert actual[0] is False
    assert '1' in actual[1]
    assert 'fake_output' in actual[1]


@patch('paasta_tools.paasta_cli.utils._run', autospec=True)
def test_run_paasta_serviceinit_status(mock_run):
    mock_run.return_value = ('unused', 'fake_output')
    expected_command = 'ssh -A -n fake_master sudo paasta_serviceinit fake_service_name.fake_instancename status'

    actual = utils.run_paasta_serviceinit(
        'status',
        'fake_master',
        'fake_service_name',
        'fake_instancename',
        'fake_cluster',
    )
    mock_run.assert_called_once_with(expected_command, timeout=mock.ANY)
    assert actual == mock_run.return_value[1]


@patch('paasta_tools.paasta_cli.utils._run', autospec=True)
def test_run_paasta_serviceinit_status_verbose(mock_run):
    mock_run.return_value = ('unused', 'fake_output')
    expected_command = 'ssh -A -n fake_master sudo paasta_serviceinit -v fake_service_name.fake_instancename status'

    actual = utils.run_paasta_serviceinit(
        'status',
        'fake_master',
        'fake_service_name',
        'fake_instancename',
        'fake_cluster',
        True,
    )
    mock_run.assert_called_once_with(expected_command, timeout=mock.ANY)
    assert actual == mock_run.return_value[1]


@patch('paasta_tools.paasta_cli.utils.calculate_remote_masters', autospec=True)
@patch('paasta_tools.paasta_cli.utils.find_connectable_master', autospec=True)
@patch('paasta_tools.paasta_cli.utils.run_paasta_serviceinit', autospec=True)
def test_execute_paasta_serviceinit_status_on_remote_master_happy_path(
    mock_run_paasta_serviceinit,
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
    mock_calculate_remote_masters.return_value = (remote_masters, None)
    mock_find_connectable_master.return_value = ('fake_connectable_master', None)

    actual = utils.execute_paasta_serviceinit_on_remote_master('status', cluster_name, service_name, instancename)
    mock_calculate_remote_masters.assert_called_once_with(cluster_name)
    mock_find_connectable_master.assert_called_once_with(remote_masters)
    mock_run_paasta_serviceinit.assert_called_once_with(
        'status',
        'fake_connectable_master',
        service_name,
        instancename,
        cluster_name,
        False,
    )
    assert actual == mock_run_paasta_serviceinit.return_value


@patch('paasta_tools.paasta_cli.utils.calculate_remote_masters', autospec=True)
@patch('paasta_tools.paasta_cli.utils.find_connectable_master', autospec=True)
@patch('paasta_tools.paasta_cli.utils.check_ssh_and_sudo_on_master', autospec=True)
@patch('paasta_tools.paasta_cli.utils.run_paasta_serviceinit', autospec=True)
def test_execute_paasta_serviceinit_on_remote_no_connectable_master(
    mock_run_paasta_serviceinit,
    mock_check_ssh_and_sudo_on_master,
    mock_find_connectable_master,
    mock_calculate_remote_masters,
):
    cluster_name = 'fake_cluster_name'
    service_name = 'fake_service'
    instancename = 'fake_instance'
    mock_find_connectable_master.return_value = (None, "fake_err_msg")
    mock_calculate_remote_masters.return_value = (['fake_master'], None)

    actual = utils.execute_paasta_serviceinit_on_remote_master('status', cluster_name, service_name, instancename)
    assert mock_check_ssh_and_sudo_on_master.call_count == 0
    assert 'ERROR: could not find connectable master in cluster %s' % cluster_name in actual
    assert "fake_err_msg" in actual


@patch('paasta_tools.paasta_cli.utils.list_instances_for_service')
@patch('paasta_tools.paasta_cli.utils.list_services')
def test_list_service_instances(
    mock_list_services,
    mock_list_instances,
):
    mock_list_services.return_value = ['fake_service']
    mock_list_instances.return_value = ['canary', 'main']
    expected = ['fake_service.canary', 'fake_service.main']
    actual = utils.list_service_instances()
    assert actual == expected


@patch('paasta_tools.paasta_cli.utils.list_instances_for_service')
@patch('paasta_tools.paasta_cli.utils.list_services')
def test_list_paasta_services(
    mock_list_services,
    mock_list_instances,
):
    mock_list_services.return_value = ['fake_service']
    mock_list_instances.return_value = ['canary', 'main']
    expected = ['fake_service']
    actual = utils.list_paasta_services()
    assert actual == expected


@patch('paasta_tools.paasta_cli.utils.guess_service_name')
@patch('paasta_tools.paasta_cli.utils.validate_service_name')
@patch('paasta_tools.paasta_cli.utils.list_instances_for_service')
def test_list_instances_with_autodetect(
    mock_list_instance_for_service,
    mock_validate_service_name,
    mock_guess_service_name,
):
    expected = ['instance1', 'instance2', 'instance3']
    mock_guess_service_name.return_value = 'fake_service'
    mock_validate_service_name.return_value = None
    mock_list_instance_for_service.return_value = expected
    actual = utils.list_instances()
    assert actual == expected
    mock_validate_service_name.assert_called_once_with('fake_service')
    mock_list_instance_for_service.assert_called_once_with('fake_service')


@patch('paasta_tools.paasta_cli.utils.guess_service_name')
@patch('paasta_tools.paasta_cli.utils.validate_service_name')
@patch('paasta_tools.paasta_cli.utils.list_instances_for_service')
@patch('paasta_tools.paasta_cli.utils.list_services')
def test_list_instances_no_service(
    mock_list_services,
    mock_list_instance_for_service,
    mock_validate_service_name,
    mock_guess_service_name,
):
    expected = ['instance1', 'instance2', 'instance3']
    mock_guess_service_name.return_value = 'unused'
    mock_list_services.return_value = ['fake_service1']
    mock_validate_service_name.side_effect = utils.NoSuchService(None)
    mock_list_instance_for_service.return_value = expected
    actual = utils.list_instances()
    mock_validate_service_name.assert_called_once_with('unused')
    mock_list_instance_for_service.assert_called_once_with('fake_service1')
    assert actual == expected


def test_lazy_choices_completer():
    completer = utils.lazy_choices_completer(lambda: ['1', '2', '3'])
    assert completer(prefix='') == ['1', '2', '3']
