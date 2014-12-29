from mock import patch
from socket import gaierror

from paasta_tools.paasta_cli import utils


@patch('paasta_tools.paasta_cli.utils.gethostbyname_ex')
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
    assert actual == set(ips)


@patch('paasta_tools.paasta_cli.utils.gethostbyname_ex')
def test_calculate_remote_masters_dns_lookup_fails(mock_gethostbyname_ex):
    cluster_name = 'fake_cluster_name'
    mock_gethostbyname_ex.side_effect = gaierror('fake omg ur dns does not exist')

    actual = utils.calculate_remote_masters(cluster_name)
    mock_gethostbyname_ex.assert_called_once_with("mesos-%s.yelpcorp.com" % cluster_name)
    assert actual == set([])


@patch('paasta_tools.paasta_cli.utils.calculate_remote_masters')
def test_execute_paasta_serviceinit_on_remote_master_happy_path(mock_calculate_remote_masters):
    cluster_name = 'fake_cluster_name'
    service_name = 'fake_service'
    instancename = 'fake_instance'

    mock_calculate_remote_masters.return_value=(
        'fake_master1',
        'fake_master2',
        'fake_master3',
    )

    actual = utils.execute_paasta_serviceinit_on_remote_master(cluster_name, service_name, instancename)
    mock_calculate_remote_masters.assert_called_once_with(cluster_name)
    assert actual is None
