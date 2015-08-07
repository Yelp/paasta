import contextlib
import mock

from paasta_tools.marathon_tools import ServiceNamespaceConfig
from paasta_tools.paasta_cli.utils import PaastaColors
from paasta_tools.paasta_cli.cmds import info
from paasta_tools.utils import NoDeploymentsAvailable


def test_get_service_info():
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_cli.cmds.info.get_team', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.info.get_runbook', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.info.read_service_configuration', autospec=True),
        mock.patch('service_configuration_lib.read_service_configuration', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.info.get_actual_deployments', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.info.get_smartstack_endpoints', autospec=True),
    ) as (
        mock_get_team,
        mock_get_runbook,
        mock_read_service_configuration,
        mock_scl_read_service_configuration,
        mock_get_actual_deployments,
        mock_get_smartstack_endpoints,
    ):
        mock_get_team.return_value = 'fake_team'
        mock_get_runbook.return_value = 'fake_runbook'
        mock_read_service_configuration.return_value = {
            'description': 'a fake service that does stuff',
            'external_link': 'http://bla',
            'smartstack': {
                'main': {
                    'proxy_port': 9001
                }
            }
        }
        mock_scl_read_service_configuration.return_value = {
            'description': 'a fake service that does stuff',
            'external_link': 'http://bla',
            'smartstack': {
                'main': {
                    'proxy_port': 9001
                }
            }
        }
        mock_get_actual_deployments.return_value = ['clusterA.main', 'clusterB.main']
        mock_get_smartstack_endpoints.return_value = ['http://foo:1234', 'tcp://bar:1234']
        actual = info.get_service_info('fake_service')
        assert 'Service Name: fake_service' in actual
        assert 'Monitored By: team fake_team' in actual
        assert 'Runbook: ' in actual
        assert 'fake_runbook' in actual
        assert 'Description: a fake service' in actual
        assert 'http://bla' in actual
        assert 'Git Repo: git@git.yelpcorp.com:services/fake_service' in actual
        assert 'Jenkins Pipeline: ' in actual
        assert 'Deployed to the following' in actual
        assert 'clusterA (%s)' % PaastaColors.cyan('http://fake_service.paasta-clusterA.yelp/') in actual
        assert 'clusterB (%s)' % PaastaColors.cyan('http://fake_service.paasta-clusterB.yelp/') in actual
        assert 'Smartstack endpoint' in actual
        assert 'http://foo:1234' in actual
        assert 'tcp://bar:1234' in actual
        assert 'Dashboard' in actual
        assert '%s (Sensu Alerts)' % PaastaColors.cyan('https://uchiwa.yelpcorp.com/#/events?q=fake_service') in actual


def test_deployments_to_clusters():
    deployments = ['A.main', 'A.canary', 'B.main', 'C.othermain']
    expected = set(['A', 'B', 'C'])
    actual = info.deployments_to_clusters(deployments)
    assert actual == expected


def test_get_smartstack_endpoints_http():
    with mock.patch(
        'service_configuration_lib.read_service_configuration', autospec=True
    ) as mock_read_service_configuration:
        mock_read_service_configuration.return_value = {
            'smartstack': {
                'main': {
                    'proxy_port': 1234
                }
            }
        }
        expected = ["http://169.254.255.254:1234 (main)"]
        actual = info.get_smartstack_endpoints('unused')
        assert actual == expected


def test_get_smartstack_endpoints_tcp():
    with mock.patch(
        'service_configuration_lib.read_service_configuration', autospec=True
    ) as mock_read_service_configuration:
        mock_read_service_configuration.return_value = {
            'smartstack': {
                'tcpone': {
                    'proxy_port': 1234,
                    'mode': 'tcp',
                }
            }
        }
        expected = ["tcp://169.254.255.254:1234 (tcpone)"]
        actual = info.get_smartstack_endpoints('unused')
        assert actual == expected


def test_get_deployments_strings_default_case_with_smartstack():
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_cli.cmds.info.get_actual_deployments', autospec=True),
        mock.patch('service_configuration_lib.read_service_configuration', autospec=True),
    ) as (
        mock_get_actual_deployments,
        mock_read_service_configuration,
    ):
        mock_get_actual_deployments.return_value = ['clusterA.main', 'clusterB.main']
        mock_read_service_configuration.return_value = {
            'smartstack': {
                'main': {
                    'proxy_port': 9001
                }
            }
        }
        actual = info.get_deployments_strings('fake_service')
        assert ' - clusterA (%s)' % PaastaColors.cyan('http://fake_service.paasta-clusterA.yelp/') in actual
        assert ' - clusterB (%s)' % PaastaColors.cyan('http://fake_service.paasta-clusterB.yelp/') in actual


def test_get_deployments_strings_protocol_tcp_case():
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_cli.cmds.info.get_actual_deployments', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.info.load_service_namespace_config', autospec=True),
    ) as (mock_get_actual_deployments, mock_load_service_namespace_config):
        mock_get_actual_deployments.return_value = ['clusterA.main', 'clusterB.main']
        mock_load_service_namespace_config.return_value = ServiceNamespaceConfig({'mode': 'tcp', 'proxy_port': 8080})
        actual = info.get_deployments_strings('unused')
        assert ' - clusterA (%s)' % PaastaColors.cyan('tcp://paasta-clusterA.yelp:8080/') in actual
        assert ' - clusterB (%s)' % PaastaColors.cyan('tcp://paasta-clusterB.yelp:8080/') in actual


def test_get_deployments_strings_non_listening_service():
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_cli.cmds.info.get_actual_deployments', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.info.load_service_namespace_config', autospec=True),
    ) as (mock_get_actual_deployments, mock_load_service_namespace_config):
        mock_get_actual_deployments.return_value = ['clusterA.main', 'clusterB.main']
        mock_load_service_namespace_config.return_value = ServiceNamespaceConfig()
        actual = info.get_deployments_strings('unused')
        assert ' - clusterA (N/A)' in actual
        assert ' - clusterB (N/A)' in actual


def test_get_deployments_strings_no_deployments():
    with mock.patch(
        'paasta_tools.paasta_cli.cmds.info.get_actual_deployments', autospec=True
    ) as mock_get_actual_deployments:
        mock_get_actual_deployments.side_effect = NoDeploymentsAvailable
        actual = info.get_deployments_strings('unused')
        assert 'N/A: Not deployed' in actual[0]
