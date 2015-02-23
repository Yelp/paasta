import contextlib
import mock

from paasta_tools.paasta_cli.cmds import info


def test_get_service_info():
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_cli.cmds.info.get_team', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.info.get_runbook', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.info.read_service_configuration', autospec=True),
        mock.patch('paasta_tools.paasta_cli.cmds.info.get_actual_deployments', autospec=True),
    ) as (
        mock_get_team,
        mock_get_runbook,
        mock_read_service_configuration,
        mock_get_actual_deployments,
    ):
        mock_get_team.return_value = 'fake_team'
        mock_get_runbook.return_value = 'fake_runbook'
        mock_read_service_configuration.return_value = {
            'description': 'a fake service that does stuff',
            'external_link': 'http://bla',
        }
        mock_get_actual_deployments.return_value = ['clusterA.main', 'clusterB.main']
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
        assert 'clusterA' in actual
        assert 'clusterB' in actual


def test_deployments_to_clusters():
    deployments = ['A.main', 'A.canary', 'B.main', 'C.othermain']
    expected = set(['A', 'B', 'C'])
    actual = info.deployments_to_clusters(deployments)
    assert actual == expected
