import mock
from pytest import raises

from paasta_tools.paasta_cli.cmds import performance_check


@mock.patch('requests.post', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.performance_check.load_performance_check_config', autospec=True)
def test_submit_performance_check_job_happy(
    mock_load_performance_check_config,
    mock_requests_post,
):
    fake_endpoint = 'http://foo:1234/submit'
    mock_load_performance_check_config.return_value = {'endpoint': fake_endpoint}
    performance_check.submit_performance_check_job('fake_service', 'fake_commit')
    mock_requests_post.assert_called_once_with(
        url=fake_endpoint,
        data={'submitter': 'jenkins', 'commit': 'fake_commit', 'service': 'fake_service'}
    )


@mock.patch('paasta_tools.paasta_cli.cmds.performance_check.submit_performance_check_job', autospec=True)
def test_main_safely_returns_when_exceptions(
    mock_submit_performance_check_job,
):
    fake_args = mock.Mock()
    fake_args.service = 'fake_service'
    fake_args.commit = 'fake_commit'
    mock_submit_performance_check_job.side_effect = raises(Exception)
    performance_check.perform_performance_check(fake_args)
    mock_submit_performance_check_job.assert_called_once_with(service='fake_service', commit='fake_commit')
