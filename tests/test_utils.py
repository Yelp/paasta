import mock

from paasta_tools import utils


def test_get_git_url():
    service = 'giiiiiiiiiiit'
    expected = 'git@git.yelpcorp.com:services/%s.git' % service
    assert utils.get_git_url(service) == expected


def test_format_log_line():
    # cluster
    # instance
    input_line = 'foo'
    fake_cluster = 'fake_cluster'
    fake_instance = 'fake_instance'
    fake_now = 'fake_now'
    expected = {
        'timestamp': fake_now,
        'cluster': fake_cluster,
        'instance': fake_instance,
        'message': input_line,
    }
    with mock.patch('paasta_tools.utils._now', autospec=True) as mock_now:
        mock_now.return_value = fake_now
        assert utils.format_log_line(fake_cluster, fake_instance, input_line) == expected


def test_get_log_name_for_service():
    service_name = 'foo'
    expected = 'stream_paasta_%s' % service_name
    assert utils.get_log_name_for_service(service_name) == expected


def test_run():
    with mock.patch('paasta_tools.utils.Popen') as mock_popen:
        fake_cmd = 'command --with --options'
        fake_rc = 0
        fake_stdout = 'goooood'
        fake_stderr = 'not gooood'
        fake_output = (fake_stdout, fake_stderr)
        mock_popen.return_value = mock.Mock()
        mock_popen.return_value.returncode = fake_rc
        mock_popen.return_value.communicate = mock.Mock()
        mock_popen.return_value.communicate.return_value = fake_output
        assert utils._run(fake_cmd) == (fake_rc, fake_stdout)
