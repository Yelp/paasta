import mock

from paasta_tools.monitoring import context


def test_get_serviceinit_status():
    with mock.patch('paasta_tools.monitoring.context._run') as mock_run:
        expected_command = 'paasta_serviceinit -v fake_service.fake_instance status'
        context.get_serviceinit_status('fake_service', 'fake_instance')
        mock_run.assert_called_once_with(expected_command, timeout=mock.ANY)


def test_get_context():
    with mock.patch('paasta_tools.monitoring.context.get_serviceinit_status') as status_patch:
        status_patch.return_value = 'fake status'
        actual = context.get_context('fake_service', 'fake_instance')
        status_patch.assert_called_once_with('fake_service', 'fake_instance')
        assert 'fake status' in actual
        assert 'More context' in actual
