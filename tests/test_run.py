import mock
from pytest import raises

from paasta_tools.run import _run


def test_run_cancels_timer_thread_on_keyboard_interrupt():
    mock_process = mock.Mock()
    mock_timer_object = mock.Mock()
    with mock.patch(
        'paasta_tools.run.Popen', autospec=True, return_value=mock_process,
    ), mock.patch(
        'paasta_tools.run.threading.Timer', autospec=True, return_value=mock_timer_object,
    ):
        mock_process.stdout.readline.side_effect = KeyboardInterrupt
        with raises(KeyboardInterrupt):
            _run('sh echo foo', timeout=10)
        assert mock_timer_object.cancel.call_count == 1


def test_run_returns_when_popen_fails():
    fake_exception = OSError(1234, 'fake error')
    with mock.patch('paasta_tools.run.Popen', autospec=True, side_effect=fake_exception):
        return_code, output = _run('nonexistant command', timeout=10)
    assert return_code == 1234
    assert 'fake error' in output
