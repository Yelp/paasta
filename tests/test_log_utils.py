import json
import sys

import mock
from pytest import raises

from paasta_tools import log_utils


def test_format_log_line():
    input_line = 'foo'
    fake_cluster = 'fake_cluster'
    fake_service = 'fake_service'
    fake_instance = 'fake_instance'
    fake_component = 'build'
    fake_level = 'debug'
    fake_now = 'fake_now'
    expected = json.dumps(
        {
            'timestamp': fake_now,
            'level': fake_level,
            'cluster': fake_cluster,
            'service': fake_service,
            'instance': fake_instance,
            'component': fake_component,
            'message': input_line,
        }, sort_keys=True,
    )
    with mock.patch('paasta_tools.log_utils._now', autospec=True) as mock_now:
        mock_now.return_value = fake_now
        actual = log_utils.format_log_line(
            level=fake_level,
            cluster=fake_cluster,
            service=fake_service,
            instance=fake_instance,
            component=fake_component,
            line=input_line,
        )
        assert actual == expected


def test_format_log_line_with_timestamp():
    input_line = 'foo'
    fake_cluster = 'fake_cluster'
    fake_service = 'fake_service'
    fake_instance = 'fake_instance'
    fake_component = 'build'
    fake_level = 'debug'
    fake_timestamp = 'fake_timestamp'
    expected = json.dumps(
        {
            'timestamp': fake_timestamp,
            'level': fake_level,
            'cluster': fake_cluster,
            'service': fake_service,
            'instance': fake_instance,
            'component': fake_component,
            'message': input_line,
        }, sort_keys=True,
    )
    actual = log_utils.format_log_line(
        fake_level,
        fake_cluster,
        fake_service,
        fake_instance,
        fake_component,
        input_line,
        timestamp=fake_timestamp,
    )
    assert actual == expected


def test_format_log_line_rejects_invalid_components():
    with raises(log_utils.NoSuchLogComponent):
        log_utils.format_log_line(
            level='debug',
            cluster='fake_cluster',
            service='fake_service',
            instance='fake_instance',
            line='fake_line',
            component='BOGUS_COMPONENT',
        )


def test_configure_log():
    fake_log_writer_config = {'driver': 'fake', 'options': {'fake_arg': 'something'}}
    with mock.patch('paasta_tools.log_utils.load_system_paasta_config', autospec=True) as mock_lspc:
        mock_lspc().get_log_writer.return_value = fake_log_writer_config
        with mock.patch('paasta_tools.log_utils.get_log_writer_class', autospec=True) as mock_get_log_writer_class:
            log_utils.configure_log()
            mock_get_log_writer_class.assert_called_once_with('fake')
            mock_get_log_writer_class('fake').assert_called_once_with(fake_arg='something')


def test_null_log_writer():
    """Basic smoke test for NullLogWriter"""
    lw = log_utils.NullLogWriter(driver='null')
    lw.log('fake_service', 'fake_line', 'build', 'BOGUS_LEVEL')


class TestFileLogWriter:
    def test_smoke(self):
        """Smoke test for FileLogWriter"""
        fw = log_utils.FileLogWriter('/dev/null')
        fw.log('fake_service', 'fake_line', 'build', 'BOGUS_LEVEL')

    def test_format_path(self):
        """Test the path formatting for FileLogWriter"""
        fw = log_utils.FileLogWriter("/logs/{service}/{component}/{level}/{cluster}/{instance}")
        expected = "/logs/a/b/c/d/e"
        assert expected == fw.format_path("a", "b", "c", "d", "e")

    def test_maybe_flock(self):
        """Make sure we flock and unflock when flock=True"""
        with mock.patch("paasta_tools.utils.fcntl", autospec=True) as mock_fcntl:
            fw = log_utils.FileLogWriter("/dev/null", flock=True)
            mock_file = mock.Mock()
            with fw.maybe_flock(mock_file):
                mock_fcntl.flock.assert_called_once_with(mock_file.fileno(), mock_fcntl.LOCK_EX)
                mock_fcntl.flock.reset_mock()

            mock_fcntl.flock.assert_called_once_with(mock_file.fileno(), mock_fcntl.LOCK_UN)

    def test_maybe_flock_flock_false(self):
        """Make sure we don't flock/unflock when flock=False"""
        with mock.patch("paasta_tools.utils.fcntl", autospec=True) as mock_fcntl:
            fw = log_utils.FileLogWriter("/dev/null", flock=False)
            mock_file = mock.Mock()
            with fw.maybe_flock(mock_file):
                assert mock_fcntl.flock.call_count == 0

            assert mock_fcntl.flock.call_count == 0

    def test_log_makes_exactly_one_write_call(self):
        """We want to make sure that log() makes exactly one call to write, since that's how we ensure atomicity."""
        fake_file = mock.Mock()
        fake_contextmgr = mock.Mock(
            __enter__=lambda _self: fake_file,
            __exit__=lambda _self, t, v, tb: None,
        )

        fake_line = "text" * 1000000

        with mock.patch("paasta_tools.log_utils.io.FileIO", return_value=fake_contextmgr, autospec=True) as mock_FileIO:
            fw = log_utils.FileLogWriter("/dev/null", flock=False)

            with mock.patch(
                "paasta_tools.log_utils.format_log_line", return_value=fake_line, autospec=True,
            ) as fake_fll:
                fw.log("service", "line", "component", level="level", cluster="cluster", instance="instance")

            fake_fll.assert_called_once_with("level", "cluster", "service", "instance", "component", "line")

            mock_FileIO.assert_called_once_with("/dev/null", mode=fw.mode, closefd=True)
            fake_file.write.assert_called_once_with("{}\n".format(fake_line).encode('UTF-8'))

    def test_write_raises_IOError(self):
        fake_file = mock.Mock()
        fake_file.write.side_effect = IOError("hurp durp")

        fake_contextmgr = mock.Mock(
            __enter__=lambda _self: fake_file,
            __exit__=lambda _self, t, v, tb: None,
        )

        fake_line = "line"

        with mock.patch(
            "paasta_tools.log_utils.io.FileIO", return_value=fake_contextmgr, autospec=True,
        ), mock.patch(
            "paasta_tools.log_utils.paasta_print", autospec=True,
        ) as mock_print, mock.patch(
            "paasta_tools.log_utils.format_log_line", return_value=fake_line, autospec=True,
        ):
            fw = log_utils.FileLogWriter("/dev/null", flock=False)
            fw.log(
                service="service",
                line="line",
                component="build",
                level="level",
                cluster="cluster",
                instance="instance",
            )

        mock_print.assert_called_once_with(
            mock.ANY,
            file=sys.stderr,
        )

        # On python3, they merged IOError and OSError. Once paasta is fully py3, replace mock.ANY above with the OSError
        # message below.
        assert mock_print.call_args[0][0] in {
            "Could not log to /dev/null: IOError: hurp durp -- would have logged: line\n",
            "Could not log to /dev/null: OSError: hurp durp -- would have logged: line\n",
        }


def test_get_log_name_for_service():
    service = 'foo'
    expected = 'stream_paasta_%s' % service
    assert log_utils.get_log_name_for_service(service) == expected


def test_ScribeLogWriter_log_raise_on_unknown_level():
    with raises(log_utils.NoSuchLogLevel):
        log_utils.ScribeLogWriter().log('fake_service', 'fake_line', 'build', 'BOGUS_LEVEL')
