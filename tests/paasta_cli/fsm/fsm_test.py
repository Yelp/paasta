import contextlib

import mock
import testify as T

import fsm
from service_wizard.questions import _yamlize
from service_wizard.service import SrvReaderWriter


class ValidateOptionsTest(T.TestCase):
    @T.setup_teardown
    def setup_mocks(self):
        with mock.patch("fsm.exists") as self.mock_exists:
            # Happpy path by default
            self.mock_exists.return_value = True
            yield

    def test_yelpsoa_config_root_exists(self):
        parser = mock.Mock()
        args = mock.Mock()
        args.yelpsoa_config_root = "non-existent thing"

        self.mock_exists.return_value = False
        T.assert_raises_and_contains(
            SystemExit,
            ("I'd Really Rather You Didn't Use A Non-Existent --yelpsoa-config-root"
                "Like %s" % args.yelpsoa_config_root),
            fsm.validate_args,
            parser,
            args,
        )

    def test_auto_and_no_service_name(self):
        parser = mock.Mock()
        args = mock.Mock()
        args.auto = True
        args.srvname = None

        T.assert_raises_and_contains(
            SystemExit,
            ("I'd Really Rather You Didn't Use --auto Without --service-name"),
            fsm.validate_args,
            parser,
            args,
        )


class GetPaastaConfigTestCase(T.TestCase):
    @T.setup_teardown
    def setup_mocks(self):
        with contextlib.nested(
            mock.patch("fsm.get_srvname", autospec=True),
            mock.patch("fsm.get_smartstack_stanza", autospec=True),
            mock.patch("fsm.get_marathon_stanza", autospec=True),
            mock.patch("fsm.get_monitoring_stanza", autospec=True),
        ) as (
            self.mock_get_srvname,
            self.mock_get_smartstack_stanza,
            self.mock_get_marathon_stanza,
            self.mock_get_monitoring_stanza,
        ):
            yield

    def test_everything_specified(self):
        """A sort of happy path test because we don't care about the logic in
        the individual get_* methods, just that all of them get called as
        expected.
        """
        yelpsoa_config_root = "fake_yelpsoa_config_root"
        srvname = "services/fake_srvname"
        auto = "UNUSED"
        port = 12345
        fsm.get_paasta_config(yelpsoa_config_root, srvname, auto, port)

        self.mock_get_srvname.assert_called_once_with(srvname, auto)
        self.mock_get_smartstack_stanza.assert_called_once_with(yelpsoa_config_root, auto, port)
        self.mock_get_marathon_stanza.assert_called_once_with()
        self.mock_get_monitoring_stanza.assert_called_once_with()


class WritePaastaConfigTestCase(T.TestCase):
    @T.setup
    def setup_mocks(self):
        self.srv = mock.Mock()
        self.srv.io = mock.Mock(spec_set=SrvReaderWriter)

    def test(self):
        smartstack_stanza = { "stack": "smrt" }
        marathon_stanza = { "springfield": "2015-04-20" }
        monitoring_stanza = { "team": "homer" }

        fsm.write_paasta_config(
            self.srv,
            smartstack_stanza,
            marathon_stanza,
            monitoring_stanza,
        )

        self.srv.io.write_file.assert_any_call(
            "smartstack.yaml",
            _yamlize(smartstack_stanza),
        )
        self.srv.io.write_file.assert_any_call(
            "marathon-devc.yaml",
            _yamlize(marathon_stanza),
        )
        self.srv.io.write_file.assert_any_call(
            "monitoring.yaml",
            _yamlize(monitoring_stanza),
        )
