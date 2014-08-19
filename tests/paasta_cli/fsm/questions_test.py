import mock
import testify as T

import fsm


class QuestionsTestCase(T.TestCase):
    pass
    @T.setup_teardown
    def setup_mocks(self):
        """Calling raw_input() from automated tests can ruin your day, so we'll
        mock it out even for those situations where we don't care about it and
        "shouldn't" call raw_input().
        """
        with mock.patch("service_wizard.questions.ask", autospec=True) as (
            self.mock_ask
        ):
            yield


class GetSrvnameTestCase(QuestionsTestCase):
    def test_arg_passed_in(self):
        """If a value is specified, use it."""
        srvname = 'services/fake'
        auto = 'UNUSED'
        expected = srvname
        actual = fsm.get_srvname(srvname, auto)
        T.assert_equal(expected, actual)
        T.assert_equal(0, self.mock_ask.call_count)

    def test_arg_not_passed_in_auto_true(self):
        """If a value is not specified but --auto was requested, calculate and
        use a sane default.

        In this specific case there is no sane default, so blow up.
        """
        srvname = None
        auto = True
        T.assert_raises_and_contains(
            SystemExit,
            "I'd Really Rather You Didn't Use --auto Without --service-name",
            fsm.get_srvname,
            srvname,
            auto,
        )
        T.assert_equal(0, self.mock_ask.call_count)

    def test_arg_not_passed_in_auto_false(self):
        """If a value is not specified but and --auto was not requested, prompt
        the user.
        """
        srvname = None
        auto = False
        fsm.get_srvname(srvname, auto)
        T.assert_equal(1, self.mock_ask.call_count)


class GetSmartstackYamlTestCase(QuestionsTestCase):
    def test_arg_passed_in(self):
        """If a port is specified, use it."""
        yelpsoa_config_root = 'fake_yelpsoa_config_root'
        port = '12345'
        auto = 'UNUSED'

        expected = { 'proxy_port': port }
        actual = fsm.get_smartstack_stanza(yelpsoa_config_root, auto, port)

        T.assert_equal(expected, actual)
        T.assert_equal(0, self.mock_ask.call_count)

    def test_arg_not_passed_in_auto_true(self):
        """If a value is not specified but --auto was requested, calculate and
        use a sane default.
        """
        yelpsoa_config_root = 'fake_yelpsoa_config_root'
        port = None
        suggested_port = 12345
        auto = True

        expected = { 'proxy_port': suggested_port }
        with mock.patch(
            "service_wizard.questions.suggest_smartstack_proxy_port",
            autospec=True,
            return_value=suggested_port,
        ) as (
            self.mock_suggest_smartstack_proxy_port
        ):
            actual = fsm.get_smartstack_stanza(yelpsoa_config_root, auto, port)

        self.mock_suggest_smartstack_proxy_port.assert_called_once_with(
            yelpsoa_config_root)
        T.assert_equal(expected, actual)
        T.assert_equal(0, self.mock_ask.call_count)

    def test_arg_not_passed_in_auto_false(self):
        """If a value is not specified but and --auto was not requested, prompt
        the user.
        """
        yelpsoa_config_root = 'fake_yelpsoa_config_root'
        port = None
        suggested_port = 12345
        auto = False

        expected = { 'proxy_port': suggested_port }
        self.mock_ask.return_value = suggested_port
        with mock.patch(
            "service_wizard.questions.suggest_smartstack_proxy_port",
            autospec=True,
            return_value=suggested_port,
        ) as (
            self.mock_suggest_smartstack_proxy_port
        ):
            actual = fsm.get_smartstack_stanza(yelpsoa_config_root, auto, port)

        self.mock_suggest_smartstack_proxy_port.assert_called_once_with(
            yelpsoa_config_root)
        T.assert_equal(expected, actual)
        self.mock_ask.assert_called_once_with(
            mock.ANY,
            suggested_port,
        )
