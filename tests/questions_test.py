import mock
import testify as T

import fsm


class AskSrvnameTestCase(T.TestCase):
    @T.setup_teardown
    def setup_mocks(self):
        """Calling raw_input() from automated tests can ruin your day, so we'll
        mock it out even for those situations where we don't care about it and
        "shouldn't" call raw_input().
        """
        with mock.patch('__builtin__.raw_input', autospec=True) as self.mock_raw_input:
            yield

    def test_arg_passed_in(self):
        """If a value is specified, use it."""
        srvname = 'services/fake'
        auto = 'UNUSED'
        expected = srvname
        actual = fsm.ask_srvname(srvname, auto)
        T.assert_equal(expected, actual)
        T.assert_equal(0, self.mock_raw_input.call_count)

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
            fsm.ask_srvname,
            srvname,
            auto,
        )
        T.assert_equal(0, self.mock_raw_input.call_count)

    def test_arg_not_passed_in_auto_false(self):
        """If a value is not specified but and --auto was not requested, prompt
        the user.
        """
        srvname = None
        auto = False
        fsm.ask_srvname(srvname, auto)
        T.assert_equal(1, self.mock_raw_input.call_count)
