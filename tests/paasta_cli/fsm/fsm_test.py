import contextlib

import mock
import testify as T

import fsm


class ValidateOptionsTest(T.TestCase):
    def test_yelpsoa_config_root_required(self):
        parser = mock.Mock()
        options = mock.Mock()
        options.yelpsoa_config_root = None

        T.assert_raises_and_contains(
            SystemExit,
            "ERROR: --yelpsoa-config-root is required",
            fsm.validate_options,
            parser,
            options,
        )


class GetPaastaConfigTestCase(T.TestCase):
    @T.setup_teardown
    def setup_mocks(self):
        with contextlib.nested(
            mock.patch("fsm.get_srvname", autospec=True),
        ) as (
            self.mock_get_srvname,
        ):
            yield

    def test_everything_specified(self):
        """A sort of happy path test because we don't care about the logic in
        the individual get_* methods, just that al of them get called as
        expected.
        """
        srvname = 'services/fake_srvname'
        auto = 'UNUSED'
        fsm.get_paasta_config(srvname, auto)

        self.mock_get_srvname.assert_called_once_with(srvname, auto)


class DoPaastaStepsTestCase(T.TestCase):
    def test(self):
        pass
