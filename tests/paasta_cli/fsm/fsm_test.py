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


class AskPaastaQuestionsTestCase(T.TestCase):
    pass


class DoPaastaStepsTestCase(T.TestCase):
    pass
