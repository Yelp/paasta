from contextlib import nested

import mock
import testify as T

from service_wizard import autosuggest
from service_wizard import config


class SuggestPortTestCase(T.TestCase):
    def test_suggest_port(self):
        # mock.patch was very confused by the config module, so I'm doing it
        # this way. One more reason to disapprove of this global config module
        # scheme.
        config.YELPSOA_CONFIG_ROOT = "fake_yelpsoa_config_root"

        walk_return = [(
            "fake_root",
            "fake_dir",
            [
                "fake_file",  # ignored
                "repl_delay_reporter.yaml",  # contains 'port' but ignored
                "port",
                "status_port",
                "weird_port",  # has bogus out-of-range value
            ]
        )]
        mock_walk = mock.Mock(return_value=walk_return)

        # See http://www.voidspace.org.uk/python/mock/examples.html#multiple-calls-with-different-effects
        get_port_from_file_returns = [
            13001,
            13002,
            55555,  # bogus out-of-range value
        ]

        def get_port_from_file_side_effect(*args):
            return get_port_from_file_returns.pop(0)
        mock_get_port_from_file = mock.Mock(side_effect=get_port_from_file_side_effect)
        with nested(
            mock.patch("os.walk", mock_walk),
            mock.patch("service_wizard.autosuggest._get_port_from_file", mock_get_port_from_file),
        ):
            actual = autosuggest.suggest_port()
        # Sanity check: our mock was called once for each legit port file in
        # walk_return
        T.assert_equal(mock_get_port_from_file.call_count, 3)

        # What we came here for: the actual output of the function under test
        T.assert_equal(actual, 13002 + 1)  # highest port + 1


# Shamelessly copied from SuggestPortTestCase
class SuggestSmartstackProxyPortTestCase(T.TestCase):
    def test_suggest_smartstack_proxy_port(self):
        yelpsoa_config_root = "fake_yelpsoa_config_root"
        walk_return = [
            ("fake_root1", "fake_dir1", ["service.yaml"]),
            ("fake_root2", "fake_dir2", ["smartstack.yaml"]),
            ("fake_root3", "fake_dir3", ["service.yaml"]),
        ]
        mock_walk = mock.Mock(return_value=walk_return)

        # See http://www.voidspace.org.uk/python/mock/examples.html#multiple-calls-with-different-effects
        get_smartstack_proxy_port_from_file_returns = [
            20001,
            20002,
            55555,  # bogus out-of-range value
        ]

        def get_smarstack_proxy_port_from_file_side_effect(*args):
            return get_smartstack_proxy_port_from_file_returns.pop(0)
        mock_get_smartstack_proxy_port_from_file = mock.Mock(side_effect=get_smarstack_proxy_port_from_file_side_effect)
        with nested(
            mock.patch("os.walk", mock_walk),
            mock.patch("service_wizard.autosuggest._get_smartstack_proxy_port_from_file",
                       mock_get_smartstack_proxy_port_from_file),
        ):
            actual = autosuggest.suggest_smartstack_proxy_port(yelpsoa_config_root)
        # Sanity check: our mock was called once for each legit port file in
        # walk_return
        T.assert_equal(mock_get_smartstack_proxy_port_from_file.call_count, 3)

        # What we came here for: the actual output of the function under test
        T.assert_equal(actual, 20002 + 1)  # highest port + 1
