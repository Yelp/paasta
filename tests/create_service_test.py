from contextlib import nested

import mock
import testify as T

import create_service
from service_setup import autosuggest
from service_setup import config


class SrvReaderWriterTestCase(T.TestCase):
    """I bailed out of this test, but I'll leave this here for now as an
    example of how to interact with the Srv* classes."""
    @T.setup
    def init_service(self):
        paths = create_service.paths.SrvPathBuilder("fake_srvpathbuilder")
        self.srw = create_service.SrvReaderWriter(paths)

class ValidateOptionsTestCase(T.TestCase):
    def test_enable_puppet_requires_puppet_root(self):
        parser = mock.Mock()
        options = mock.Mock()
        options.enable_puppet = True
        options.puppet_root = None
        with T.assert_raises(SystemExit):
            create_service.validate_options(parser, options)

    def test_enable_nagios_requires_nagios_root(self):
        parser = mock.Mock()
        options = mock.Mock()
        options.enable_nagios = True
        options.nagios_root = None
        with T.assert_raises(SystemExit):
            create_service.validate_options(parser, options)

class AutosuggestTestCase(T.TestCase):
    def test_suggest_port(self):
        # mock.patch was very confused by the config module, so I'm doing it
        # this way. One more reason to disapprove of this global config module
        # scheme.
        config.PUPPET_ROOT = "fake_puppet_root"

        walk_return = [(
            "fake_root",
            "fake_dir",
            [
                "fake_file", # ignored
                "repl_delay_reporter.yaml", # contains 'port' but ignored
                "port",
                "status_port",
                "admin_port", # has bogus out-of-range value
            ]
        )]
        mock_walk = mock.Mock(return_value=walk_return)

        # See http://www.voidspace.org.uk/python/mock/examples.html#multiple-calls-with-different-effects
        get_port_from_port_file_returns = [
            13001,
            13002,
            55555, # bogus out-of-range value
        ]
        def get_port_from_port_file_side_effect(*args):
            return get_port_from_port_file_returns.pop(0)
        mock_get_port_from_port_file = mock.Mock(side_effect=get_port_from_port_file_side_effect)
        with nested(
            mock.patch("os.walk", mock_walk),
            mock.patch("service_setup.autosuggest._get_port_from_port_file", mock_get_port_from_port_file),
        ):
            actual = autosuggest.suggest_port()
        # Sanity check: our mock was called once for each legit port file in
        # walk_return
        T.assert_equal(mock_get_port_from_port_file.call_count, 3)

        # What we came here for: the actual output of the function under test
        T.assert_equal(actual, 13002 + 1) # highest port + 1


if __name__ == "__main__":
    T.run()
